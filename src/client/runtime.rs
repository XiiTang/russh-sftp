//! Runtime abstraction over tokio (native) and wasm-bindgen-futures / gloo-timers (wasm32).

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use super::error::Error;

/// A task's join failure, preserving the runtime's original panic payload.
#[derive(Debug)]
pub struct JoinError {
    #[cfg(not(target_arch = "wasm32"))]
    cause: tokio::task::JoinError,
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(not(target_arch = "wasm32"))]
        {
            std::fmt::Display::fmt(&self.cause, f)
        }
        #[cfg(target_arch = "wasm32")]
        {
            f.write_str("task ended without publishing its result")
        }
    }
}

impl std::error::Error for JoinError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            Some(&self.cause)
        }
        #[cfg(target_arch = "wasm32")]
        {
            None
        }
    }
}

pub struct JoinHandle<T: Send> {
    #[cfg(not(target_arch = "wasm32"))]
    handle: tokio::task::JoinHandle<T>,
    #[cfg(target_arch = "wasm32")]
    handle: tokio::sync::oneshot::Receiver<T>,
}

pub fn spawn<F, T>(future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    #[cfg(not(target_arch = "wasm32"))]
    {
        JoinHandle {
            handle: tokio::spawn(future),
        }
    }
    #[cfg(target_arch = "wasm32")]
    {
        let (sender, handle) = tokio::sync::oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let _ = sender.send(future.await);
        });
        JoinHandle { handle }
    }
}

impl<T: Send> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            Pin::new(&mut self.handle)
                .poll(cx)
                .map(|result| result.map_err(|cause| JoinError { cause }))
        }
        #[cfg(target_arch = "wasm32")]
        {
            Pin::new(&mut self.handle)
                .poll(cx)
                .map(|result| result.map_err(|_| JoinError {}))
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub fn timeout<F: Future>(
    duration: Duration,
    future: F,
) -> impl Future<Output = Result<F::Output, Error>> {
    // Queued requests may only be polled after earlier replies have arrived.
    let deadline = tokio::time::Instant::now().checked_add(duration);
    async move {
        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, future)
                .await
                .map_err(|_| Error::Timeout),
            None => Ok(future.await),
        }
    }
}

// wasm32-unknown-unknown is single-threaded, so Send is trivially safe
#[cfg(target_arch = "wasm32")]
struct SendWrapper<F>(F);

#[cfg(target_arch = "wasm32")]
unsafe impl<F> Send for SendWrapper<F> {}

#[cfg(target_arch = "wasm32")]
unsafe impl<F> Sync for SendWrapper<F> {}

#[cfg(target_arch = "wasm32")]
impl<F: Future> Future for SendWrapper<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe { self.map_unchecked_mut(|w| &mut w.0) }.poll(cx)
    }
}

#[cfg(target_arch = "wasm32")]
pub fn timeout<F: Future>(
    duration: Duration,
    future: F,
) -> impl Future<Output = Result<F::Output, Error>> {
    let timer = SendWrapper(gloo_timers::future::TimeoutFuture::new(
        duration.as_millis() as u32,
    ));
    async move {
        tokio::pin!(future);
        tokio::pin!(timer);
        tokio::select! {
            v = &mut future => Ok(v),
            _ = &mut timer => Err(Error::Timeout),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod panic_tests {
    use super::*;
    use std::{
        error::Error as _,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    };

    #[tokio::test]
    async fn join_keeps_string_panic_causes() {
        for payload in [
            Box::new("literal panic") as Box<dyn std::any::Any + Send>,
            Box::new(String::from("owned panic")),
        ] {
            let error = spawn(async move { std::panic::resume_unwind(payload) })
                .await
                .unwrap_err();
            assert!(error.to_string().contains("panic"));
            assert!(error
                .source()
                .unwrap()
                .downcast_ref::<tokio::task::JoinError>()
                .unwrap()
                .is_panic());
        }
        assert_eq!(spawn(async { 7 }).await.unwrap(), 7);
    }

    struct Payload(Arc<AtomicBool>);
    impl Drop for Payload {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn join_retains_a_non_string_panic_payload_until_the_error_is_dropped() {
        let dropped = Arc::new(AtomicBool::new(false));
        let payload = Payload(dropped.clone());
        let error = spawn(async move { std::panic::panic_any(payload) })
            .await
            .unwrap_err();
        assert!(
            !dropped.load(Ordering::SeqCst),
            "join lost the panic payload"
        );
        assert!(error
            .source()
            .unwrap()
            .downcast_ref::<tokio::task::JoinError>()
            .unwrap()
            .is_panic());
        drop(error);
        assert!(dropped.load(Ordering::SeqCst));
    }
}
