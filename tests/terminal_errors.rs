use russh_sftp::client::{self, error::Error, RawSftpSession};
use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    sync::{oneshot, Notify},
};

struct Fault {
    gate: oneshot::Receiver<()>,
    writes: Arc<AtomicUsize>,
    written: Arc<Notify>,
    write_failure: bool,
}
impl AsyncRead for Fault {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        use std::future::Future;
        match Pin::new(&mut self.gate).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "peer-reset-evidence",
            ))),
        }
    }
}
impl AsyncWrite for Fault {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.write_failure {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "peer-write-evidence",
            )));
        }
        self.writes.fetch_add(1, Ordering::Release);
        self.written.notify_one();
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
fn io_cause<'a>(error: &'a (dyn std::error::Error + 'static)) -> &'a io::Error {
    let mut cause = Some(error);
    while let Some(error) = cause {
        if let Some(error) = error.downcast_ref::<io::Error>() {
            return error;
        }
        cause = error.source();
    }
    panic!("missing typed I/O cause: {error:?}")
}
#[tokio::test]
async fn first_read_failure_wakes_all_pending_and_survives_shutdown_and_new_requests() {
    let (release, gate) = oneshot::channel();
    let writes = Arc::new(AtomicUsize::new(0));
    let written = Arc::new(Notify::new());
    let session = RawSftpSession::new(Fault {
        gate,
        writes: writes.clone(),
        written: written.clone(),
        write_failure: false,
    });
    let fail = async {
        loop {
            let ready = written.notified();
            if writes.load(Ordering::Acquire) >= 2 {
                break;
            }
            ready.await;
        }
        release.send(()).unwrap();
    };
    let (first, second, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(session.init(), session.stat("file"), fail)
    })
    .await
    .unwrap();
    for error in [first.unwrap_err(), second.unwrap_err()] {
        assert!(!matches!(error, Error::NotSent(_)));
        assert_eq!(io_cause(&error).kind(), io::ErrorKind::ConnectionReset);
        assert_eq!(io_cause(&error).to_string(), "peer-reset-evidence");
    }
    session.shutdown().await;
    let error = session.stat("later").await.unwrap_err();
    assert!(matches!(error, Error::NotSent(_)));
    assert_eq!(io_cause(&error).to_string(), "peer-reset-evidence");
    assert_eq!(writes.load(Ordering::Acquire), 2);
}
#[tokio::test]
async fn writer_failure_preserves_typed_cause() {
    let (_release, gate) = oneshot::channel();
    let session = RawSftpSession::new(Fault {
        gate,
        writes: Arc::new(AtomicUsize::new(0)),
        written: Arc::new(Notify::new()),
        write_failure: true,
    });
    let error = tokio::time::timeout(std::time::Duration::from_secs(2), session.init())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(io_cause(&error).kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(io_cause(&error).to_string(), "peer-write-evidence");
    session.shutdown().await;
}
#[tokio::test]
async fn malformed_and_truncated_responses_retain_the_driver_reason() {
    for wire in [vec![0, 0, 0, 1, 255], vec![0, 0], vec![0, 0, 0, 4, 2]] {
        let (stream, mut peer) = tokio::io::duplex(128);
        let session = RawSftpSession::new(stream);
        let (result, ()) = tokio::join!(session.init(), async {
            let length = peer.read_u32().await.unwrap();
            let mut request = vec![0; length as usize];
            peer.read_exact(&mut request).await.unwrap();
            peer.write_all(&wire).await.unwrap();
            peer.shutdown().await.unwrap();
        });
        let error = result.unwrap_err();
        if wire != [0, 0, 0, 1, 255] {
            assert_eq!(io_cause(&error).kind(), io::ErrorKind::UnexpectedEof);
        } else {
            assert!(!error.to_string().contains("sender dropped"));
            assert!(matches!(error, Error::Protocol(_)));
        }
        session.shutdown().await;
    }
}
struct Handler;
impl client::Handler for Handler {
    type Error = Error;
}
#[tokio::test]
async fn full_and_closed_output_are_distinct_and_blocked_write_can_join() {
    let (stream, _peer) = tokio::io::duplex(1);
    let driver = client::run(stream, Handler);
    let mut last = Ok(());
    for _ in 0..34 {
        last = driver.send(bytes::Bytes::from_static(b"blocked"));
    }
    assert!(matches!(last, Err(Error::Limited(_))));
    tokio::time::timeout(std::time::Duration::from_secs(2), driver.shutdown())
        .await
        .unwrap();
    assert!(matches!(
        driver.send(bytes::Bytes::from_static(b"later")),
        Err(Error::NotSent(_))
    ));
}
#[tokio::test]
async fn duplicate_init_does_not_remove_the_original_waiter() {
    let (stream, mut peer) = tokio::io::duplex(128);
    let session = RawSftpSession::new(stream);
    let mut first = Box::pin(session.init());
    tokio::select! {
        _ = &mut first => panic!("peer has not replied"),
        length = peer.read_u32() => { let mut bytes = vec![0;length.unwrap() as usize]; peer.read_exact(&mut bytes).await.unwrap(); }
    }
    assert!(matches!(
        session.init().await,
        Err(Error::UnexpectedBehavior(_))
    ));
    peer.write_all(&[0, 0, 0, 5, 2, 0, 0, 0, 3]).await.unwrap();
    assert_eq!(first.await.unwrap().version, 3);
    session.shutdown().await;
}
