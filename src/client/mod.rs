pub mod error;
pub mod fs;
mod handler;
pub mod rawsession;
pub(crate) mod runtime;
mod session;

pub use handler::Handler;
pub use rawsession::RawSftpSession;
pub use session::SftpSession;

use bytes::Bytes;
use std::sync::{Arc, Mutex};
use tokio::{
    io::{self, AsyncRead, AsyncWrite, AsyncWriteExt},
    select,
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

use crate::{error::Error, protocol::Packet, utils::read_packet};

macro_rules! into_wrap {
    ($handler:expr) => {
        match $handler.await {
            Err(error) => Err(error.into()),
            Ok(()) => Ok(()),
        }
    };
}

#[derive(Clone, Debug)]
pub struct Config {
    /// Maximum size of a single packet in bytes. Default: 256 KiB.
    pub max_packet_len: u32,
    /// Maximum number of concurrent in-flight read requests. Default: 16.
    pub max_concurrent_reads: usize,
    /// Maximum number of concurrent in-flight write requests. Default: 16.
    pub max_concurrent_writes: usize,
    /// Preferred maximum size of a framed write packet. Default: 32 KiB.
    pub max_write_packet_len: u32,
    /// Timeout in seconds for each request. Default: 10.
    pub request_timeout_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_packet_len: 262144,
            max_concurrent_reads: 16,
            max_concurrent_writes: 16,
            max_write_packet_len: 32768,
            request_timeout_secs: 10,
        }
    }
}

async fn execute_handler<H>(bytes: &mut Bytes, handler: &mut H) -> Result<(), error::Error>
where
    H: Handler + Send,
{
    match Packet::try_from(bytes)? {
        Packet::Version(p) => into_wrap!(handler.version(p)),
        Packet::Status(p) => into_wrap!(handler.status(p)),
        Packet::Handle(p) => into_wrap!(handler.handle(p)),
        Packet::Data(p) => into_wrap!(handler.data(p)),
        Packet::Name(p) => into_wrap!(handler.name(p)),
        Packet::Attrs(p) => into_wrap!(handler.attrs(p)),
        Packet::ExtendedReply(p) => into_wrap!(handler.extended_reply(p)),
        _ => Err(error::Error::UnexpectedBehavior(
            "A packet was received that could not be processed.".to_owned(),
        )),
    }
}

async fn process_handler<S, H>(stream: &mut S, handler: &mut H, maximum: u32) -> Result<(), Error>
where
    S: AsyncRead + Unpin,
    H: Handler + Send,
{
    let mut bytes = read_packet(stream, maximum).await?;
    Ok(execute_handler(&mut bytes, handler).await?)
}

/// Run processing stream as SFTP client. Is a simple handler of incoming
/// and outgoing packets. Can be used for non-standard implementations
pub fn run<S, H>(stream: S, handler: H) -> Driver
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: Handler + Send + 'static,
{
    run_with_config(stream, handler, &Config::default())
}

pub struct Driver {
    tx: mpsc::Sender<Bytes>,
    stop: CancellationToken,
    tasks: tokio::sync::Mutex<TaskState>,
    terminal: Arc<Mutex<Option<error::Error>>>,
}
#[derive(Default)]
struct TaskState {
    handles: Vec<runtime::JoinHandle<()>>,
    // Retained across cancellation of a waiter that already joined an earlier child.
    errors: Vec<error::Error>,
}

impl Driver {
    pub fn is_closed(&self) -> bool {
        self.stop.is_cancelled() || self.tx.is_closed()
    }
    pub fn send(&self, bytes: Bytes) -> Result<(), error::Error> {
        // Serialize admission with publishing termination. A successful enqueue
        // may already have reached the wire; a refused enqueue never has.
        let terminal = self.terminal.lock().unwrap();
        if let Some(error) = &*terminal {
            return Err(error::Error::NotSent(Arc::new(error.clone())));
        }
        self.tx.try_send(bytes).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => {
                error::Error::Limited("SFTP pending output queue is full".into())
            }
            mpsc::error::TrySendError::Closed(_) => {
                error::Error::NotSent(Arc::new(error::Error::Closed))
            }
        })
    }
    pub fn close(&self) {
        self.terminal
            .lock()
            .unwrap()
            .get_or_insert(error::Error::Closed);
        self.stop.cancel();
    }
    /// Stop both halves and read every task failure. A cancelled waiter leaves
    /// unjoined handles and already collected failures for the next call.
    pub async fn shutdown(&self) -> Vec<error::Error> {
        self.close();
        let mut tasks = self.tasks.lock().await;
        while let Some(task) = tasks.handles.last_mut() {
            let end = task.await;
            tasks.handles.pop();
            if let Err(error) = end {
                tasks
                    .errors
                    .push(error::Error::IO(Arc::new(std::io::Error::other(error))));
            }
        }
        std::mem::take(&mut tasks.errors)
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        self.close();
    }
}

pub fn run_with_config<S, H>(stream: S, mut handler: H, config: &Config) -> Driver
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: Handler + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel::<Bytes>(32);
    let maximum = config.max_packet_len;

    let (mut rd, mut wr) = io::split(stream);

    let rc = CancellationToken::new();
    let wc = rc.clone();
    let stop = rc.clone();
    let terminal = Arc::new(Mutex::new(None));
    let read_terminal = terminal.clone();
    let write_terminal = terminal.clone();
    let reader = runtime::spawn(async move {
        loop {
            select! {
                result = process_handler(&mut rd, &mut handler, maximum) => {
                    if let Err(err) = result {
                        read_terminal.lock().unwrap().get_or_insert(err.into());
                        break;
                    }
                },
                _ = rc.cancelled() => break,
            }
        }

        let error = read_terminal
            .lock()
            .unwrap()
            .get_or_insert(error::Error::Closed)
            .clone();
        handler.closed(error);
        rc.cancel();
        debug!("read half of sftp stream ended");
    });

    let writer = runtime::spawn(async move {
        loop {
            select! {
                data = rx.recv() => {
                    let Some(data) = data else { break; };
                    if data.is_empty() {
                        select! { result = wr.shutdown() => {
                            if let Err(error) = result { write_terminal.lock().unwrap().get_or_insert(error.into()); }
                        }, _ = wc.cancelled() => {} }
                        break;
                    }

                    select! {
                        result = wr.write_all(&data[..]) => if let Err(error) = result {
                            write_terminal.lock().unwrap().get_or_insert(error.into());
                            break;
                        },
                        _ = wc.cancelled() => break,
                    }
                },
                _ = wc.cancelled() => break,
            }
        }

        write_terminal
            .lock()
            .unwrap()
            .get_or_insert(error::Error::Closed);
        wc.cancel();
        debug!("write half of sftp stream ended");
    });

    Driver {
        tx,
        stop,
        tasks: tokio::sync::Mutex::new(TaskState {
            handles: vec![reader, writer],
            errors: Vec::new(),
        }),
        terminal,
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;
    use futures::FutureExt;

    fn driver(tasks: Vec<runtime::JoinHandle<()>>) -> Driver {
        let (tx, _) = mpsc::channel(1);
        Driver {
            tx,
            stop: CancellationToken::new(),
            tasks: tokio::sync::Mutex::new(TaskState {
                handles: tasks,
                errors: Vec::new(),
            }),
            terminal: Arc::new(Mutex::new(None)),
        }
    }

    #[tokio::test]
    async fn shutdown_reports_every_task_failure_and_repeated_calls_are_clean() {
        let driver = driver(vec![
            runtime::spawn(async { panic!("reader task failed") }),
            runtime::spawn(async { panic!("writer task failed") }),
        ]);
        let failures = driver.shutdown().await;
        assert_eq!(failures.len(), 2);
        assert!(failures
            .iter()
            .all(|error| matches!(error, error::Error::IO(_))));
        assert!(driver.shutdown().await.is_empty());
    }

    #[tokio::test]
    async fn cancelled_shutdown_keeps_unjoined_tasks_for_the_next_waiter() {
        let (release, held) = tokio::sync::oneshot::channel();
        let (panicking, ready) = tokio::sync::oneshot::channel();
        let failed = runtime::spawn(async move {
            panicking.send(()).unwrap();
            panic!("earlier joined task failed");
        });
        // On this current-thread runtime the panicking poll finishes before
        // the signaled test is polled again, so the first join is ready.
        ready.await.unwrap();
        let driver = driver(vec![
            runtime::spawn(async move {
                held.await.unwrap();
            }),
            failed,
        ]);
        let mut first = Box::pin(driver.shutdown());
        assert!(first.as_mut().now_or_never().is_none());
        drop(first);
        {
            let tasks = driver.tasks.lock().await;
            assert_eq!(tasks.handles.len(), 1);
            assert_eq!(tasks.errors.len(), 1);
        }
        release.send(()).unwrap();
        assert_eq!(driver.shutdown().await.len(), 1);
        assert!(driver.tasks.lock().await.handles.is_empty());
    }
}
