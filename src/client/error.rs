use std::{io, sync::Arc};
use tokio::sync::mpsc::error::SendError as MpscSendError;
use tokio::sync::oneshot::error::RecvError as OneshotRecvError;

use crate::{error, protocol::Status};

/// Enum for client errors
#[derive(Debug, Clone)]
pub enum Error {
    /// Contains an error status packet
    Status(Status),
    /// Any errors related to I/O
    IO(Arc<io::Error>),
    /// A codec or protocol failure with its original cause.
    Protocol(Arc<error::Error>),
    /// Explicit local transport shutdown.
    Closed,
    /// The transport rejected this request before output admission.
    NotSent(Arc<Error>),
    /// Time limit for receiving response packet exceeded
    Timeout,
    /// Occurs due to exceeding the limits set by the `limits@openssh.com` extension
    Limited(String),
    /// Occurs when an unexpected packet is sent
    UnexpectedPacket,
    /// Occurs when unexpected server behavior differs from the protocol specifition
    UnexpectedBehavior(String),
}

impl From<Status> for Error {
    fn from(status: Status) -> Self {
        Self::Status(status)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::IO(Arc::new(error))
    }
}

impl<T> From<MpscSendError<T>> for Error {
    fn from(err: MpscSendError<T>) -> Self {
        Self::UnexpectedBehavior(format!("SendError: {}", err))
    }
}

impl From<OneshotRecvError> for Error {
    fn from(err: OneshotRecvError) -> Self {
        Self::UnexpectedBehavior(format!("RecvError: {}", err))
    }
}

impl From<error::Error> for Error {
    fn from(error: error::Error) -> Self {
        match error {
            error::Error::IO(error) => Self::IO(error),
            error::Error::Client(error) => (*error).clone(),
            error => Self::Protocol(Arc::new(error)),
        }
    }
}

impl From<Error> for io::Error {
    fn from(error: Error) -> Self {
        let kind = match &error {
            Error::Timeout => io::ErrorKind::TimedOut,
            Error::IO(error) => error.kind(),
            _ => io::ErrorKind::Other,
        };
        Self::new(kind, error)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Status(v) => write!(f, "{}: {}", v.status_code, v.error_message),
            Self::IO(v) => write!(f, "I/O: {v}"),
            Self::Protocol(v) => v.fmt(f),
            Self::Closed => f.write_str("SFTP session closed"),
            Self::NotSent(v) => write!(f, "SFTP request not sent: {v}"),
            Self::Timeout => f.write_str("Timeout"),
            Self::Limited(v) => write!(f, "Limit exceeded: {v}"),
            Self::UnexpectedPacket => f.write_str("Unexpected packet"),
            Self::UnexpectedBehavior(v) => f.write_str(v),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IO(v) => Some(v.as_ref()),
            Self::Protocol(v) => Some(v.as_ref()),
            Self::NotSent(v) => Some(v.as_ref()),
            _ => None,
        }
    }
}
