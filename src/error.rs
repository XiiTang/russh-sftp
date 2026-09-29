use bytes::TryGetError;
use std::{fmt, io, sync::Arc};

use crate::client;

#[derive(Debug, Clone)]
pub enum Error {
    IO(Arc<io::Error>),
    UnexpectedEof,
    BadMessage(String),
    Client(Arc<client::error::Error>),
    UnexpectedBehavior(String),
}

impl From<client::error::Error> for Error {
    fn from(error: client::error::Error) -> Self {
        Self::Client(Arc::new(error))
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::IO(Arc::new(err))
    }
}

impl From<TryGetError> for Error {
    fn from(err: TryGetError) -> Self {
        Self::BadMessage(format!(
            "only {} bytes remaining, but {} requested",
            err.available, err.requested
        ))
    }
}

impl serde::ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display,
    {
        Self::BadMessage(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display,
    {
        Self::BadMessage(msg.to_string())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IO(v) => write!(f, "I/O: {v}"),
            Self::UnexpectedEof => f.write_str("Unexpected EOF on stream"),
            Self::BadMessage(v) => write!(f, "Bad message: {v}"),
            Self::Client(v) => write!(f, "Client error. ({v})"),
            Self::UnexpectedBehavior(v) => write!(f, "Unexpected behavior: {v}"),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IO(v) => Some(v.as_ref()),
            Self::Client(v) => Some(v.as_ref()),
            _ => None,
        }
    }
}
