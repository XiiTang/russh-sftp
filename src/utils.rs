use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::time::SystemTime;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::Error;

pub(crate) fn unix(time: SystemTime) -> u32 {
    DateTime::<Utc>::from(time).timestamp() as u32
}

pub(crate) async fn read_packet<S: AsyncRead + Unpin>(
    stream: &mut S,
    max_length: u32,
) -> Result<Bytes, Error> {
    let mut header = [0; 4];
    if stream.read(&mut header[..1]).await? == 0 {
        return Err(Error::UnexpectedEof);
    }
    stream.read_exact(&mut header[1..]).await?;
    let length = u32::from_be_bytes(header);
    if length > max_length {
        return Err(Error::BadMessage("packet length limit exceeded".to_owned()));
    }

    let mut buf = vec![0; length as usize];
    stream.read_exact(&mut buf).await?;

    Ok(Bytes::from(buf))
}
