use bytes::Bytes;
use dashmap::DashMap as HashMap;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{ready, Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::oneshot,
};

use super::{error::Error, runtime, Handler};
use crate::{
    client::{run_with_config, Config},
    de,
    extensions::{
        self, ExpandPathExtension, FsyncExtension, HardlinkExtension, LimitsExtension, Statvfs,
        StatvfsExtension,
    },
    protocol::{
        Attrs, Close, Data, Extended, ExtendedReply, FSetStat, FileAttributes, Fstat, Handle, Init,
        Lstat, MkDir, Name, Open, OpenDir, OpenFlags, Packet, Read, ReadDir, ReadLink, RealPath,
        Remove, Rename, RmDir, SetStat, Stat, Status, StatusCode, Symlink, Version, Write,
        WriteRef,
    },
};

pub type SftpResult<T> = Result<T, Error>;
type SharedRequests = HashMap<Option<u32>, oneshot::Sender<SftpResult<Packet>>>;

/// An admitted request whose response remains owned until this future is awaited or dropped.
/// Dropping it cancels only response observation; it never retries the request.
#[must_use = "await the request to observe its acknowledgement"]
pub struct Request {
    id: Option<u32>,
    requests: Arc<SharedRequests>,
    response: oneshot::Receiver<SftpResult<Packet>>,
    timeout: Pin<Box<dyn Future<Output = SftpResult<()>> + Send + Sync>>,
    completed: bool,
    terminal: Arc<Mutex<Option<Error>>>,
}

impl Future for Request {
    type Output = SftpResult<Packet>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(result) = Pin::new(&mut self.response).poll(cx) {
            self.completed = true;
            return Poll::Ready(result.unwrap_or_else(|_| {
                Err(self
                    .terminal
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or(Error::Closed))
            }));
        }

        let _ = ready!(self.timeout.as_mut().poll(cx));
        self.requests.remove(&self.id);
        self.completed = true;
        Poll::Ready(Err(Error::Timeout))
    }
}

impl Drop for Request {
    fn drop(&mut self) {
        if !self.completed {
            self.requests.remove(&self.id);
        }
    }
}

pub(crate) struct SessionInner {
    version: Option<u32>,
    requests: Arc<SharedRequests>,
}

impl SessionInner {
    pub fn reply(&mut self, id: Option<u32>, packet: Packet) -> SftpResult<()> {
        if let Some((_, sender)) = self.requests.remove(&id) {
            let validate = if id.is_some() && self.version.is_none() {
                Err(Error::UnexpectedPacket)
            } else if id.is_none() && self.version.is_some() {
                Err(Error::UnexpectedBehavior("Duplicate version".to_owned()))
            } else {
                Ok(())
            };

            // Ignore send error: receiver was dropped (request timed out).
            let _ = sender.send(validate.clone().map(|_| packet));

            return validate;
        }

        // Reads may have been cancelled by seek, a short reply or a timeout.
        if id.is_some() {
            debug!("ignoring reply for completed or cancelled request {:?}", id);
            return Ok(());
        }

        Err(Error::UnexpectedBehavior(format!(
            "Packet {:?} for unknown recipient",
            id
        )))
    }
}

impl Drop for SessionInner {
    fn drop(&mut self) {
        // Wake pending requests when the transport stops.
        self.requests.clear();
    }
}

impl Handler for SessionInner {
    type Error = Error;

    fn closed(&mut self, error: Error) {
        // New registrations either precede the driver's terminal publication
        // or are refused by send and removed when their Request is dropped.
        self.requests.retain(|_, sender| {
            let (replacement, _) = oneshot::channel();
            let sender = std::mem::replace(sender, replacement);
            let _ = sender.send(Err(error.clone()));
            false
        });
    }

    async fn version(&mut self, packet: Version) -> Result<(), Self::Error> {
        let version = packet.version;
        self.reply(None, packet.into())?;
        self.version = Some(version);
        Ok(())
    }

    async fn name(&mut self, name: Name) -> Result<(), Self::Error> {
        self.reply(Some(name.id), name.into())
    }

    async fn status(&mut self, status: Status) -> Result<(), Self::Error> {
        self.reply(Some(status.id), status.into())
    }

    async fn handle(&mut self, handle: Handle) -> Result<(), Self::Error> {
        self.reply(Some(handle.id), handle.into())
    }

    async fn data(&mut self, data: Data) -> Result<(), Self::Error> {
        self.reply(Some(data.id), data.into())
    }

    async fn attrs(&mut self, attrs: Attrs) -> Result<(), Self::Error> {
        self.reply(Some(attrs.id), attrs.into())
    }

    async fn extended_reply(&mut self, reply: ExtendedReply) -> Result<(), Self::Error> {
        self.reply(Some(reply.id), reply.into())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Limits {
    pub packet_len: Option<u64>,
    pub read_len: Option<u64>,
    pub write_len: Option<u64>,
    pub open_handles: Option<u64>,
}

impl From<LimitsExtension> for Limits {
    fn from(limits: LimitsExtension) -> Self {
        Self {
            packet_len: (limits.max_packet_len > 0).then_some(limits.max_packet_len),
            read_len: (limits.max_read_len > 0).then_some(limits.max_read_len),
            write_len: (limits.max_write_len > 0).then_some(limits.max_write_len),
            open_handles: (limits.max_open_handles > 0).then_some(limits.max_open_handles),
        }
    }
}

/// Implements raw work with the protocol in request-response format.
/// If the server returns a `Status` packet and it has the code Ok
/// then the packet is returned as Ok in other error cases
/// the packet is stored as Err.
pub struct RawSftpSession {
    tx: super::Driver,
    requests: Arc<SharedRequests>,
    next_req_id: AtomicU64,
    handles: Mutex<std::collections::HashSet<Vec<u8>>>,
    timeout: AtomicU64,
    limits: Limits,
    maximum_packet: u32,
}

macro_rules! into_with_status {
    ($result:ident, $packet:ident) => {
        match $result {
            Packet::$packet(p) => Ok(p),
            Packet::Status(p) => Err(p.into()),
            _ => Err(Error::UnexpectedPacket),
        }
    };
}

macro_rules! into_status {
    ($result:ident) => {
        match $result {
            Packet::Status(status) if status.status_code == StatusCode::Ok => Ok(status),
            Packet::Status(status) => Err(status.into()),
            _ => Err(Error::UnexpectedPacket),
        }
    };
}

/// A READ admitted synchronously, whose reply is checked as [`RawSftpSession::read`]
/// checks it: DATA of at most the requested length, or the server's STATUS as an error.
/// Dropping it cancels only reply observation; it never retries the request.
#[must_use = "await the request to observe its reply"]
pub struct ReadRequest {
    request: Request,
    len: u32,
}

impl Future for ReadRequest {
    type Output = SftpResult<Data>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let len = self.len;
        let packet = ready!(Pin::new(&mut self.request).poll(cx))?;
        Poll::Ready(data_reply(packet, len))
    }
}

/// The DATA a READ of `len` bytes answers, or its STATUS as an error.
fn data_reply(result: Packet, len: u32) -> SftpResult<Data> {
    if let Packet::Data(data) = &result {
        if data.data.len() > len as usize {
            return Err(Error::UnexpectedPacket);
        }
    }
    into_with_status!(result, Data)
}

impl RawSftpSession {
    pub fn new<S>(stream: S) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::new_with_config(stream, Config::default())
    }

    pub fn new_with_config<S>(stream: S, cfg: Config) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let req_map = Arc::new(HashMap::new());
        let inner = SessionInner {
            version: None,
            requests: req_map.clone(),
        };

        Self {
            tx: run_with_config(stream, inner, &cfg),
            requests: req_map,
            next_req_id: AtomicU64::new(1),
            handles: Mutex::new(std::collections::HashSet::new()),
            timeout: AtomicU64::new(cfg.request_timeout_secs),
            limits: Limits::default(),
            maximum_packet: cfg.max_packet_len,
        }
    }

    /// Set the maximum response time in seconds.
    /// Default: 10 seconds
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    pub fn set_timeout(&self, secs: u64) {
        self.timeout.store(secs, Ordering::Relaxed);
    }

    /// Setting limits. For the `limits@openssh.com` extension
    pub fn set_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    fn send(&self, id: Option<u32>, packet: Packet) -> SftpResult<Request> {
        self.send_bytes(id, Bytes::try_from(packet)?)
    }

    fn send_bytes(&self, id: Option<u32>, bytes: Bytes) -> SftpResult<Request> {
        if bytes.len().saturating_sub(4) > self.maximum_packet as usize {
            return Err(Error::Limited(
                "packet exceeds configured transport bound".into(),
            ));
        }
        if let Some(max_len) = self.limits.packet_len {
            if bytes.len() as u64 > max_len {
                return Err(Error::Limited("packet exceeds server limit".to_owned()));
            }
        }

        let (tx, rx) = oneshot::channel();
        let timeout = Duration::from_secs(self.timeout.load(Ordering::Relaxed));
        let timeout = runtime::timeout(timeout, std::future::pending());
        let mut request = Request {
            id,
            requests: self.requests.clone(),
            response: rx,
            timeout: Box::pin(timeout),
            completed: true,
            terminal: self.tx.terminal.clone(),
        };
        match self.requests.entry(id) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(tx);
            }
            dashmap::mapref::entry::Entry::Occupied(_) => {
                return Err(Error::UnexpectedBehavior(
                    "SFTP request identifier already in use".into(),
                ));
            }
        }
        request.completed = false;
        self.tx.send(bytes)?;

        Ok(request)
    }

    async fn request(&self, id: Option<u32>, packet: Packet) -> SftpResult<Packet> {
        self.send(id, packet)?.await
    }

    fn use_next_id(&self) -> SftpResult<u32> {
        self.next_req_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| {
                (id <= u32::MAX as u64).then_some(id + 1)
            })
            .map(|id| id as u32)
            .map_err(|_| {
                Error::Limited(
                    "SFTP request identifier space exhausted; open a new subsystem".into(),
                )
            })
    }

    /// Closes the inner channel stream. Called by [`Drop`]
    pub fn close_session(&self) -> SftpResult<()> {
        self.tx.close();
        Ok(())
    }
    /// Cancel both transport halves and wait until they have released the stream.
    pub async fn shutdown(&self) {
        self.tx.shutdown().await;
    }

    pub async fn init(&self) -> SftpResult<Version> {
        let result = self.request(None, Init::default().into()).await?;
        if let Packet::Version(version) = result {
            Ok(version)
        } else {
            Err(Error::UnexpectedPacket)
        }
    }

    pub async fn open<T: Into<String>>(
        &self,
        filename: T,
        flags: OpenFlags,
        attrs: FileAttributes,
    ) -> SftpResult<Handle> {
        if self
            .limits
            .open_handles
            .is_some_and(|h| self.handles.lock().unwrap().len() as u64 >= h)
        {
            return Err(Error::Limited("handle limit reached".to_owned()));
        }

        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                Open {
                    id,
                    filename: filename.into(),
                    pflags: flags,
                    attrs,
                }
                .into(),
            )
            .await?;

        if let Packet::Handle(handle) = &result {
            if handle.handle.len() > 256 {
                return Err(Error::UnexpectedPacket);
            }
            self.handles.lock().unwrap().insert(handle.handle.clone());
        }

        into_with_status!(result, Handle)
    }

    pub async fn close<H: Into<Vec<u8>>>(&self, handle: H) -> SftpResult<Status> {
        let result = self.close_submit(handle.into())?.await?;
        into_status!(result)
    }

    /// Submit CLOSE synchronously, returning ownership of the pending reply.
    ///
    /// On success the handle is retired before this method returns, even if the
    /// reply future is later cancelled or the peer rejects CLOSE. On synchronous
    /// refusal the handle stays open. The future returns the original packet;
    /// callers must inspect its STATUS code, as `close` does.
    pub fn close_submit(&self, handle: Vec<u8>) -> SftpResult<Request> {
        // Admission is synchronous. Keep the handle until it succeeds and
        // serialize repeated closes so neither can release a sibling's slot.
        let mut handles = self.handles.lock().unwrap();
        if !handles.contains(&handle) {
            return Err(Error::UnexpectedBehavior("SFTP handle is not open".into()));
        }
        let id = self.use_next_id()?;
        let request = self.send(
            Some(id),
            Close {
                id,
                handle: handle.clone(),
            }
            .into(),
        )?;
        handles.remove(&handle);
        Ok(request)
    }

    pub async fn read<H: Into<Vec<u8>>>(
        &self,
        handle: H,
        offset: u64,
        len: u32,
    ) -> SftpResult<Data> {
        if self.limits.read_len.is_some_and(|r| len as u64 > r) {
            return Err(Error::Limited("read limit reached".to_owned()));
        }

        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                Read {
                    id,
                    handle: handle.into(),
                    offset,
                    len,
                }
                .into(),
            )
            .await?;

        data_reply(result, len)
    }

    /// Submit READ synchronously for a handle this session holds open, returning
    /// ownership of the pending reply.
    ///
    /// Admission shares [`Self::close_submit`]'s handle lock: once CLOSE of a
    /// handle was admitted, no READ of it is sent. A handle not open, or a refused
    /// admission, sends nothing and leaves no pending request.
    pub fn read_submit(&self, handle: &[u8], offset: u64, len: u32) -> SftpResult<ReadRequest> {
        let handles = self.handles.lock().unwrap();
        if !handles.contains(handle) {
            return Err(Error::UnexpectedBehavior("SFTP handle is not open".into()));
        }
        let request = self.read_nowait(handle, offset, len)?;
        Ok(ReadRequest { request, len })
    }

    pub async fn write<H: Into<Vec<u8>>>(
        &self,
        handle: H,
        offset: u64,
        data: Vec<u8>,
    ) -> SftpResult<Status> {
        if self.limits.write_len.is_some_and(|w| data.len() as u64 > w) {
            return Err(Error::Limited("write limit reached".to_owned()));
        }

        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                Write {
                    id,
                    handle: handle.into(),
                    offset,
                    data,
                }
                .into(),
            )
            .await?;

        into_status!(result)
    }

    /// Sends a borrowed write without awaiting the server's acknowledgement.
    pub(crate) fn write_nowait_from_slice(
        &self,
        handle: &[u8],
        offset: u64,
        data: &[u8],
    ) -> SftpResult<Request> {
        if self
            .limits
            .write_len
            .is_some_and(|limit| data.len() as u64 > limit)
        {
            return Err(Error::Limited("write limit reached".to_owned()));
        }

        let id = self.use_next_id()?;
        self.send_bytes(
            Some(id),
            Bytes::try_from(WriteRef {
                id,
                handle,
                offset,
                data,
            })?,
        )
    }

    pub(crate) fn read_nowait(&self, handle: &[u8], offset: u64, len: u32) -> SftpResult<Request> {
        if self.limits.read_len.is_some_and(|limit| len as u64 > limit) {
            return Err(Error::Limited("read limit reached".to_owned()));
        }

        let id = self.use_next_id()?;
        self.send(
            Some(id),
            Read {
                id,
                handle: handle.to_owned(),
                offset,
                len,
            }
            .into(),
        )
    }

    pub async fn lstat<P: Into<String>>(&self, path: P) -> SftpResult<Attrs> {
        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                Lstat {
                    id,
                    path: path.into(),
                }
                .into(),
            )
            .await?;

        into_with_status!(result, Attrs)
    }

    pub async fn fstat<H: Into<Vec<u8>>>(&self, handle: H) -> SftpResult<Attrs> {
        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                Fstat {
                    id,
                    handle: handle.into(),
                }
                .into(),
            )
            .await?;

        into_with_status!(result, Attrs)
    }

    pub async fn setstat<P: Into<String>>(
        &self,
        path: P,
        attrs: FileAttributes,
    ) -> SftpResult<Status> {
        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                SetStat {
                    id,
                    path: path.into(),
                    attrs,
                }
                .into(),
            )
            .await?;

        into_status!(result)
    }

    pub async fn fsetstat<H: Into<Vec<u8>>>(
        &self,
        handle: H,
        attrs: FileAttributes,
    ) -> SftpResult<Status> {
        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                FSetStat {
                    id,
                    handle: handle.into(),
                    attrs,
                }
                .into(),
            )
            .await?;

        into_status!(result)
    }

    pub async fn opendir<P: Into<String>>(&self, path: P) -> SftpResult<Handle> {
        if self
            .limits
            .open_handles
            .is_some_and(|h| self.handles.lock().unwrap().len() as u64 >= h)
        {
            return Err(Error::Limited("Handle limit reached".to_owned()));
        }

        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                OpenDir {
                    id,
                    path: path.into(),
                }
                .into(),
            )
            .await?;

        if let Packet::Handle(handle) = &result {
            if handle.handle.len() > 256 {
                return Err(Error::UnexpectedPacket);
            }
            self.handles.lock().unwrap().insert(handle.handle.clone());
        }

        into_with_status!(result, Handle)
    }

    pub async fn readdir<H: Into<Vec<u8>>>(&self, handle: H) -> SftpResult<Name> {
        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                ReadDir {
                    id,
                    handle: handle.into(),
                }
                .into(),
            )
            .await?;

        into_with_status!(result, Name)
    }

    pub async fn remove<T: Into<String>>(&self, filename: T) -> SftpResult<Status> {
        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                Remove {
                    id,
                    filename: filename.into(),
                }
                .into(),
            )
            .await?;

        into_status!(result)
    }

    pub async fn mkdir<P: Into<String>>(
        &self,
        path: P,
        attrs: FileAttributes,
    ) -> SftpResult<Status> {
        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                MkDir {
                    id,
                    path: path.into(),
                    attrs,
                }
                .into(),
            )
            .await?;

        into_status!(result)
    }

    pub async fn rmdir<P: Into<String>>(&self, path: P) -> SftpResult<Status> {
        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                RmDir {
                    id,
                    path: path.into(),
                }
                .into(),
            )
            .await?;

        into_status!(result)
    }

    pub async fn realpath<P: Into<String>>(&self, path: P) -> SftpResult<Name> {
        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                RealPath {
                    id,
                    path: path.into(),
                }
                .into(),
            )
            .await?;

        into_with_status!(result, Name)
    }

    pub async fn stat<P: Into<String>>(&self, path: P) -> SftpResult<Attrs> {
        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                Stat {
                    id,
                    path: path.into(),
                }
                .into(),
            )
            .await?;

        into_with_status!(result, Attrs)
    }

    pub async fn rename<O, N>(&self, oldpath: O, newpath: N) -> SftpResult<Status>
    where
        O: Into<String>,
        N: Into<String>,
    {
        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                Rename {
                    id,
                    oldpath: oldpath.into(),
                    newpath: newpath.into(),
                }
                .into(),
            )
            .await?;

        into_status!(result)
    }

    pub async fn readlink<P: Into<String>>(&self, path: P) -> SftpResult<Name> {
        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                ReadLink {
                    id,
                    path: path.into(),
                }
                .into(),
            )
            .await?;

        into_with_status!(result, Name)
    }

    pub async fn symlink<P, T>(&self, path: P, target: T) -> SftpResult<Status>
    where
        P: Into<String>,
        T: Into<String>,
    {
        let id = self.use_next_id()?;
        let result = self
            .request(
                Some(id),
                Symlink {
                    id,
                    linkpath: path.into(),
                    targetpath: target.into(),
                }
                .into(),
            )
            .await?;

        into_status!(result)
    }

    /// OpenSSH's deployed SFTP v3 dialect reverses the two wire fields.
    pub async fn symlink_openssh(
        &self,
        linkpath: String,
        targetpath: String,
    ) -> SftpResult<Status> {
        self.symlink(targetpath, linkpath).await
    }

    /// Equivalent to `SSH_FXP_EXTENDED`. Allows protocol expansion.
    /// The extension can return any packet, so it's not specific
    pub async fn extended<R: Into<String>>(&self, request: R, data: Vec<u8>) -> SftpResult<Packet> {
        let id = self.use_next_id()?;
        self.request(
            Some(id),
            Extended {
                id,
                request: request.into(),
                data,
            }
            .into(),
        )
        .await
    }

    pub async fn limits(&self) -> SftpResult<LimitsExtension> {
        match self.extended(extensions::LIMITS, vec![]).await? {
            Packet::ExtendedReply(reply) => decode_extension(reply.data),
            Packet::Status(status) if status.status_code != StatusCode::Ok => {
                Err(Error::Status(status))
            }
            _ => Err(Error::UnexpectedPacket),
        }
    }

    pub async fn hardlink<O, N>(&self, oldpath: O, newpath: N) -> SftpResult<Status>
    where
        O: Into<String>,
        N: Into<String>,
    {
        let result = self
            .extended(
                extensions::HARDLINK,
                HardlinkExtension {
                    oldpath: oldpath.into(),
                    newpath: newpath.into(),
                }
                .try_into()?,
            )
            .await?;

        into_status!(result)
    }

    pub async fn fsync<H: Into<Vec<u8>>>(&self, handle: H) -> SftpResult<Status> {
        let result = self
            .extended(
                extensions::FSYNC,
                FsyncExtension {
                    handle: handle.into(),
                }
                .try_into()?,
            )
            .await?;

        into_status!(result)
    }

    pub async fn statvfs<P>(&self, path: P) -> SftpResult<Statvfs>
    where
        P: Into<String>,
    {
        let result = self
            .extended(
                extensions::STATVFS,
                StatvfsExtension { path: path.into() }.try_into()?,
            )
            .await?;

        match result {
            Packet::ExtendedReply(reply) => decode_extension(reply.data),
            Packet::Status(status) if status.status_code != StatusCode::Ok => {
                Err(Error::Status(status))
            }
            _ => Err(Error::UnexpectedPacket),
        }
    }

    pub async fn posix_rename(&self, oldpath: String, newpath: String) -> SftpResult<Status> {
        let result = self
            .extended(
                extensions::POSIX_RENAME,
                extensions::PosixRenameExtension { oldpath, newpath }.try_into()?,
            )
            .await?;
        into_status!(result)
    }
    pub async fn fstatvfs(&self, handle: Vec<u8>) -> SftpResult<Statvfs> {
        match self
            .extended(
                extensions::FSTATVFS,
                extensions::FstatvfsExtension { handle }.try_into()?,
            )
            .await?
        {
            Packet::ExtendedReply(reply) => {
                let mut bytes = reply.data.into();
                let result = de::from_bytes(&mut bytes)?;
                if !bytes.is_empty() {
                    return Err(Error::UnexpectedPacket);
                }
                Ok(result)
            }
            Packet::Status(status) => Err(Error::Status(status)),
            _ => Err(Error::UnexpectedPacket),
        }
    }
    pub async fn copy_data(&self, request: extensions::CopyDataExtension) -> SftpResult<Status> {
        let result = self
            .extended(extensions::COPY_DATA, request.try_into()?)
            .await?;
        into_status!(result)
    }

    /// Expands `~`/`~user` and canonicalizes the path.
    /// Replies in the same format as [`RawSftpSession::realpath`]
    pub async fn expand_path<P: Into<String>>(&self, path: P) -> SftpResult<Name> {
        let result = self
            .extended(
                extensions::EXPAND_PATH,
                ExpandPathExtension { path: path.into() }.try_into()?,
            )
            .await?;

        into_with_status!(result, Name)
    }
}

impl Drop for RawSftpSession {
    fn drop(&mut self) {
        let _ = self.close_session();
    }
}

fn decode_extension<T: serde::de::DeserializeOwned>(data: Vec<u8>) -> SftpResult<T> {
    let mut bytes = Bytes::from(data);
    let result = de::from_bytes(&mut bytes)?;
    if !bytes.is_empty() {
        return Err(Error::UnexpectedPacket);
    }
    Ok(result)
}

#[cfg(test)]
mod close_admission_tests {
    use super::*;

    #[tokio::test]
    async fn admitted_close_releases_only_its_handle_even_if_cancelled() {
        let (client, _peer) = tokio::io::duplex(1);
        let session = RawSftpSession::new(client);
        session
            .handles
            .lock()
            .unwrap()
            .extend([vec![0, 255], vec![1]]);
        let request = session.close_submit(vec![0, 255]).unwrap();
        assert_eq!(session.handles.lock().unwrap().len(), 1);
        drop(request);
        assert!(session.close_submit(vec![0, 255]).is_err());
        assert!(session.handles.lock().unwrap().contains(&vec![1]));
        session.shutdown().await;
    }

    #[tokio::test]
    async fn refused_close_keeps_handle_and_request_registration_is_removed() {
        let (client, _peer) = tokio::io::duplex(1);
        let session = RawSftpSession::new(client);
        session.handles.lock().unwrap().insert(vec![0, 255]);
        // No await: the writer cannot drain this bounded output queue.
        for _ in 0..32 {
            session.tx.send(Bytes::from_static(b"queued")).unwrap();
        }
        assert!(matches!(
            session.close_submit(vec![0, 255]),
            Err(Error::Limited(_))
        ));
        assert_eq!(session.handles.lock().unwrap().len(), 1);
        assert!(session.requests.is_empty());
        session.shutdown().await;
    }

    #[tokio::test]
    async fn read_is_admitted_only_for_a_handle_held_open() {
        let (client, _peer) = tokio::io::duplex(1);
        let session = RawSftpSession::new(client);
        assert!(matches!(
            session.read_submit(&[7], 0, 4),
            Err(Error::UnexpectedBehavior(_))
        ));
        assert!(session.requests.is_empty());
        session.handles.lock().unwrap().insert(vec![7]);
        let read = session.read_submit(&[7], 0, 4).unwrap();
        assert_eq!(session.requests.len(), 1);
        drop(read);
        // Once CLOSE is admitted no READ of the handle is sent.
        let close = session.close_submit(vec![7]).unwrap();
        assert!(matches!(
            session.read_submit(&[7], 0, 4),
            Err(Error::UnexpectedBehavior(_))
        ));
        assert_eq!(session.requests.len(), 1);
        drop(close);
        session.shutdown().await;
    }

    #[tokio::test]
    async fn refused_read_admission_leaves_no_pending_request() {
        let (client, _peer) = tokio::io::duplex(1);
        let session = RawSftpSession::new(client);
        session.handles.lock().unwrap().insert(vec![7]);
        for _ in 0..32 {
            session.tx.send(Bytes::from_static(b"queued")).unwrap();
        }
        assert!(matches!(
            session.read_submit(&[7], 0, 4),
            Err(Error::Limited(_))
        ));
        assert!(session.requests.is_empty());
        session.shutdown().await;
    }

    #[tokio::test]
    async fn a_submitted_read_checks_its_reply_as_read_does() {
        let (client, _peer) = tokio::io::duplex(1);
        let session = RawSftpSession::new(client);
        session.handles.lock().unwrap().insert(vec![7]);
        let reply = |packet: Packet| {
            let read = session.read_submit(&[7], 0, 4).unwrap();
            let id = read.request.id.unwrap();
            let (_, sender) = session.requests.remove(&Some(id)).unwrap();
            sender.send(Ok(packet)).unwrap();
            read
        };
        let data = |data: &[u8]| {
            Packet::Data(Data {
                id: 0,
                data: data.to_vec(),
            })
        };
        assert_eq!(reply(data(b"four")).await.unwrap().data, b"four");
        assert!(matches!(
            reply(data(b"fives")).await,
            Err(Error::UnexpectedPacket)
        ));
        assert!(matches!(
            reply(Packet::status(0, StatusCode::Eof, "end", "")).await,
            Err(Error::Status(Status {
                status_code: StatusCode::Eof,
                ..
            }))
        ));
        session.shutdown().await;
    }

    #[tokio::test]
    async fn failed_close_reply_does_not_restore_an_invalid_handle() {
        let (client, _peer) = tokio::io::duplex(1);
        let session = RawSftpSession::new(client);
        session.handles.lock().unwrap().insert(vec![128]);
        let request = session.close_submit(vec![128]).unwrap();
        let id = request.id.unwrap();
        let (_, reply) = session.requests.remove(&Some(id)).unwrap();
        reply
            .send(Ok(Packet::status(
                id,
                StatusCode::Failure,
                "close failed",
                "",
            )))
            .unwrap();
        let result = request.await.unwrap();
        assert!(matches!(
            result,
            Packet::Status(Status {
                status_code: StatusCode::Failure,
                ..
            })
        ));
        assert!(session.handles.lock().unwrap().is_empty());
        session.shutdown().await;
    }
}
