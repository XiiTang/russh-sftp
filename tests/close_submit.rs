use bytes::Bytes;
use futures::FutureExt;
use russh_sftp::{
    client::{rawsession::Request, RawSftpSession},
    protocol::*,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

async fn read(peer: &mut DuplexStream) -> Packet {
    let len = peer.read_u32().await.unwrap();
    let mut bytes = vec![0; len as usize];
    peer.read_exact(&mut bytes).await.unwrap();
    Packet::try_from(&mut Bytes::from(bytes)).unwrap()
}

async fn send(peer: &mut DuplexStream, packet: Packet) {
    peer.write_all(&Bytes::try_from(packet).unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn close_submit_keeps_the_ack_after_synchronous_admission() {
    // This budget only detects a stalled fixture; ordering uses the wire and signals.
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let (client, mut peer) = tokio::io::duplex(4096);
        let session = RawSftpSession::new(client);
        let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let owner = async {
            session.init().await.unwrap();
            let first = session
                .open("first", OpenFlags::READ, FileAttributes::default())
                .await
                .unwrap();
            let sibling = session
                .open("sibling", OpenFlags::READ, FileAttributes::default())
                .await
                .unwrap();
            let request: Request = session.close_submit(first.handle.clone()).unwrap();
            assert!(session.close_submit(first.handle).is_err());
            let mut request = Box::pin(request);
            seen_rx.await.unwrap();
            assert!(request.as_mut().now_or_never().is_none());
            reply_tx.send(()).unwrap();
            assert!(matches!(
                request.await.unwrap(),
                Packet::Status(Status {
                    status_code: StatusCode::Failure,
                    ..
                })
            ));
            session.fstat(sibling.handle).await.unwrap();
            session.shutdown().await;
        };
        let broker = async {
            assert!(matches!(read(&mut peer).await, Packet::Init(_)));
            send(
                &mut peer,
                Packet::Version(Version {
                    version: 3,
                    extensions: vec![],
                }),
            )
            .await;
            for handle in [vec![0, 255], vec![1]] {
                let Packet::Open(open) = read(&mut peer).await else {
                    panic!("expected OPEN")
                };
                send(
                    &mut peer,
                    Packet::Handle(Handle {
                        id: open.id,
                        handle,
                    }),
                )
                .await;
            }
            let Packet::Close(close) = read(&mut peer).await else {
                panic!("expected CLOSE")
            };
            assert_eq!(close.handle, [0, 255]);
            seen_tx.send(()).unwrap();
            reply_rx.await.unwrap();
            send(
                &mut peer,
                Packet::status(close.id, StatusCode::Failure, "close failed", ""),
            )
            .await;
            let Packet::Fstat(stat) = read(&mut peer).await else {
                panic!("expected sibling FSTAT")
            };
            assert_eq!(stat.handle, [1]);
            send(
                &mut peer,
                Packet::Attrs(Attrs {
                    id: stat.id,
                    attrs: FileAttributes::default(),
                }),
            )
            .await;
        };
        tokio::join!(owner, broker);
    })
    .await
    .unwrap();
}
