use bytes::{Buf, Bytes};
use russh_sftp::{
    client::{Config, RawSftpSession},
    protocol::*,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn opaque_handles_extended_attributes_unknown_status_and_announcements() {
    let packet = Packet::Handle(Handle {
        id: 9,
        handle: vec![0, 255, 128],
    });
    let mut wire = Bytes::try_from(packet).unwrap();
    wire.advance(4);
    let Packet::Handle(decoded) = Packet::try_from(&mut wire).unwrap() else {
        panic!()
    };
    assert_eq!(decoded.handle, [0, 255, 128]);
    let attrs = FileAttributes {
        size: Some(u64::MAX),
        extended: vec![ExtendedAttribute {
            kind: vec![255],
            data: vec![0, 128],
        }],
        ..Default::default()
    };
    let mut wire = Bytes::try_from(Packet::Attrs(Attrs { id: 1, attrs })).unwrap();
    wire.advance(4);
    let Packet::Attrs(decoded) = Packet::try_from(&mut wire).unwrap() else {
        panic!()
    };
    assert_eq!(decoded.attrs.size, Some(u64::MAX));
    assert_eq!(decoded.attrs.extended[0].data, [0, 128]);
    let mut wire =
        Bytes::try_from(Packet::status(1, StatusCode(10000), "peer detail", "zh")).unwrap();
    wire.advance(4);
    let Packet::Status(decoded) = Packet::try_from(&mut wire).unwrap() else {
        panic!()
    };
    assert_eq!(decoded.status_code.0, 10000);
    let mut wire = Bytes::try_from(Packet::Version(Version {
        version: 3,
        extensions: vec![
            Extension {
                name: "x".into(),
                data: b"1".to_vec(),
            },
            Extension {
                name: "x".into(),
                data: vec![255],
            },
        ],
    }))
    .unwrap();
    wire.advance(4);
    let Packet::Version(version) = Packet::try_from(&mut wire).unwrap() else {
        panic!()
    };
    assert_eq!(version.extensions.len(), 2);
    assert_eq!(version.extensions[1].data, [255]);
}

#[test]
fn malformed_attributes_counts_and_trailing_data_are_rejected() {
    for bytes in [
        vec![105, 0, 0, 0, 1, 0, 0, 0, 1],  // size flag with missing u64
        vec![105, 0, 0, 0, 1, 0, 0, 0, 16], // unknown flags
        vec![105, 0, 0, 0, 1, 128, 0, 0, 0, 255, 255, 255, 255], // impossible extended count
        vec![105, 0, 0, 0, 1, 0, 0, 0, 0, 99], // trailing byte
    ] {
        assert!(Packet::try_from(&mut Bytes::from(bytes)).is_err());
    }
}

#[tokio::test]
async fn cancelling_blocked_writer_joins_both_halves() {
    let (client, _peer) = tokio::io::duplex(1);
    let session = RawSftpSession::new(client);
    let mut init = Box::pin(session.init());
    tokio::select! { _ = &mut init => panic!("peer has not read INIT"), _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {} }
    tokio::time::timeout(std::time::Duration::from_millis(250), session.shutdown())
        .await
        .unwrap();
    assert!(init.await.is_err());
    assert!(session.is_closed());
}

#[tokio::test]
async fn raw_client_rejects_length_before_reading_payload() {
    let (client, mut peer) = tokio::io::duplex(32);
    let session = RawSftpSession::new_with_config(
        client,
        Config {
            max_packet_len: 64,
            ..Default::default()
        },
    );
    let (result, ()) = tokio::join!(session.init(), async {
        let len = peer.read_u32().await.unwrap();
        let mut init = vec![0; len as usize];
        peer.read_exact(&mut init).await.unwrap();
        peer.write_u32(65).await.unwrap();
    });
    assert!(result.is_err());
    session.shutdown().await;
}
