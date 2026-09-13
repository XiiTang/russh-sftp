use super::{impl_packet_for, impl_request_id, Packet, RequestId};

/// Lossless SSH_FXP_STATUS code, including peer-defined extension values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StatusCode(pub u32);
#[allow(non_upper_case_globals)]
impl StatusCode {
    pub const Ok: Self = Self(0);
    pub const Eof: Self = Self(1);
    pub const NoSuchFile: Self = Self(2);
    pub const PermissionDenied: Self = Self(3);
    pub const Failure: Self = Self(4);
    pub const BadMessage: Self = Self(5);
    pub const NoConnection: Self = Self(6);
    pub const ConnectionLost: Self = Self(7);
    pub const OpUnsupported: Self = Self(8);
}
impl std::fmt::Display for StatusCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SFTP status {}", self.0)
    }
}

/// Implementation for SSH_FXP_STATUS as defined in the specification draft
/// <https://datatracker.ietf.org/doc/html/draft-ietf-secsh-filexfer-02#section-7>
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Status {
    pub id: u32,
    pub status_code: StatusCode,
    pub error_message: String,
    pub language_tag: String,
}

impl_request_id!(Status);
impl_packet_for!(Status);
