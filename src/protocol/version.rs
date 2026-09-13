use super::{impl_packet_for, Packet, VERSION};

/// An extension name and opaque value. Duplicate announcements are retained.
#[derive(Debug, Clone)]
pub struct Extension {
    pub name: String,
    pub data: Vec<u8>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Version {
    pub version: u32,
    #[serde(with = "announcements")]
    pub extensions: Vec<Extension>,
}
impl_packet_for!(Version);
impl Version {
    pub fn new() -> Self {
        Self {
            version: VERSION,
            extensions: Vec::new(),
        }
    }
    pub fn supports(&self, name: &str, version: &[u8]) -> bool {
        self.extensions
            .iter()
            .any(|e| e.name == name && e.data == version)
    }
}
impl Default for Version {
    fn default() -> Self {
        Self::new()
    }
}
mod announcements {
    use super::Extension;
    use serde::{
        de::{MapAccess, Visitor},
        ser::SerializeMap,
        Deserializer, Serializer,
    };
    pub fn serialize<S: Serializer>(
        values: &[Extension],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(values.len()))?;
        for value in values {
            map.serialize_entry(&value.name, serde_bytes::Bytes::new(&value.data))?;
        }
        map.end()
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Extension>, D::Error> {
        struct Announcements;
        impl<'de> Visitor<'de> for Announcements {
            type Value = Vec<Extension>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("SFTP extension announcements")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some((name, data)) = map.next_entry::<String, serde_bytes::ByteBuf>()? {
                    values.push(Extension {
                        name,
                        data: data.into_vec(),
                    });
                }
                Ok(values)
            }
        }
        d.deserialize_map(Announcements)
    }
}
