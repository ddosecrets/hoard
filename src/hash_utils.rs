use crate::error::GenericError;
use digest::Digest;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef};
use rusqlite::ToSql;
use sha1::Sha1;
use sha2::{Sha256, Sha384, Sha512};
use sha3::{Sha3_256, Sha3_384, Sha3_512};
use std::collections::HashMap;
use std::fmt;
use std::io::Read;
use std::iter::ExactSizeIterator;

pub fn make_hashes<'a, R: Read, I, II>(
    mut input: R,
    hash_algorithms: II,
) -> anyhow::Result<HashMap<&'a HashAlgorithm, Vec<u8>>>
where
    I: ExactSizeIterator<Item = &'a HashAlgorithm>,
    II: IntoIterator<IntoIter = I, Item = &'a HashAlgorithm>,
{
    let hash_algorithms = hash_algorithms.into_iter();

    // TODO make this use `is_empty` once this feature stabilizes:
    // https://github.com/rust-lang/rust/issues/35428
    if hash_algorithms.len() == 0 {
        return Ok(HashMap::new());
    }

    let mut hashes = HashMap::new();
    for name in hash_algorithms {
        hashes.insert(name, name.container());
    }

    let mut buf = vec![0u8; 4096];
    loop {
        let bytes_read = input.read(&mut buf)?;
        if bytes_read == 0 {
            break;
        }
        for (_, container) in hashes.iter_mut() {
            container.update(&buf[0..bytes_read]);
        }
    }

    Ok(hashes
        .drain()
        .map(|(name, container)| (name, container.finalize()))
        .collect())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum HashAlgorithm {
    #[serde(rename = "sha1")]
    Sha1,
    #[serde(rename = "sha2-256")]
    Sha256,
    #[serde(rename = "sha2-384")]
    Sha384,
    #[serde(rename = "sha2-512")]
    Sha512,
    #[serde(rename = "sha3-256")]
    Sha3_256,
    #[serde(rename = "sha3-384")]
    Sha3_384,
    #[serde(rename = "sha3-512")]
    Sha3_512,
}

impl HashAlgorithm {
    fn container(&self) -> HasherContainer {
        match self {
            Self::Sha1 => HasherContainer::Sha1(Sha1::new()),
            Self::Sha256 => HasherContainer::Sha256(Sha256::new()),
            Self::Sha384 => HasherContainer::Sha384(Sha384::new()),
            Self::Sha512 => HasherContainer::Sha512(Sha512::new()),
            Self::Sha3_256 => HasherContainer::Sha3_256(Sha3_256::new()),
            Self::Sha3_384 => HasherContainer::Sha3_384(Sha3_384::new()),
            Self::Sha3_512 => HasherContainer::Sha3_512(Sha3_512::new()),
        }
    }
}

impl fmt::Display for HashAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let val = match self {
            Self::Sha1 => "sha1".to_string(),
            Self::Sha256 => "sha2-256".to_string(),
            Self::Sha384 => "sha2-384".to_string(),
            Self::Sha512 => "sha2-512".to_string(),
            Self::Sha3_256 => "sha3-256".to_string(),
            Self::Sha3_384 => "sha3-384".to_string(),
            Self::Sha3_512 => "sha3-512".to_string(),
        };
        write!(f, "{}", val)
    }
}

impl TryFrom<&str> for HashAlgorithm {
    type Error = GenericError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "sha1" => Ok(Self::Sha1),
            "sha2-256" => Ok(Self::Sha256),
            "sha2-384" => Ok(Self::Sha384),
            "sha2-512" => Ok(Self::Sha512),
            "sha3-256" => Ok(Self::Sha3_256),
            "sha3-384" => Ok(Self::Sha3_384),
            "sha3-512" => Ok(Self::Sha3_512),
            x => Err(GenericError::new(format!(
                "Not a known hash algorithm: {x}"
            ))),
        }
    }
}

impl ToSql for HashAlgorithm {
    #[inline]
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}

impl FromSql for HashAlgorithm {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Text(bytes) => ::std::str::from_utf8(bytes)
                .map_err(|e| FromSqlError::Other(Box::new(e)))?
                .try_into()
                .map_err(|e| FromSqlError::Other(Box::new(e))),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

enum HasherContainer {
    Sha1(Sha1),
    Sha256(Sha256),
    Sha384(Sha384),
    Sha512(Sha512),
    Sha3_256(Sha3_256),
    Sha3_384(Sha3_384),
    Sha3_512(Sha3_512),
}

impl HasherContainer {
    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Sha1(hasher) => hasher.update(bytes),
            Self::Sha256(hasher) => hasher.update(bytes),
            Self::Sha384(hasher) => hasher.update(bytes),
            Self::Sha512(hasher) => hasher.update(bytes),
            Self::Sha3_256(hasher) => hasher.update(bytes),
            Self::Sha3_384(hasher) => hasher.update(bytes),
            Self::Sha3_512(hasher) => hasher.update(bytes),
        }
    }

    fn finalize(self) -> Vec<u8> {
        match self {
            Self::Sha1(hasher) => Vec::from(hasher.finalize().as_slice()),
            Self::Sha256(hasher) => Vec::from(hasher.finalize().as_slice()),
            Self::Sha384(hasher) => Vec::from(hasher.finalize().as_slice()),
            Self::Sha512(hasher) => Vec::from(hasher.finalize().as_slice()),
            Self::Sha3_256(hasher) => Vec::from(hasher.finalize().as_slice()),
            Self::Sha3_384(hasher) => Vec::from(hasher.finalize().as_slice()),
            Self::Sha3_512(hasher) => Vec::from(hasher.finalize().as_slice()),
        }
    }
}
