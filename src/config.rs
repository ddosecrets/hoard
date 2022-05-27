use crate::hash_utils::HashAlgorithm;
use std::fs::File;
use std::path::Path;

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    db: DbConfig,
    files: FileConfig,
}

impl Config {
    pub fn from_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(serde_yaml::from_reader(File::open(path)?)?)
    }

    pub fn db(&self) -> &DbConfig {
        &self.db
    }

    pub fn files(&self) -> &FileConfig {
        &self.files
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct DbConfig {
    path: String,
}

impl DbConfig {
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            path: "db.sqlite".to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct FileConfig {
    hashes: Vec<HashAlgorithm>,
}

impl FileConfig {
    pub fn hashes(&self) -> &[HashAlgorithm] {
        &self.hashes
    }
}

impl Default for FileConfig {
    fn default() -> Self {
        Self {
            hashes: vec![
                HashAlgorithm::Sha256,
                HashAlgorithm::Sha512,
                HashAlgorithm::Sha3_256,
            ],
        }
    }
}
