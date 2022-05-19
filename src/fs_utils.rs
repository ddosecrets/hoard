use std::fs;
use std::path::{Component, Path, PathBuf};

/// Create all directories of the `suffix` if they don't exist. Assumes the suffix is a stripped
/// absolute path.
pub fn create_dirs_from(prefix: impl AsRef<Path>, suffix: impl AsRef<Path>) -> anyhow::Result<()> {
    let mut path = prefix.as_ref().to_owned();
    for component in suffix.as_ref().components() {
        if let Component::Normal(part) = component {
            path.push(part);
            if !Path::exists(&path) {
                log::debug!("Path does not exist. Creating: {}", path.to_string_lossy());
                fs::create_dir(&path)?;
            }
        }
    }
    Ok(())
}

pub fn canonical_path(in_path: impl AsRef<Path>) -> Result<PathBuf, String> {
    let in_path = PathBuf::from(in_path.as_ref());
    let mut out_path = PathBuf::new();

    for (i, component) in in_path.as_path().components().enumerate() {
        match (i, component) {
            (0, Component::RootDir) => out_path.push("/"),
            (0, _) => return Err("Path was not absolute (must begin with `/`)".to_string()),
            (_, Component::CurDir) => {
                return Err("Path was not absolute (cannot contain `.`)".to_string())
            }
            (_, Component::ParentDir) => {
                return Err("Path was not absolute (cannot contain `..`)".to_string())
            }
            (_, Component::Normal(part)) => out_path.push(part),
            _ => unreachable!(),
        }
    }

    Ok(out_path)
}

/// Assuming a canonical, absolute path: remove the leading `/`
pub fn strip_root(path: impl AsRef<Path>) -> PathBuf {
    let mut stripped_path = PathBuf::new();
    for component in path.as_ref().components() {
        if let Component::Normal(part) = component {
            stripped_path.push(part)
        }
    }
    stripped_path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_root() {
        let input = PathBuf::from("/foo/bar");
        let expected = PathBuf::from("foo/bar");
        assert_eq!(strip_root(input), expected);

        let input = PathBuf::from("foo/bar");
        let expected = PathBuf::from("foo/bar");
        assert_eq!(strip_root(input), expected);
    }
}
