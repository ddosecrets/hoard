use std::io::{Read, Seek};

pub fn list_files<R: Read + Seek>(
    file_name: &str,
    reader: R,
) -> anyhow::Result<Vec<(String, u64)>> {
    match file_name.split('.').last() {
        Some("tar") => list_tar_files(reader),
        Some("zip") => list_zip_files(reader),
        _ => Ok(Vec::new()),
    }
}

fn list_tar_files<R: Read>(reader: R) -> anyhow::Result<Vec<(String, u64)>> {
    let mut archive = tar::Archive::new(reader);
    let entries = match archive.entries() {
        Ok(entries) => entries,
        Err(e) => bail!("Could not list entries of archive: {}", e),
    };

    let mut has_error = false;
    let mut data = Vec::new();
    for entry in entries {
        let entry = entry?;
        let path = entry.path()?;
        match path.to_str() {
            Some(path) => {
                if !has_error {
                    data.push((path.to_string(), entry.size()));
                }
            }
            None => {
                log::error!("Path was not UTF-8: {}", path.to_string_lossy());
                has_error = true;
            }
        }
    }

    if has_error {
        bail!("Some paths in the tar file were not UTF-8. See logs for details.")
    }
    Ok(data)
}

fn list_zip_files<R: Read + Seek>(reader: R) -> anyhow::Result<Vec<(String, u64)>> {
    let mut zip = zip::ZipArchive::new(reader)?;
    let mut data = Vec::new();
    for i in 0..zip.len() {
        let file = zip.by_index(i)?;
        data.push((file.name().to_string(), file.size()));
    }
    Ok(data)
}