use crate::archive_utils;
use crate::config::Config;
use crate::db::init_connection;
use crate::db::types::{
    Collection, Disk, File, FileHash, FilePlacement, NewCollection, NewDisk, NewFile,
    NewFileArchive, NewFileHash, NewFilePlacement, NewPartition, Partition,
};
use crate::db::{auto_transaction, migrate};
use crate::dev_utils::{self, get_disk_for_path, get_partition_for_path, get_partition_for_uuid};
use crate::fs_utils::{canonical_path, create_dirs_from, strip_root};
use crate::hash_utils::make_hashes;
use regex::Regex;
use rusqlite::Connection;
use serde::Serialize;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub struct Manager {
    config: Config,
    conn: Connection,
}

impl Manager {
    pub fn new(config: Config, conn: Connection) -> Self {
        Self { config, conn }
    }

    pub(crate) fn conn(&mut self) -> &mut Connection {
        &mut self.conn
    }

    pub fn init(config_path: impl AsRef<Path>) -> anyhow::Result<()> {
        let config_path = config_path.as_ref();
        let config_dir = config_path.parent().unwrap(); // unwrap ok because files have parents
        if !config_dir.exists() {
            log::info!("Directory did not exist, creating");
            // TODO create in a way that allows setting of permissions
            fs::create_dir(config_dir)?;
        }

        let config = if !config_path.exists() {
            log::info!("Config file did not exist, writing default");
            let config = Config::default();
            serde_yaml::to_writer(fs::File::create(config_path)?, &config)?;
            config
        } else {
            serde_yaml::from_reader(fs::File::open(config_path)?)?
        };
        log::info!("Directories and config set up.");

        let conn = Connection::open(config.db().path())?;
        init_connection(&conn)?;

        let mut manager = Self { config, conn };
        manager.db_migrate()?;

        log::info!("DB migrated.");
        log::info!("Initialization completed.");
        Ok(())
    }

    pub fn db_migrate(&mut self) -> anyhow::Result<()> {
        migrate(&mut self.conn)
    }

    pub fn db_vacuum(&self) -> anyhow::Result<()> {
        self.conn.execute_batch("VACUUM").map_err(Into::into)
    }

    pub fn add_collection(&mut self, name: &str) -> anyhow::Result<()> {
        auto_transaction(&mut self.conn, |tx| {
            NewCollection { name }.insert(tx).map(|_| ())
        })?;
        log::info!("Collection added: {name}");
        Ok(())
    }

    pub fn list_collections(&self) -> anyhow::Result<Vec<Collection>> {
        Collection::all(&self.conn)
    }

    pub fn add_disk(&mut self, disk_path: &str, label: &str) -> anyhow::Result<()> {
        let disk = get_disk_for_path(disk_path)?;
        auto_transaction(&mut self.conn, |tx| {
            NewDisk {
                serial_number: disk.serial_number(),
                label,
            }
            .insert(tx)
            .map(|_| ())
        })?;
        log::info!("Disk added: {}", label);
        Ok(())
    }

    pub fn list_disks(&self) -> anyhow::Result<Vec<Disk>> {
        Disk::all(&self.conn)
    }

    pub fn add_partition(&mut self, disk_path: &str) -> anyhow::Result<()> {
        let (disk, partition) = get_partition_for_path(disk_path)?;
        auto_transaction(&mut self.conn, |tx| {
            NewPartition::create_for_disk(tx, &disk, &partition).map(|_| ())
        })?;
        log::info!("Partition added to disk: {}", disk.serial_number());
        Ok(())
    }

    pub fn list_partitions(&self) -> anyhow::Result<Vec<Partition>> {
        Partition::all(&self.conn)
    }

    pub fn add_file(
        &mut self,
        collection_id: &Uuid,
        partition_id: Option<&Uuid>,
        src_path: &str,
        dest_path: impl AsRef<Path>,
        move_file: bool,
    ) -> anyhow::Result<()> {
        Self::add_file_check_src_path(src_path)?;
        let dest_path = dest_path.as_ref();
        self.add_file_check_dest_path(collection_id, dest_path)?;

        let (db_part, part) = match partition_id {
            Some(id) => match Partition::for_id(&self.conn, id)? {
                Some(db_part) => {
                    let part = get_partition_for_uuid(db_part.uuid())?;
                    (db_part, part)
                }
                None => bail!("No partition was found for ID {}", id.hyphenated()),
            },
            None => self.random_partition()?,
        };

        let full_target_path = self.add_file_prep_target(&part, collection_id, dest_path)?;
        self.add_file_do_insert(
            collection_id,
            db_part.id(),
            src_path,
            dest_path,
            &full_target_path,
            move_file,
        )?;
        log::info!("File added: {}", dest_path.to_string_lossy());
        Ok(())
    }

    fn add_file_check_src_path(src_path: &str) -> anyhow::Result<()> {
        if Path::new(src_path).is_dir() {
            bail!("Source path cannot be a directory: {src_path}")
        }
        Ok(())
    }

    fn add_file_check_dest_path(
        &self,
        collection_id: &Uuid,
        dest_path: &Path,
    ) -> anyhow::Result<()> {
        // TODO handle dest_path being a directory somehow
        let dest_path = match canonical_path(dest_path.as_os_str()) {
            Ok(path) => path,
            Err(e) => bail!("{}", e),
        };
        if dest_path.ends_with("/") {
            bail!(
                "Destination path cannot be a directory: {}",
                dest_path.to_string_lossy()
            )
        }

        let mut ancestors = dest_path.ancestors().collect::<Vec<_>>();
        ancestors.reverse();
        for ancestor in dest_path.ancestors() {
            if let Some(file) = File::get_by_collection_and_path(
                &self.conn,
                collection_id,
                ancestor
                    .to_str()
                    .ok_or_else(|| anyhow!("Path was not UTF-8: {}", ancestor.to_string_lossy()))?,
            )? {
                bail!("Creation of directories for path {:?} would collide with the exiseting file {:?}", dest_path.to_string_lossy(), file.path())
            }
        }
        Ok(())
    }

    fn add_file_prep_target(
        &self,
        dev_partition: &dev_utils::Partition,
        collection_id: &Uuid,
        dest_path: &Path,
    ) -> anyhow::Result<PathBuf> {
        let target_path = Self::virtual_path(collection_id, dest_path)?;
        let target_dir = target_path.parent().ok_or_else(|| {
            anyhow!(
                "Unable to find parent directory for {}",
                target_path.to_string_lossy()
            )
        })?;
        create_dirs_from(&dev_partition.mount_point(), &target_dir)?;
        Ok(dev_partition.mount_point().join(&target_path))
    }

    fn add_file_do_insert(
        &mut self,
        collection_id: &Uuid,
        partition_id: &Uuid,
        src_path: &str,
        dest_path: &Path,
        full_target_path: &Path,
        move_file: bool,
    ) -> anyhow::Result<()> {
        let hashes = make_hashes(fs::File::open(src_path)?, self.config.files().hashes())?;
        let file_meta = fs::metadata(src_path)?;

        auto_transaction(&mut self.conn, |tx| {
            let file_id = NewFile {
                collection_id,
                path: dest_path
                    .to_str()
                    .ok_or_else(|| anyhow!("Path was not a UTF-8 string"))?,
                size: file_meta.size(),
            }
            .insert(tx)?;

            NewFilePlacement {
                partition_id,
                file_id: &file_id,
            }
            .insert(tx)?;

            for (hash_algorithm, hash_value) in hashes.iter() {
                NewFileHash {
                    file_id: &file_id,
                    hash_algorithm: hash_algorithm.to_string().as_str(),
                    hash_value,
                }
                .insert(tx)?;
            }

            for (archive_path, size) in
                archive_utils::list_files(src_path, fs::File::open(src_path)?)?
            {
                NewFileArchive {
                    file_id: &file_id,
                    path: &archive_path,
                    size,
                }
                .insert(tx)?;
            }

            // copy/move after insert to use the DB as a check against overwriting known files
            // obviously this is bad for concurrent writes, but that's ok for now
            let add_res = if move_file {
                fs::rename(src_path, &full_target_path)
            } else {
                fs::copy(src_path, &full_target_path).map(|_| ())
            };
            match add_res {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow!(
                    "Error copying {} to {}: {:?}",
                    src_path,
                    full_target_path.to_string_lossy(),
                    e
                )),
            }
        })
    }

    fn virtual_path(collection_id: &Uuid, dest_path: &Path) -> anyhow::Result<PathBuf> {
        let root = PathBuf::from(format!("hoard/collections/{}", collection_id.hyphenated()));
        let virt_path = canonical_path(dest_path.as_os_str()).map_err(|e| anyhow!("{}", e))?;
        Ok(root.join(strip_root(virt_path)))
    }

    fn random_partition(&self) -> anyhow::Result<(Partition, dev_utils::Partition)> {
        let mut partitions = dev_utils::get_all_partitions()?;
        log::trace!("Current partitions: {:#?}", partitions);

        let partition_ids = partitions.iter().map(|p| p.uuid()).collect::<Vec<_>>();
        match Partition::random(&self.conn, &partition_ids)? {
            Some(db_part) => {
                let part = partitions
                    .drain(..)
                    .find(|p| p.uuid() == db_part.uuid())
                    .unwrap();
                Ok((db_part, part))
            }
            None => bail!(concat!(
                "No currently mounted partitions were found in the DB. ",
                "Try mounting one or adding one to the DB.",
            ),),
        }
    }

    pub fn list_files<'a, I, II>(
        &self,
        collection_id: &Uuid,
        files: II,
        all: bool,
    ) -> anyhow::Result<Vec<File>>
    where
        I: Iterator<Item = &'a str>,
        II: IntoIterator<Item = &'a str, IntoIter = I>,
    {
        let mut output = Vec::new();

        for file in files {
            // if the path is a file
            if !file.ends_with('/') {
                if let Some(db_file) =
                    File::get_by_collection_and_path(&self.conn, collection_id, file)?
                {
                    output.push(db_file);
                    continue; // can't be both a file and a dir
                }
            }

            // if the path is a dir
            output.extend(File::get_by_collection_and_directory(
                &self.conn,
                collection_id,
                all,
                file,
            )?);
        }

        output.sort_by(|a, b| a.path().cmp(b.path()));
        Ok(output)
    }

    pub fn find_files<'a, I, II>(
        &self,
        collection_id: &Uuid,
        min_depth: Option<u32>,
        max_depth: Option<u32>,
        name: Option<&Regex>,
        path: Option<&Regex>,
        files: II,
    ) -> anyhow::Result<Vec<File>>
    where
        I: Iterator<Item = &'a str>,
        II: IntoIterator<Item = &'a str, IntoIter = I>,
    {
        match (min_depth, max_depth) {
            (Some(min), Some(max)) if min > max => {
                bail!("Min depth ({min}) cannot be greater than max depth ({max})")
            }
            _ => (),
        }

        let mut output = Vec::new();

        for file_name in files {
            // if the path is a file
            if !file_name.ends_with('/') {
                if let Some(db_file) =
                    File::get_by_collection_and_path(&self.conn, collection_id, file_name)?
                {
                    output.push(db_file);
                    continue;
                }
            }

            let matches = File::find_in_dir(
                &self.conn,
                collection_id,
                file_name,
                min_depth,
                max_depth,
                name,
                path,
            )?;
            output.extend(matches);
        }

        output.sort_by(|a, b| a.path().cmp(b.path()));
        Ok(output)
    }

    #[cfg(feature = "cli")]
    pub fn inspect_file(&self, collection_id: &Uuid, path: &str) -> anyhow::Result<String> {
        let coll = Collection::for_id(&self.conn, collection_id)?
            .ok_or_else(|| anyhow!("Collection not found"))?;
        let file = File::get_by_collection_and_path(&self.conn, collection_id, path)?
            .ok_or_else(|| anyhow!("File not found"))?;

        let file_placements = FilePlacement::get_by_file_id(&self.conn, file.id())?;
        let mut placements = Vec::new();
        for fp in file_placements {
            let disk = Disk::for_partition_id(&self.conn, fp.partition_id())?.unwrap();
            placements.push(disk.label().to_string())
        }

        let file_hashes = FileHash::get_by_file_id(&self.conn, file.id())?;
        let mut hashes = Vec::new();
        for fh in file_hashes {
            hashes.push(HashDisplay {
                algorithm: fh.hash_algorithm().to_string(),
                value: fh.hash_value_hex(),
            });
        }

        let disp = FileDisplay {
            path: file.path().to_string(),
            size: file.size(),
            file_id: *file.id(),
            collection: CollectionDisplay {
                id: *collection_id,
                name: coll.name().to_string(),
            },
            placements,
            hashes,
        };

        Ok(serde_yaml::to_string(&disp)?)
    }
}

#[derive(Serialize)]
struct FileDisplay {
    path: String,
    size: u64,
    file_id: Uuid,
    collection: CollectionDisplay,
    placements: Vec<String>,
    hashes: Vec<HashDisplay>,
}

#[derive(Serialize)]
struct CollectionDisplay {
    name: String,
    id: Uuid,
}

#[derive(Serialize)]
struct HashDisplay {
    algorithm: String,
    value: String,
}

#[cfg(test)]
mod tests {
    use crate::manager::Manager;
    use crate::test_utils::fixtures;
    use rusqlite::Connection;
    use tempfile::tempdir;

    #[test_log::test]
    fn db_migrate() {
        let conn = Connection::open_in_memory().unwrap();
        let mut manager = Manager::new(fixtures::config(), conn);
        manager.db_migrate().unwrap();
    }

    #[test_log::test]
    fn db_vacuum() {
        let manager = fixtures::manager();
        manager.db_vacuum().unwrap();
    }

    #[test_log::test]
    fn init() {
        let td = tempdir().unwrap();
        let inner_path = td.path().join("hoard"); // to force dir creation in `init`
        let config_path = inner_path.join("config.yaml");
        Manager::init(&config_path).unwrap();
        assert!(config_path.exists());
    }

    #[test_log::test]
    fn add_collection() {
        let mut manager = fixtures::manager();
        manager.add_collection("foo").unwrap();
    }

    #[test_log::test]
    fn list_collections() {
        let mut manager = fixtures::manager();
        let _ = fixtures::collection(&mut manager.conn);
        let colls = manager.list_collections().unwrap();
        assert_ne!(colls, vec![]);
    }

    #[test_log::test]
    fn list_disks() {
        let mut manager = fixtures::manager();
        let _ = fixtures::disk(&mut manager.conn);
        let disks = manager.list_disks().unwrap();
        assert_ne!(disks, vec![]);
    }

    #[test_log::test]
    fn list_partitions() {
        let mut manager = fixtures::manager();
        let disk = fixtures::disk(&mut manager.conn);
        let _ = fixtures::partition(&mut manager.conn, &disk);
        let parts = manager.list_partitions().unwrap();
        assert_ne!(parts, vec![]);
    }

    #[test_log::test]
    fn list_files() {
        let mut manager = fixtures::manager();
        let coll = fixtures::collection(&mut manager.conn);
        let file = fixtures::file(&mut manager.conn, &coll);
        let files = manager
            .list_files(coll.id(), vec![file.path()], true)
            .unwrap();
        assert_ne!(files, vec![]);
    }

    #[test_log::test]
    fn inspect_file() {
        let mut manager = fixtures::manager();
        let disk = fixtures::disk(&mut manager.conn);
        let partition = fixtures::partition(&mut manager.conn, &disk);
        let coll = fixtures::collection(&mut manager.conn);
        let file = fixtures::file_full(&mut manager.conn, &partition, &coll);
        manager.inspect_file(coll.id(), file.path()).unwrap();
    }
}
