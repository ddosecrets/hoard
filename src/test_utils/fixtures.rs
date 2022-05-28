use crate::config::Config;
use crate::db::init_connection;
use crate::db::types::{
    Collection, Disk, File, FileHash, FilePlacement, Location, NewCollection, NewDisk, NewFile,
    NewFileHash, NewFilePlacement, NewLocation, NewPartition, Partition,
};
use crate::db::{auto_transaction, migrate};
use crate::hash_utils::HashAlgorithm;
use crate::manager::Manager;
use rusqlite::Connection;
use std::fs;

pub fn config() -> Config {
    serde_yaml::from_reader(fs::File::open("./dev-config.yaml").unwrap()).unwrap()
}

pub fn db() -> Connection {
    let mut conn = Connection::open_in_memory().unwrap();
    init_connection(&conn).unwrap();
    migrate(&mut conn).unwrap();
    conn
}

pub fn manager() -> Manager {
    Manager::new(config(), db())
}

pub fn location(conn: &mut Connection) -> Location {
    let id = auto_transaction::<'_, _, anyhow::Error, _>(conn, |tx| {
        NewLocation {
            name: "test-location",
        }
        .insert(tx)
    })
    .unwrap();
    Location::for_id(conn, &id).unwrap().unwrap()
}

pub fn disk(conn: &mut Connection, location: &Location) -> Disk {
    let id = auto_transaction::<'_, _, anyhow::Error, _>(conn, |tx| {
        NewDisk {
            label: "test-disk",
            location_id: location.id(),
            serial_number: "some-serial-123",
        }
        .insert(tx)
    })
    .unwrap();
    Disk::for_id(conn, &id).unwrap().unwrap()
}

pub fn partition(conn: &mut Connection, disk: &Disk) -> Partition {
    let id = auto_transaction::<'_, _, anyhow::Error, _>(conn, |tx| {
        NewPartition {
            disk_id: disk.id(),
            uuid: "AFA-161-420-69",
            capacity: 420,
        }
        .insert(tx)
    })
    .unwrap();
    Partition::for_id(conn, &id).unwrap().unwrap()
}

pub fn collection(conn: &mut Connection) -> Collection {
    let id = auto_transaction::<'_, _, anyhow::Error, _>(conn, |tx| {
        NewCollection {
            name: "some-collection",
        }
        .insert(tx)
    })
    .unwrap();
    Collection::for_id(conn, &id).unwrap().unwrap()
}

pub fn file(conn: &mut Connection, collection: &Collection) -> File {
    let id = auto_transaction::<'_, _, anyhow::Error, _>(conn, |tx| {
        NewFile {
            collection_id: collection.id(),
            path: "/foo.txt",
            size: 6969,
        }
        .insert(tx)
    })
    .unwrap();
    File::for_id(conn, &id).unwrap().unwrap()
}

pub fn file_hash(conn: &mut Connection, file: &File) -> FileHash {
    let alg = HashAlgorithm::Sha1;
    auto_transaction::<'_, _, anyhow::Error, _>(conn, |tx| {
        NewFileHash {
            file_id: file.id(),
            hash_algorithm: &alg,
            hash_value: &[1, 3, 1, 2],
        }
        .insert(tx)
    })
    .unwrap();

    FileHash::get_by_file_id(conn, file.id())
        .unwrap()
        .drain(..)
        .next()
        .unwrap()
}

pub fn file_full(
    conn: &mut Connection,
    partition: &Partition,
    collection: &Collection,
) -> (File, Vec<FilePlacement>, Vec<FileHash>) {
    let id = auto_transaction::<'_, _, anyhow::Error, _>(conn, |tx| {
        let file_id = NewFile {
            collection_id: collection.id(),
            path: "/foo.txt",
            size: 6969,
        }
        .insert(tx)?;

        NewFileHash {
            file_id: &file_id,
            hash_algorithm: &HashAlgorithm::Sha256,
            hash_value: b"a1c3a1b2",
        }
        .insert(tx)?;

        NewFilePlacement {
            file_id: &file_id,
            partition_id: partition.id(),
        }
        .insert(tx)?;

        Ok(file_id)
    })
    .unwrap();
    (
        File::for_id(conn, &id).unwrap().unwrap(),
        FilePlacement::get_by_file_id(conn, &id).unwrap(),
        FileHash::get_by_file_id(conn, &id).unwrap(),
    )
}
