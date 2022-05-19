use crate::config::Config;
use crate::db::add_functions;
use crate::db::types::{
    Collection, Disk, File, NewCollection, NewDisk, NewFile, NewFileHash, NewFilePlacement,
    NewPartition, Partition,
};
use crate::db::{auto_transaction, migrate};
use crate::manager::Manager;
use rusqlite::Connection;
use std::fs;

pub fn config() -> Config {
    serde_yaml::from_reader(fs::File::open("./dev-config.yaml").unwrap()).unwrap()
}

pub fn db() -> Connection {
    let mut conn = Connection::open_in_memory().unwrap();
    add_functions(&conn).unwrap();
    migrate(&mut conn).unwrap();
    conn
}

pub fn manager() -> Manager {
    Manager::new(config(), db())
}

pub fn disk(conn: &mut Connection) -> Disk {
    let id = auto_transaction::<'_, _, anyhow::Error, _>(conn, |tx| {
        NewDisk {
            label: "test-disk",
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

pub fn file_full(conn: &mut Connection, partition: &Partition, collection: &Collection) -> File {
    let id = auto_transaction::<'_, _, anyhow::Error, _>(conn, |tx| {
        let file_id = NewFile {
            collection_id: collection.id(),
            path: "/foo.txt",
            size: 6969,
        }
        .insert(tx)?;

        NewFileHash {
            file_id: &file_id,
            hash_algorithm: "sha256",
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
    File::for_id(conn, &id).unwrap().unwrap()
}
