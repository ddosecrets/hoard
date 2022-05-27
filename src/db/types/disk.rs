use crate::db::types::Timestamp;
use crate::db::unique_violation;
use crate::dev_utils;
use crate::sql_utils::add_array_to_query;
use rand::seq::SliceRandom;
use rand::thread_rng;
use rusqlite::{Connection, OptionalExtension, Row, Transaction};
use uuid::Uuid;

#[derive(Debug, PartialEq)]
#[cfg_attr(feature = "cli", derive(Table))]
pub struct Disk {
    #[cfg_attr(feature = "cli", table(title = "ID"))]
    id: Uuid,
    #[cfg_attr(feature = "cli", table(title = "Created Date"))]
    created_date: Timestamp,
    #[cfg_attr(feature = "cli", table(title = "Serial Number"))]
    serial_number: String,
    #[cfg_attr(feature = "cli", table(title = "Label"))]
    label: String,
}

impl Disk {
    #[cfg(test)]
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    fn star_mapper(row: &Row) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get("id")?,
            serial_number: row.get("serial_number")?,
            label: row.get("label")?,
            created_date: row.get("created_date")?,
        })
    }

    fn for_serial_number(conn: &Connection, serial_number: &str) -> anyhow::Result<Option<Self>> {
        conn.query_row(
            "SELECT * FROM disks WHERE serial_number = ?",
            [serial_number],
            Self::star_mapper,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn for_partition_id(
        conn: &Connection,
        partition_id: &Uuid,
    ) -> anyhow::Result<Option<Self>> {
        conn.query_row(
            concat!(
                "SELECT * FROM disks AS d ",
                "INNER JOIN partitions AS p ON p.disk_id = d.id ",
                "WHERE p.id = ?",
            ),
            [partition_id],
            Self::star_mapper,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn for_id(conn: &Connection, id: &Uuid) -> anyhow::Result<Option<Self>> {
        conn.query_row(
            "SELECT * FROM disks WHERE id = ?",
            params![id],
            Self::star_mapper,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn all(conn: &Connection) -> anyhow::Result<Vec<Self>> {
        let mut stmt = conn.prepare("SELECT * FROM disks")?;
        let mut rows = stmt
            .query_and_then([], Self::star_mapper)?
            .map(|r| r.map_err(Into::into))
            .collect::<Vec<anyhow::Result<Self>>>();
        rows.drain(..).collect::<anyhow::Result<Vec<Self>>>()
    }
}

pub struct NewDisk<'a> {
    pub serial_number: &'a str,
    pub label: &'a str,
}

impl<'a> NewDisk<'a> {
    pub fn insert<'b>(&self, tx: &Transaction<'b>) -> anyhow::Result<Uuid> {
        let id = Uuid::new_v4();
        match tx.execute(
            "INSERT INTO disks (id, serial_number, label, created_date) VALUES (?, ?, ?, ?)",
            params![
                id.as_bytes(),
                self.serial_number,
                self.label,
                Timestamp::now()
            ],
        ) {
            Ok(_) => Ok(id),
            Err(ref e) if unique_violation(e, ["disks.serial_number"]) => {
                bail!("Disk serial number was not unique: {}", self.serial_number)
            }
            Err(ref e) if unique_violation(e, ["disks.label"]) => {
                bail!("Disk label was not unique: {}", self.label)
            }
            Err(e) => bail!("Unexpected DB error: {e:?}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "cli", derive(Table))]
pub struct Partition {
    #[cfg_attr(feature = "cli", table(title = "ID"))]
    id: Uuid,
    // TODO hide this from the Table
    #[cfg_attr(feature = "cli", table(title = "Disk ID"))]
    disk_id: Uuid,
    // TODO hide this from the Table
    #[cfg_attr(feature = "cli", table(title = "UUID"))]
    uuid: String,
    #[cfg_attr(feature = "cli", table(title = "Capacity (bytes)"))]
    capacity: u64,
}

impl Partition {
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    pub fn uuid(&self) -> &str {
        &self.uuid
    }

    fn star_mapper(row: &Row) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get("id")?,
            disk_id: row.get("disk_id")?,
            uuid: row.get("uuid")?,
            capacity: row.get("capacity")?,
        })
    }

    pub fn for_id(conn: &Connection, id: &Uuid) -> anyhow::Result<Option<Self>> {
        conn.query_row(
            "SELECT * FROM partitions WHERE id = ?",
            [id],
            Self::star_mapper,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn current(
        conn: &Connection,
        current_partition_uuids: &[&str],
    ) -> anyhow::Result<Vec<Self>> {
        // sqlite doesn't have a concept of arrays, so we have to pull everything and filter
        // ourselves
        let mut sql = "SELECT * FROM partitions WHERE uuid IN".to_string();
        let mut params = Vec::new();
        add_array_to_query(&mut sql, &mut params, current_partition_uuids.iter());

        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt
            .query_and_then(&*params, Self::star_mapper)?
            .map(|r| r.map_err(Into::into))
            .collect::<Vec<anyhow::Result<Self>>>();
        rows.drain(..).collect::<anyhow::Result<Vec<Self>>>()
    }

    pub fn random(
        conn: &Connection,
        current_partition_uuids: &[&str],
    ) -> anyhow::Result<Option<Self>> {
        let choices = Self::current(conn, current_partition_uuids)?;
        let mut rng = thread_rng();
        Ok(choices.choose(&mut rng).cloned())
    }

    pub fn all(conn: &Connection) -> anyhow::Result<Vec<Self>> {
        let mut stmt = conn.prepare("SELECT * FROM partitions")?;
        let mut rows = stmt
            .query_and_then([], Self::star_mapper)?
            .map(|r| r.map_err(Into::into))
            .collect::<Vec<anyhow::Result<Self>>>();
        rows.drain(..).collect::<anyhow::Result<Vec<Self>>>()
    }
}

pub struct NewPartition<'a> {
    pub disk_id: &'a Uuid,
    pub uuid: &'a str,
    pub capacity: u64,
}

impl<'a> NewPartition<'a> {
    pub fn create_for_disk<'b>(
        tx: &Transaction<'b>,
        disk: &dev_utils::Disk,
        partition: &dev_utils::Partition,
    ) -> anyhow::Result<Uuid> {
        let db_disk = match Disk::for_serial_number(tx, disk.serial_number())? {
            Some(db_disk) => db_disk,
            None => bail!(
                "Disk not found for serial number {:?}. Try adding it first?",
                disk.serial_number(),
            ),
        };

        NewPartition {
            disk_id: &db_disk.id,
            uuid: partition.uuid(),
            capacity: partition.capacity(),
        }
        .insert(tx)
    }

    pub fn insert<'b>(&self, tx: &Transaction<'b>) -> anyhow::Result<Uuid> {
        let id = Uuid::new_v4();
        match tx.execute(
            "INSERT INTO partitions (id, disk_id, uuid, capacity) VALUES (?, ?, ?, ?)",
            params![id.as_bytes(), self.disk_id, self.uuid, self.capacity],
        ) {
            Ok(_) => Ok(id),
            Err(ref e) if unique_violation(e, ["partitions.uuid"]) => {
                bail!("Patition UUID was not unique: {}", self.uuid)
            }
            Err(e) => bail!("Unexpected DB error: {e:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::auto_transaction;
    use crate::test_utils::fixtures;

    #[test_log::test]
    fn disk_insert_and_fetch() {
        let mut conn = fixtures::db();
        let new_disk = NewDisk {
            serial_number: "acab-1312",
            label: "s3cr3t d4t4",
        };
        let id = auto_transaction(&mut conn, |tx| new_disk.insert(tx)).unwrap();
        assert_eq!(
            Disk::all(&conn)
                .unwrap()
                .iter()
                .map(|d| d.id)
                .collect::<Vec<_>>(),
            vec![id]
        );
    }

    #[test_log::test]
    fn partition_insert_and_fetch() {
        let mut conn = fixtures::db();
        let disk = fixtures::disk(&mut conn);
        let new_part = NewPartition {
            disk_id: &disk.id,
            uuid: "abc-123",
            capacity: 161,
        };
        let part_id = auto_transaction(&mut conn, |tx| new_part.insert(tx)).unwrap();
        assert_eq!(
            Partition::all(&conn)
                .unwrap()
                .iter()
                .map(|p| p.id)
                .collect::<Vec<_>>(),
            vec![part_id]
        );
    }

    #[test]
    fn current_partitions_none() {
        let conn = fixtures::db();
        let current = Partition::current(&conn, &["abc"]).unwrap();
        assert_eq!(&current, &[]);
    }

    #[test]
    fn current_partitions_some() {
        let mut conn = fixtures::db();
        let disk = fixtures::disk(&mut conn);
        let partition = fixtures::partition(&mut conn, &disk);
        let current = Partition::current(&conn, &[partition.uuid()]).unwrap();
        assert_eq!(&current, &[partition]);
    }
}
