use crate::db::types::Timestamp;
use crate::db::unique_violation;
use rusqlite::{Connection, OptionalExtension, Row, Transaction};
use uuid::Uuid;

#[derive(Debug, PartialEq)]
#[cfg_attr(feature = "cli", derive(Table))]
pub struct Collection {
    #[cfg_attr(feature = "cli", table(title = "ID"))]
    id: Uuid,
    #[cfg_attr(feature = "cli", table(title = "Created Date"))]
    created_date: Timestamp,
    #[cfg_attr(feature = "cli", table(title = "Name"))]
    name: String,
}

impl Collection {
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn star_mapper(row: &Row) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get("id")?,
            name: row.get("name")?,
            created_date: row.get("created_date")?,
        })
    }

    pub fn for_id(conn: &Connection, id: &Uuid) -> anyhow::Result<Option<Self>> {
        conn.query_row(
            "SELECT * FROM collections WHERE id = ?",
            params![id],
            Self::star_mapper,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn all(conn: &Connection) -> anyhow::Result<Vec<Self>> {
        let mut stmt = conn.prepare("SELECT * FROM collections")?;
        let mut rows = stmt
            .query_and_then([], Self::star_mapper)?
            .map(|r| r.map_err(Into::into))
            .collect::<Vec<anyhow::Result<Self>>>();
        rows.drain(..).collect::<anyhow::Result<Vec<Self>>>()
    }
}

pub struct NewCollection<'a> {
    pub name: &'a str,
}

impl<'a> NewCollection<'a> {
    pub fn insert<'b>(&self, tx: &Transaction<'b>) -> anyhow::Result<Uuid> {
        let id = Uuid::new_v4();
        match tx.execute(
            "INSERT INTO collections (id, name, created_date) VALUES (?, ?, ?)",
            params![id.as_bytes(), self.name, Timestamp::now()],
        ) {
            Ok(_) => Ok(id),
            Err(ref e) if unique_violation(e, ["collections.name"]) => {
                bail!("Collection name was not unique: {}", self.name)
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
    fn collection_insert_and_fetch() {
        let mut conn = fixtures::db();
        let new_col = NewCollection {
            name: "hella leaks",
        };
        let id = auto_transaction(&mut conn, |tx| new_col.insert(tx)).unwrap();
        assert_eq!(
            Collection::all(&conn)
                .unwrap()
                .iter()
                .map(|c| c.id)
                .collect::<Vec<_>>(),
            vec![id]
        );
    }
}
