use rusqlite::ffi::{self, ErrorCode};
use rusqlite::{Connection, Transaction};

mod functions;
mod migrations;
pub mod types;

pub use migrations::migrate;

pub fn init_connection(conn: &Connection) -> anyhow::Result<()> {
    rusqlite::vtab::array::load_module(conn)?;
    functions::add_functions(conn)
}

pub fn auto_transaction<'a, T, E, F>(conn: &'a mut Connection, mut func: F) -> anyhow::Result<T>
where
    F: FnMut(&mut Transaction<'a>) -> Result<T, E>,
    E: Into<anyhow::Error>,
{
    let mut tx = conn.transaction()?;
    match func(&mut tx) {
        Ok(res) => {
            tx.commit()?;
            Ok(res)
        }
        Err(e) => {
            tx.rollback()?;
            Err(e.into())
        }
    }
}

pub fn unique_violation<const N: usize>(
    err: &rusqlite::Error,
    mut columns: [&'static str; N],
) -> bool {
    match err {
        rusqlite::Error::SqliteFailure(
            ffi::Error {
                code,
                extended_code,
            },
            Some(msg),
        ) => {
            if !(code == &ErrorCode::ConstraintViolation && *extended_code == 2067) {
                return false;
            }
            let unique_prefix = "UNIQUE constraint failed: ";
            if !msg.starts_with(unique_prefix) {
                return false;
            }

            let mut violated_columns = msg[unique_prefix.len()..]
                .split(',')
                .map(|s| s.trim())
                .collect::<Vec<_>>();

            if violated_columns.len() != columns.len() {
                return false;
            }

            violated_columns.sort_unstable();
            violated_columns.dedup();
            columns.sort_unstable(); // assumed to not be duplicated

            for i in 0..violated_columns.len() {
                if violated_columns[i] != columns[i] {
                    return false;
                }
            }

            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_error_with_matching_column() {
        let error = rusqlite::Error::SqliteFailure(
            ffi::Error {
                code: ErrorCode::ConstraintViolation,
                extended_code: 2067,
            },
            Some("UNIQUE constraint failed: foo.bar".to_string()),
        );
        assert!(unique_violation(&error, ["foo.bar"]));
    }

    #[test]
    fn unique_error_no_matching_column() {
        let error = rusqlite::Error::SqliteFailure(
            ffi::Error {
                code: ErrorCode::ConstraintViolation,
                extended_code: 2067,
            },
            Some("UNIQUE constraint failed: foo.bar".to_string()),
        );
        assert!(!unique_violation(&error, ["foo.quuz"]));
    }

    fn simple_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE foo (id INT UNIQUE NOT NULL);")
            .unwrap();
        conn
    }

    #[test]
    fn auto_transaction_success() {
        let mut conn = simple_db();
        auto_transaction(&mut conn, |tx| {
            tx.execute_batch("INSERT INTO foo (id) VALUES (1)")
        })
        .unwrap();
        let res = conn
            .query_row("SELECT id FROM foo", [], |row| row.get::<_, i32>("id"))
            .unwrap();
        assert_eq!(res, 1);
    }

    #[test]
    fn auto_transaction_failure() {
        let mut conn = simple_db();
        let insert_res: Result<(), _> = auto_transaction(&mut conn, |tx| {
            tx.execute_batch("INSERT INTO foo (id) VALUES (1)")?;
            Err(anyhow!("SOME ERROR"))
        });
        assert!(insert_res.is_err());

        let res = conn.query_row("SELECT id FROM foo", [], |row| row.get::<_, i32>("id"));
        match res {
            Err(rusqlite::Error::QueryReturnedNoRows) => (),
            x => panic!("Unexepcted result: {:?}", x),
        }
    }
}
