use include_dir::{include_dir, Dir};
use rusqlite::ffi::{self, ErrorCode};
use rusqlite::Connection;

const INIT_SCHEMA_SQL: &str = include_str!("./sql/init-schema.sql");

static MIGRATION_DIR: Dir = include_dir!("./src/db/sql/migrations/");

struct Migration {
    version: i32,
    file_name: String,
    sql: String,
}

pub fn migrate(conn: &mut Connection) -> anyhow::Result<()> {
    let version = current_version(conn)?;
    log::debug!("Detected DB at version {}", version);

    let pending = get_pending_migrations(version);
    run_migrations(conn, &pending)?;
    Ok(())
}

fn current_version(conn: &mut Connection) -> anyhow::Result<i32> {
    match conn.query_row_and_then("SELECT version FROM db_version", [], |row| {
        row.get::<_, i32>("version")
    }) {
        Ok(version) => Ok(version),
        Err(rusqlite::Error::SqliteFailure(
            ffi::Error {
                code,
                extended_code,
            },
            Some(msg),
        )) if code == ErrorCode::Unknown
            && extended_code == 1
            && &msg == "no such table: db_version" =>
        {
            log::info!("DB does not have initial schema. Applying.");
            conn.execute_batch(INIT_SCHEMA_SQL)?;
            Ok(0)
        }
        Err(e) => bail!("Migration version check error: {}", e),
    }
}

fn get_pending_migrations(current_version: i32) -> Vec<Migration> {
    let mut pending = MIGRATION_DIR
        .files()
        .filter_map(|file| {
            let file_name = file.path().file_name().unwrap().to_str().unwrap();

            // filters to help with development
            if file_name.starts_with('.') {
                return None;
            }

            Some(Migration {
                version: file_name.split('-').next().unwrap().parse().unwrap(),
                file_name: file_name.to_string(),
                sql: file.contents_utf8().unwrap().to_string(),
            })
        })
        .filter(|mig| mig.version > current_version)
        .collect::<Vec<_>>();
    pending.sort_by(|a, b| a.version.cmp(&b.version));
    pending
}

fn run_migrations(conn: &mut Connection, migrations: &[Migration]) -> anyhow::Result<()> {
    if migrations.is_empty() {
        log::debug!("No migrations to run.");
        return Ok(());
    }

    log::info!("Running {} migration(s)", migrations.len());
    for migration in migrations {
        log::info!("Running migration: {}", migration.file_name);
        let tx = conn.transaction()?;
        tx.execute_batch(&migration.sql)?;
        tx.execute(
            "UPDATE db_version SET version = ?",
            params![migration.version],
        )?;
        tx.commit()?;
        log::debug!("Completed migration: {}", migration.file_name);
    }

    log::info!("Database up to date.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test_log::test]
    fn migrations_are_fetchable() {
        // checks all the unwraps and ensure we actually fetch some migrations
        let migrations = get_pending_migrations(0);
        assert!(!migrations.is_empty());
    }

    #[test_log::test]
    fn run_migrations() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrate(&mut conn).unwrap();
        // ensure no open transactions
        let _ = conn.unchecked_transaction().unwrap();
    }

    #[test_log::test]
    fn run_migrations_twice() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrate(&mut conn).unwrap();
        migrate(&mut conn).unwrap();
        // ensure no open transactions
        let _ = conn.unchecked_transaction().unwrap();
    }
}
