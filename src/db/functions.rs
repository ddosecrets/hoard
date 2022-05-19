use regex::Regex;
use rusqlite::functions::FunctionFlags;
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::Arc;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

// TODO? add uuidv4 function if it simplifies

pub fn add_functions(conn: &Connection) -> anyhow::Result<()> {
    add_regexp(conn)?;
    add_relative_depth(conn)?;
    add_basename(conn)?;
    Ok(())
}

fn add_regexp(conn: &Connection) -> rusqlite::Result<()> {
    conn.create_scalar_function(
        "regexp",
        2, // (regex, string)
        FunctionFlags::SQLITE_UTF8
            | FunctionFlags::SQLITE_DETERMINISTIC
            | FunctionFlags::SQLITE_DIRECTONLY,
        move |ctx| {
            let regexp: Arc<Regex> = ctx.get_or_create_aux(0, |vr| -> Result<_, BoxError> {
                Ok(Regex::new(vr.as_str()?)?)
            })?;
            let text = ctx
                .get_raw(1)
                .as_str()
                .map_err(|e| rusqlite::Error::UserFunctionError(e.into()))?;
            Ok(regexp.is_match(text))
        },
    )
}

fn add_relative_depth(conn: &Connection) -> rusqlite::Result<()> {
    conn.create_scalar_function(
        "relative_depth",
        2, // (root, candidate)
        FunctionFlags::SQLITE_UTF8
            | FunctionFlags::SQLITE_DETERMINISTIC
            | FunctionFlags::SQLITE_DIRECTONLY,
        move |ctx| {
            let root_parts: Arc<Vec<String>> =
                ctx.get_or_create_aux(0, |vr| -> Result<_, BoxError> {
                    let mut string = vr.as_str()?;
                    // we need to yank the trailing slash otherwise previxes break because the
                    // resulting vector has as the last element an empty string ("")
                    if string.ends_with('/') {
                        string = &string[0..string.len() - 1]
                    }
                    Ok(string.split('/').map(|s| s.to_string()).collect())
                })?;
            // this assumes that it doesn't end in a trailing slash which is correct for now
            // because all entries in the DB are files
            let cmp_parts = ctx
                .get_raw(1)
                .as_str()
                .map_err(|e| rusqlite::Error::UserFunctionError(e.into()))?
                .split('/')
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            if !cmp_parts.starts_with(&root_parts) {
                return Ok(None);
            }
            Ok(cmp_parts.len().checked_sub(root_parts.len()))
        },
    )
}

fn add_basename(conn: &Connection) -> rusqlite::Result<()> {
    conn.create_scalar_function(
        "basename",
        1,
        FunctionFlags::SQLITE_UTF8
            | FunctionFlags::SQLITE_DETERMINISTIC
            | FunctionFlags::SQLITE_DIRECTONLY,
        move |ctx| {
            let text = ctx
                .get_raw(0)
                .as_str()
                .map_err(|e| rusqlite::Error::UserFunctionError(e.into()))?;
            Ok(PathBuf::from(text).file_name().map(|s| {
                s.to_str()
                    .expect("string in sqlite query was not utf-8")
                    .to_string()
            }))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regex_as_function() {
        let conn = Connection::open_in_memory().unwrap();
        add_regexp(&conn).unwrap();
        let is_match: bool = conn
            .query_row("SELECT regexp('^foo(bar)?$', 'foobar')", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert!(is_match)
    }

    #[test]
    fn regex_as_operator() {
        let conn = Connection::open_in_memory().unwrap();
        add_regexp(&conn).unwrap();
        let text: String = conn
            .query_row(
                "SELECT text FROM (SELECT 'foobar' AS text) WHERE text REGEXP '^foo(bar)?$'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(&text, "foobar")
    }

    #[test]
    fn relative_depths() {
        let conn = Connection::open_in_memory().unwrap();
        add_relative_depth(&conn).unwrap();
        let mut stmt = conn.prepare("SELECT relative_depth(?, ?)").unwrap();
        let cases = &[
            (Some(0u64), "/foo", "/foo"),
            (Some(1u64), "/", "/foo"),
            (None, "/foo", "/"),
            (Some(1u64), "/foo", "/foo/bar"),
            (Some(2u64), "/foo", "/foo/bar/baz"),
            (Some(1u64), "/foo/bar/", "/foo/bar/baz"),
        ];
        for (expected_depth, root, path) in cases {
            let found_depth: Option<u64> = stmt.query_row([root, path], |row| row.get(0)).unwrap();
            assert_eq!(
                found_depth,
                *expected_depth,
                "Expected {path:?} to have relative depth to root {root:?} of {expected_depth:?} but found {found_depth:?}",
            );
        }
    }

    #[test]
    fn basename() {
        let conn = Connection::open_in_memory().unwrap();
        add_basename(&conn).unwrap();
        let mut stmt = conn.prepare("SELECT basename(?)").unwrap();
        let cases = &[
            (Some("foo"), "/foo"),
            (Some("bar"), "/foo/bar"),
            (Some("bar"), "/foo/bar/"),
        ];
        for (expected, path) in cases {
            let found_depth: Option<String> = stmt.query_row([path], |row| row.get(0)).unwrap();
            assert_eq!(
                &found_depth.as_deref(),
                expected,
                "Expected {path:?} to have basename {expected:?} found {found_depth:?}",
            );
        }
    }
}
