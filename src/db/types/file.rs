use crate::db::types::Timestamp;
use crate::db::unique_violation;
use crate::hash_utils::HashAlgorithm;
use regex::Regex;
use rusqlite::{Connection, OptionalExtension, Row, ToSql, Transaction};
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub struct File {
    id: Uuid,
    collection_id: Uuid,
    path: String,
    created_date: Timestamp,
    size: u64,
}

impl File {
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    fn star_mapper(row: &Row) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get("id")?,
            collection_id: row.get("collection_id")?,
            path: row.get("path")?,
            created_date: row.get("created_date")?,
            size: row.get("size")?,
        })
    }

    #[cfg(test)]
    pub fn for_id(conn: &Connection, id: &Uuid) -> anyhow::Result<Option<Self>> {
        conn.query_row(
            "SELECT * FROM files WHERE id = ?",
            params![id],
            Self::star_mapper,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn get_by_collection_and_path(
        conn: &Connection,
        collection_id: &Uuid,
        path: &str,
    ) -> anyhow::Result<Option<Self>> {
        conn.query_row(
            "SELECT * FROM files WHERE collection_id = ? AND path = ?",
            params![collection_id, path],
            Self::star_mapper,
        )
        .optional()
        .map_err(Into::into)
    }

    // TODO this doesn't include directories, only files
    // e.g., if `/foo/bar/baz` exists and one does `ls /foo/`, then `/foo/bar` isn't returned
    pub fn get_by_collection_and_directory(
        conn: &Connection,
        collection_id: &Uuid,
        all: bool,
        path: &str,
    ) -> anyhow::Result<Vec<Self>> {
        let mut re = regex::escape(path);
        re.insert(0, '^');
        if !re.ends_with('/') {
            re.push('/')
        }
        if !all {
            re.push_str(r"[^\.]");
        }
        re.push_str("[^/]*$");

        let mut stmt =
            conn.prepare("SELECT * FROM files WHERE collection_id = ? AND path REGEXP ?")?;
        let mut rows = stmt
            .query_and_then(params![collection_id, re], Self::star_mapper)?
            .map(|r| r.map_err(Into::into))
            .collect::<Vec<anyhow::Result<Self>>>();
        rows.drain(..).collect::<anyhow::Result<Vec<Self>>>()
    }

    pub fn find_in_dir(
        conn: &Connection,
        collection_id: &Uuid,
        prefix: &str,
        min_depth: Option<u32>,
        max_depth: Option<u32>,
        name: Option<&Regex>,
        path: Option<&Regex>,
    ) -> anyhow::Result<Vec<Self>> {
        let mut sql = "SELECT * FROM files WHERE collection_id = :collection_id".to_string();
        let mut params: Vec<(&str, &dyn ToSql)> = vec![(":collection_id", collection_id)];

        // need refs for lifetimes
        let min_depth = min_depth.as_ref();
        let max_depth = max_depth.as_ref();
        let name = name.map(ToString::to_string);
        let path = path.map(ToString::to_string);
        let name = name.as_ref();
        let path = path.as_ref();

        match (min_depth, max_depth) {
            (Some(min), Some(max)) => {
                sql += " AND relative_depth(:prefix, path) BETWEEN :min_depth AND :max_depth";
                params.push((":prefix", &prefix));
                params.push((":min_depth", min));
                params.push((":max_depth", max));
            }
            (Some(min), None) => {
                sql += " AND relative_depth(:prefix, path) >= :min_depth";
                params.push((":prefix", &prefix));
                params.push((":min_depth", min));
            }
            (None, Some(max)) => {
                sql += " AND relative_depth(:prefix, path) <= :max_depth";
                params.push((":prefix", &prefix));
                params.push((":max_depth", max));
            }
            (None, None) => (),
        }

        if let Some(name) = name {
            sql += " AND basename(path) REGEXP :name";
            params.push((":name", name));
        }

        if let Some(path) = path {
            sql += " AND path REGEXP :path";
            params.push((":path", path));
        }

        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt
            .query_and_then(&*params, Self::star_mapper)?
            .map(|r| r.map_err(Into::into))
            .collect::<Vec<anyhow::Result<Self>>>();
        rows.drain(..).collect::<anyhow::Result<Vec<Self>>>()
    }
}

#[derive(Debug, PartialEq)]
pub struct NewFile<'a> {
    pub collection_id: &'a Uuid,
    pub path: &'a str,
    pub size: u64,
}

impl<'a> NewFile<'a> {
    pub fn insert<'b>(&self, tx: &Transaction<'b>) -> anyhow::Result<Uuid> {
        let id = Uuid::new_v4();
        match tx.execute(
            concat!(
                "INSERT INTO files (id, collection_id, path, size, created_date) ",
                "VALUES (:id, :collection_id, :path, :size, :created_date)"
            ),
            named_params! {
                ":id": id.as_bytes(),
                ":collection_id": self.collection_id,
                ":path": self.path,
                ":size": self.size,
                ":created_date": Timestamp::now(),
            },
        ) {
            Ok(_) => Ok(id),
            Err(ref e) if unique_violation(e, ["files.collection_id", "files.path"]) => {
                bail!(
                    "That path already exists for the collect {}: {}",
                    self.collection_id.hyphenated(),
                    self.path
                )
            }
            Err(e) => bail!("Unexpected DB error: {e:?}"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct FilePlacement {
    file_id: Uuid,
    partition_id: Uuid,
}

impl FilePlacement {
    pub fn partition_id(&self) -> &Uuid {
        &self.partition_id
    }

    fn star_mapper(row: &Row) -> rusqlite::Result<Self> {
        Ok(Self {
            file_id: row.get("file_id")?,
            partition_id: row.get("partition_id")?,
        })
    }

    pub fn get_by_file_id(conn: &Connection, file_id: &Uuid) -> anyhow::Result<Vec<Self>> {
        let mut stmt = conn.prepare("SELECT * FROM file_placements WHERE file_id = ?")?;
        let mut rows = stmt
            .query_and_then([file_id], Self::star_mapper)?
            .map(|r| r.map_err(Into::into))
            .collect::<Vec<anyhow::Result<Self>>>();
        rows.drain(..).collect::<anyhow::Result<Vec<Self>>>()
    }
}

#[derive(Debug, PartialEq)]
pub struct NewFilePlacement<'a> {
    pub file_id: &'a Uuid,
    pub partition_id: &'a Uuid,
}

impl<'a> NewFilePlacement<'a> {
    pub fn insert<'b>(&self, tx: &Transaction<'b>) -> anyhow::Result<()> {
        match tx.execute(
            "INSERT INTO file_placements (file_id, partition_id) VALUES (?, ?)",
            params![&self.file_id, &self.partition_id],
        ) {
            Ok(_) => Ok(()),
            Err(ref e)
                if unique_violation(
                    e,
                    ["file_placements.file_id", "file_placements.partition_id"],
                ) =>
            {
                bail!(
                    "A file with id {} is already on partition {}",
                    self.file_id.hyphenated(),
                    self.partition_id.hyphenated()
                )
            }
            Err(e) => bail!("Unexpected DB error: {e:?}"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct FileHash {
    file_id: Uuid,
    hash_algorithm: HashAlgorithm,
    hash_value: Vec<u8>,
}

impl FileHash {
    pub fn hash_algorithm(&self) -> HashAlgorithm {
        self.hash_algorithm
    }

    pub fn hash_value_hex(&self) -> String {
        hex::encode(&self.hash_value)
    }

    fn star_mapper(row: &Row) -> rusqlite::Result<Self> {
        Ok(Self {
            file_id: row.get("file_id")?,
            hash_algorithm: row.get("hash_algorithm")?,
            hash_value: row.get("hash_value")?,
        })
    }

    pub fn get_by_file_id(conn: &Connection, file_id: &Uuid) -> anyhow::Result<Vec<Self>> {
        let mut stmt = conn.prepare("SELECT * FROM file_hashes WHERE file_id = ?")?;
        let mut rows = stmt
            .query_and_then([file_id], Self::star_mapper)?
            .map(|r| r.map_err(Into::into))
            .collect::<Vec<anyhow::Result<Self>>>();
        rows.drain(..).collect::<anyhow::Result<Vec<Self>>>()
    }
}

#[derive(Debug, PartialEq)]
pub struct NewFileHash<'a> {
    pub file_id: &'a Uuid,
    pub hash_algorithm: &'a HashAlgorithm,
    pub hash_value: &'a [u8],
}

impl<'a> NewFileHash<'a> {
    pub fn insert<'b>(&self, tx: &Transaction<'b>) -> anyhow::Result<Uuid> {
        let id = Uuid::new_v4();
        match tx.execute(
            "INSERT INTO file_hashes (id, file_id, hash_algorithm, hash_value) VALUES (?, ?, ?, ?)",
            params![&id, &self.file_id, &self.hash_algorithm, &self.hash_value],
        ) {
            Ok(_) => Ok(id),
            Err(ref e)
                if unique_violation(e, ["file_hashes.file_id", "file_hashes.hash_algorithm"]) =>
            {
                bail!(
                    "The file with ID {} is already has a hash with name {}",
                    self.file_id.hyphenated(),
                    self.hash_algorithm,
                )
            }
            Err(e) => bail!("Unexpected DB error: {e:?}"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct NewFileArchive<'a> {
    pub file_id: &'a Uuid,
    pub path: &'a str,
    pub size: u64,
}

impl<'a> NewFileArchive<'a> {
    pub fn insert<'b>(&self, tx: &Transaction<'b>) -> anyhow::Result<Uuid> {
        let id = Uuid::new_v4();
        match tx.execute(
            "INSERT INTO file_archives (id, file_id, path, size) VALUES (?, ?, ?, ?)",
            params![&id, &self.file_id, &self.path, &self.size],
        ) {
            Ok(_) => Ok(id),
            Err(ref e) if unique_violation(e, ["file_archives.file_id", "file_archives.path"]) => {
                bail!(
                    "The archive with file ID {} already the path {}",
                    self.file_id.hyphenated(),
                    self.path,
                )
            }
            Err(e) => bail!("Unexpected DB error: {e:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::auto_transaction;
    use crate::db::types::Collection;
    use crate::test_utils::fixtures;
    use std::collections::HashSet;

    #[test_log::test]
    fn file_insert_and_fetch() {
        let mut conn = fixtures::db();
        let coll = fixtures::collection(&mut conn);
        let new_file = NewFile {
            collection_id: coll.id(),
            path: "/foo",
            size: 123,
        };
        let file_id = auto_transaction(&mut conn, |tx| new_file.insert(tx)).unwrap();
        assert_eq!(File::for_id(&conn, &file_id).unwrap().unwrap().id, file_id);
    }

    #[test_log::test]
    fn file_placement_insert() {
        let mut conn = fixtures::db();
        let coll = fixtures::collection(&mut conn);
        let disk = fixtures::disk(&mut conn);
        let partition = fixtures::partition(&mut conn, &disk);
        let file = fixtures::file(&mut conn, &coll);
        let new_placement = NewFilePlacement {
            file_id: file.id(),
            partition_id: partition.id(),
        };
        auto_transaction(&mut conn, |tx| new_placement.insert(tx)).unwrap();
    }

    #[test_log::test]
    fn file_hash_insert() {
        let mut conn = fixtures::db();
        let coll = fixtures::collection(&mut conn);
        let file = fixtures::file(&mut conn, &coll);
        let new_hash = NewFileHash {
            file_id: file.id(),
            hash_algorithm: &HashAlgorithm::Sha256,
            hash_value: b"1337",
        };
        auto_transaction(&mut conn, |tx| new_hash.insert(tx)).unwrap();
    }

    #[test_log::test]
    fn file_archive_insert() {
        let mut conn = fixtures::db();
        let coll = fixtures::collection(&mut conn);
        let file = fixtures::file(&mut conn, &coll);
        let new_archive_file = NewFileArchive {
            file_id: file.id(),
            path: "./wat.txt",
            size: 6969,
        };
        auto_transaction(&mut conn, |tx| new_archive_file.insert(tx)).unwrap();
    }

    #[test]
    fn file_ls_dir_simple() {
        let mut conn = fixtures::db();
        let coll = fixtures::collection(&mut conn);
        let paths = hashset! {"/foo/bar", "/foo/.baz"};
        auto_transaction::<'_, _, anyhow::Error, _>(&mut conn, |tx| {
            for path in &paths {
                NewFile {
                    collection_id: coll.id(),
                    path,
                    size: 123,
                }
                .insert(tx)?;
            }
            Ok(())
        })
        .unwrap();

        let files = File::get_by_collection_and_directory(&conn, coll.id(), true, "/foo").unwrap();
        let found_paths = files.iter().map(|f| f.path()).collect::<HashSet<_>>();
        assert_eq!(found_paths, paths);
    }

    #[test]
    fn file_ls_dir_hidden() {
        let mut conn = fixtures::db();
        let coll = fixtures::collection(&mut conn);
        auto_transaction::<'_, _, anyhow::Error, _>(&mut conn, |tx| {
            for path in &["/foo/bar", "/foo/.baz"] {
                NewFile {
                    collection_id: coll.id(),
                    path,
                    size: 123,
                }
                .insert(tx)?;
            }
            Ok(())
        })
        .unwrap();

        let files = File::get_by_collection_and_directory(&conn, coll.id(), false, "/foo").unwrap();
        let found_paths = files.iter().map(|f| f.path()).collect::<Vec<_>>();
        assert_eq!(found_paths, vec!["/foo/bar"]);
    }

    const FIND_IN_DIR_PATHS: &[&str] = &[
        "/foo.txt",
        "/foo/bar.txt",
        "/foo/bar/baz.txt",
        "/foo/bar/baz/quux.txt",
        "/foo/bar/baz/quux/lol.txt",
    ];

    fn find_in_dir_test_cases() -> (Connection, Collection) {
        let mut conn = fixtures::db();
        let coll = fixtures::collection(&mut conn);

        auto_transaction::<'_, _, anyhow::Error, _>(&mut conn, |tx| {
            for path in FIND_IN_DIR_PATHS {
                NewFile {
                    collection_id: coll.id(),
                    path,
                    size: 123,
                }
                .insert(tx)?;
            }
            Ok(())
        })
        .unwrap();
        (conn, coll)
    }

    #[test]
    fn find_in_dir_simple() {
        let (conn, coll) = find_in_dir_test_cases();
        let res = File::find_in_dir(&conn, coll.id(), "/", None, None, None, None).unwrap();
        let res = res.iter().map(|f| f.path()).collect::<HashSet<_>>();
        assert_eq!(res, FIND_IN_DIR_PATHS.iter().copied().collect());
    }

    #[test]
    fn find_in_dir_max_depth() {
        let (conn, coll) = find_in_dir_test_cases();
        let res = File::find_in_dir(&conn, coll.id(), "/", None, Some(2), None, None).unwrap();
        let res = res.iter().map(|f| f.path()).collect::<HashSet<_>>();
        assert_eq!(res, FIND_IN_DIR_PATHS[0..2].iter().copied().collect());
    }

    #[test]
    fn find_in_dir_min_depth() {
        let (conn, coll) = find_in_dir_test_cases();
        let res = File::find_in_dir(&conn, coll.id(), "/", Some(2), None, None, None).unwrap();
        let res = res.iter().map(|f| f.path()).collect::<HashSet<_>>();
        assert_eq!(res, FIND_IN_DIR_PATHS[1..].iter().copied().collect());
    }

    #[test]
    fn find_in_dir_max_and_min_depth() {
        let (conn, coll) = find_in_dir_test_cases();
        let res = File::find_in_dir(&conn, coll.id(), "/", Some(2), Some(4), None, None).unwrap();
        let res = res.iter().map(|f| f.path()).collect::<HashSet<_>>();
        assert_eq!(res, FIND_IN_DIR_PATHS[1..4].iter().copied().collect());
    }

    #[test]
    fn find_in_dir_non_root_max_and_min_depth() {
        let (conn, coll) = find_in_dir_test_cases();
        let res =
            File::find_in_dir(&conn, coll.id(), "/foo", Some(1), Some(3), None, None).unwrap();
        let res = res.iter().map(|f| f.path()).collect::<HashSet<_>>();
        assert_eq!(res, FIND_IN_DIR_PATHS[1..4].iter().copied().collect());
    }

    #[test]
    fn find_in_dir_file_name_regex() {
        let (conn, coll) = find_in_dir_test_cases();
        let re = Regex::new("^bar").unwrap();
        let res = File::find_in_dir(&conn, coll.id(), "/", None, None, Some(&re), None).unwrap();
        let res = res.iter().map(|f| f.path()).collect::<HashSet<_>>();
        assert_eq!(res, FIND_IN_DIR_PATHS[1..2].iter().copied().collect());
    }

    #[test]
    fn find_in_dir_file_path_regex() {
        let (conn, coll) = find_in_dir_test_cases();
        let re = Regex::new("/bar/").unwrap();
        let res = File::find_in_dir(&conn, coll.id(), "/", None, None, None, Some(&re)).unwrap();
        let res = res.iter().map(|f| f.path()).collect::<HashSet<_>>();
        assert_eq!(res, FIND_IN_DIR_PATHS[2..].iter().copied().collect());
    }
}
