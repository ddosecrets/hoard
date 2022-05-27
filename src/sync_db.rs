use std::rc::Rc;
use rusqlite::types::Value;
use crate::config::FileConfig;
use crate::db::auto_transaction;
use crate::db::types::NewFileHash;
use crate::dev_utils;
use crate::hash_utils::make_hashes;
use crate::hash_utils::HashAlgorithm;
use crate::manager::Manager;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::fs;
use uuid::Uuid;

pub fn sync_db(
    file_config: &FileConfig,
    conn: &mut Connection,
    collection_id: &Uuid,
) -> anyhow::Result<()> {
    sync_hashes(conn, file_config.hashes(), collection_id)?;
    log::info!("Sync completed");
    Ok(())
}

fn sync_hashes(
    conn: &mut Connection,
    algos: &[HashAlgorithm],
    collection_id: &Uuid,
) -> anyhow::Result<()> {
    log::info!("Checking if hash syncing needed.");
    if algos.is_empty() || !is_sync_hash_needed(conn, algos, collection_id)? {
        log::info!("Hash syncing not needed. Skipping.");
        return Ok(());
    }
    do_sync_hashes(conn, algos, collection_id)
}

fn is_sync_hash_needed(
    conn: &Connection,
    algos: &[HashAlgorithm],
    collection_id: &Uuid,
) -> anyhow::Result<bool> {
    let sql = concat!(
        "SELECT exists(",
        "  SELECT f.id FROM files AS f",
        "  LEFT OUTER JOIN file_hashes AS h",
        "  ON f.id = h.file_id AND h.hash_algorithm IN rarray(:algorithms)",
        "  WHERE f.collection_id = :collection_id",
        "  GROUP BY f.id HAVING count(h.file_id) != :expected_count",
        ")",
    );
    log::trace!("SQL:\n{}", sql);

    let algos = Rc::new(algos.iter().map(|a| Value::Text(a.to_string())).collect::<Vec<_>>());
    let params = named_params! {
        ":algorithms": algos,
        ":collection_id": collection_id,
        ":expected_count": algos.len(),
    };
    let mut stmt = conn.prepare(&sql)?;
    stmt.query_row(&*params, |row| row.get(0))
        .map_err(Into::into)
}

fn get_missing_hashes(
    conn: &Connection,
    algos: &[HashAlgorithm],
    collection_id: &Uuid,
) -> anyhow::Result<HashBucket> {
    let sql = concat!(
        "SELECT f.id AS file_id, f.path AS file_path, p.partition_id AS partition_id, ",
        "       pa.uuid AS partition_uuid, h.hash_algorithm AS hash_algorithm ",
        "FROM (",
        // subquery to give us the file IDs
        "  SELECT f.id AS id, f.path AS path FROM files AS f",
        "  LEFT OUTER JOIN file_hashes AS h",
        "  ON f.id = h.file_id AND h.hash_algorithm IN rarray(:algorithms)",
        "  WHERE f.collection_id = :collection_id",
        "  GROUP BY f.id, f.path",
        "  HAVING count(h.file_id) != :expected_count",
        ") AS f ",
        // this inner join assumes there is at least one placement
        "INNER JOIN file_placements AS p ",
        "ON p.file_id = f.id ",
        "INNER JOIN partitions AS pa ",
        "ON pa.id = p.partition_id ",
        "LEFT OUTER JOIN file_hashes AS h ",
        "ON h.file_id = f.id",
    );
    log::trace!("SQL:\n{}", sql);

    let algos_value = Rc::new(algos.iter().map(|a| Value::Text(a.to_string())).collect::<Vec<_>>());
    let params = named_params! {
        ":algorithms": algos_value,
        ":collection_id": collection_id,
        ":expected_count": algos.len(),
    };

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params, |row| {
        Ok((
            (
                row.get::<_, Uuid>("file_id")?,
                row.get::<_, String>("file_path")?,
            ),
            (
                row.get::<_, Uuid>("partition_id")?,
                row.get::<_, String>("partition_uuid")?,
            ),
            row.get::<_, Option<HashAlgorithm>>("hash_algorithm")?,
        ))
    })?;

    let mut bucket = HashBucket::new();
    for row in rows {
        let (f_id, p_info, hash) = row?;
        bucket.add_entry(f_id, p_info, hash);
    }

    bucket.invert(algos);
    Ok(bucket)
}

fn do_sync_hashes(
    conn: &mut Connection,
    algos: &[HashAlgorithm],
    collection_id: &Uuid,
) -> anyhow::Result<()> {
    let missing_hashes = get_missing_hashes(conn, algos, collection_id)?;

    let mut skipped_files = 0_u32;
    let mut skipped_partitions = HashSet::<Uuid>::new();
    let mounted_partitions = dev_utils::get_all_partitions()?;

    for ((file_id, file_path), (partition_info, missing_algos)) in missing_hashes.data.iter() {
        let part = mounted_partitions.iter().find(|dev_part| {
            partition_info
                .iter()
                .any(|(_, p_uuid)| p_uuid == dev_part.uuid())
        });

        match part {
            Some(part) => {
                log::info!(
                    "Adding missing hashes for file ID {} at path {}",
                    file_id,
                    file_path
                );
                let path = Manager::path_on_partition(collection_id, file_path)?;
                let full_path = part.mount_point().join(path);
                let hashes = make_hashes(fs::File::open(full_path)?, missing_algos)?;
                auto_transaction::<'_, _, anyhow::Error, _>(conn, |tx| {
                    for (hash_algorithm, hash_value) in hashes.iter() {
                        NewFileHash {
                            file_id,
                            hash_algorithm,
                            hash_value,
                        }
                        .insert(tx)?;
                    }
                    Ok(())
                })?;
            }
            None => {
                skipped_files += 1;
                skipped_partitions.extend(partition_info.iter().map(|(p_id, _)| p_id));
            }
        }
    }

    if skipped_files == 0 {
        Ok(())
    } else {
        bail!(
            concat!(
                "While syncing hashes, {} files were skipped because their partitions were not mounted. ",
                "The skipped partition IDs were: {:?}",
            ),
            skipped_files,
            skipped_partitions,
        )
    }
}

struct HashBucket {
    #[allow(clippy::type_complexity)]
    // (file_id, path) -> ([(partition_id, "uuid")], [hash_alg])
    data: HashMap<(Uuid, String), (HashSet<(Uuid, String)>, HashSet<HashAlgorithm>)>,
}

impl HashBucket {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    fn add_entry(
        &mut self,
        file_info: (Uuid, String),
        partition_info: (Uuid, String),
        algorithm: Option<HashAlgorithm>,
    ) {
        let (partitions, algos) = self
            .data
            .entry(file_info)
            .or_insert((HashSet::new(), HashSet::new()));
        partitions.insert(partition_info);
        if let Some(algorithm) = algorithm {
            algos.insert(algorithm);
        }
    }

    fn invert(&mut self, target_algorithms: &[HashAlgorithm]) {
        let target_algorithms = target_algorithms.iter().cloned().collect::<HashSet<_>>();
        let missing = self
            .data
            .drain()
            .filter_map(|(f_info, (p_infos, algos))| {
                let diff = target_algorithms
                    .difference(&algos)
                    .copied()
                    .collect::<HashSet<_>>();

                if diff.is_empty() {
                    None
                } else {
                    Some((f_info, (p_infos, diff)))
                }
            })
            .collect();
        self.data = missing;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::fixtures;

    // trivially simple test that will fail if we have a SQL error
    #[test_log::test]
    fn is_sync_hash_needed_empty_tables() {
        let conn = fixtures::db();
        assert!(!is_sync_hash_needed(&conn, &[HashAlgorithm::Sha1], &Uuid::new_v4()).unwrap());
    }

    #[test_log::test]
    fn is_sync_hash_needed_all_present() {
        let mut conn = fixtures::db();
        let coll = fixtures::collection(&mut conn);
        let file = fixtures::file(&mut conn, &coll);
        let file_hash = fixtures::file_hash(&mut conn, &file);
        assert!(!is_sync_hash_needed(&conn, &[file_hash.hash_algorithm()], coll.id()).unwrap());
    }

    #[test_log::test]
    fn is_sync_hash_needed_all_one_missing() {
        let mut conn = fixtures::db();
        let coll = fixtures::collection(&mut conn);
        let file = fixtures::file(&mut conn, &coll);
        let file_hash = fixtures::file_hash(&mut conn, &file);
        let other_hash = HashAlgorithm::Sha3_256;
        assert_ne!(file_hash.hash_algorithm(), other_hash); // prerequisite
        assert!(
            is_sync_hash_needed(&conn, &[file_hash.hash_algorithm(), other_hash], coll.id())
                .unwrap(),
        );
    }

    #[test_log::test]
    fn get_hashes_all_present() {
        let mut conn = fixtures::db();
        let coll = fixtures::collection(&mut conn);
        let disk = fixtures::disk(&mut conn);
        let part = fixtures::partition(&mut conn, &disk);
        let (_, _, hashes) = fixtures::file_full(&mut conn, &part, &coll);
        let bucket = get_missing_hashes(&conn, &[hashes[0].hash_algorithm()], coll.id()).unwrap();
        assert!(
            bucket.data.is_empty(),
            "Bucket not empty: {:?}",
            bucket.data
        );
    }

    #[test_log::test]
    fn get_hashes_one_missing() {
        let mut conn = fixtures::db();
        let coll = fixtures::collection(&mut conn);
        let disk = fixtures::disk(&mut conn);
        let part = fixtures::partition(&mut conn, &disk);
        let (file, placements, hashes) = fixtures::file_full(&mut conn, &part, &coll);
        let other_hash = HashAlgorithm::Sha3_256;

        assert!(!placements.is_empty()); // prerequisite
        assert_eq!(hashes.len(), 1); // prerequisite
        assert_ne!(hashes[0].hash_algorithm(), other_hash); // prerequisite

        let bucket = get_missing_hashes(&conn, &[other_hash], coll.id()).unwrap();

        let mut expected_bucket = HashBucket::new();
        expected_bucket.add_entry(
            (*file.id(), file.path().to_owned()),
            (*placements[0].partition_id(), part.uuid().to_owned()),
            Some(other_hash),
        );

        assert_eq!(bucket.data, expected_bucket.data);
    }

    #[test]
    fn hash_bucket_invert() {
        let mut bucket = HashBucket::new();
        let f_path = "/path".to_owned();
        let f_id = uuid::Uuid::new_v4();
        let p_id = uuid::Uuid::new_v4();
        let p_uuid = "abc-123".to_string();
        let current_alg = HashAlgorithm::Sha1;
        let target_alg = HashAlgorithm::Sha256;
        bucket.add_entry((f_id, f_path.clone()), (p_id, p_uuid), Some(current_alg));
        bucket.invert(&[target_alg]);
        assert_eq!(bucket.data[&(f_id, f_path)].1, hashset! {target_alg});
    }
}
