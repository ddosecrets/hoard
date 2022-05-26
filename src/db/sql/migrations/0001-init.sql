-- if we ever want to support non-utf-8 file paths
-- then this will help prevent bugs when converting
PRAGMA encoding = "UTF-8"; 

-- the default is off according to docs
PRAGMA foreign_keys = ON;

CREATE TABLE collections (
    id BINARY(16) NOT NULL
        PRIMARY KEY CONSTRAINT pk_collections
        CHECK (length(id) = 16) CONSTRAINT ck_collections_id,
    name TEXT NOT NULL
        UNIQUE CONSTRAINT uq_collections_name,
    created_date TEXT NOT NULL
);

CREATE TABLE disks (
    id BINARY(16) NOT NULL
        PRIMARY KEY CONSTRAINT pk_disks
        CHECK (length(id) = 16) CONSTRAINT ck_disks_id,
    serial_number TEXT NOT NULL
        UNIQUE CONSTRAINT uq_disks_serial_number,
    label TEXT NOT NULL
        UNIQUE CONSTRAINT uq_disks_label,
    created_date TEXT NOT NULL
);

CREATE TABLE partitions (
    id BINARY(16) NOT NULL
        PRIMARY KEY CONSTRAINT pk_partitions
        CHECK (length(id) = 16)
        CONSTRAINT ck_partitions_id,
    disk_id BINARY(16) NOT NULL
        CHECK (length(id) = 16)
        CONSTRAINT ck_partitions_disk_id,
    uuid TEXT NOT NULL
        UNIQUE CONSTRAINT uq_partitions_uuid,
    capacity BIGINT NOT NULL
        CHECK (capacity >= 0)
        CONSTRAINT ck_files_capacity,
    FOREIGN KEY (disk_id)
        REFERENCES disks(id)
        CONSTRAINT fk_partitions_disk_id
);

CREATE TABLE files (
    id BINARY(16) NOT NULL
        PRIMARY KEY CONSTRAINT pk_files
        CHECK (length(id) = 16) CONSTRAINT ck_files_id,
    collection_id BINARY(16) NOT NULL,
    created_date TEXT NOT NULL,
    path TEXT NOT NULL
        CHECK (length(path) > 0)
        CONSTRAINT ck_files_path,
    size BIGINT NOT NULL
        CHECK (size >= 0)
        CONSTRAINT ck_files_size,
    UNIQUE (collection_id, path)
        CONSTRAINT uq_files_collection_id_path,
    FOREIGN KEY (collection_id)
        REFERENCES collections(id)
        CONSTRAINT fk_files_collection_id
);

CREATE TABLE file_placements (
    partition_id BINARY(16) NOT NULL,
    file_id BINARY(16) NOT NULL,
    PRIMARY KEY (partition_id, file_id)
        CONSTRAINT pk_file_placements,
    FOREIGN KEY (partition_id)
        REFERENCES partitions(id)
        CONSTRAINT fk_file_placements_partition_id,
    FOREIGN KEY (file_id)
        REFERENCES files(id)
        CONSTRAINT fk_file_placements_file_id
);

CREATE TABLE file_hashes (
    id BINARY(16) NOT NULL
        PRIMARY KEY CONSTRAINT pk_file_hashes
        CHECK (length(id) = 16) CONSTRAINT ck_file_hashes_id,
    file_id BINARY(16) NOT NULL,
    hash_algorithm TEXT NOT NULL,
    hash_value BINARY NOT NULL,
    UNIQUE (file_id, hash_algorithm)
        CONSTRAINT uq_file_hashes_file_id_hash_algorithm
);

CREATE TABLE file_archives (
    id BINARY(16) NOT NULL
        PRIMARY KEY CONSTRAINT pk_file_archives
        CHECK (length(id) = 16) CONSTRAINT ck_file_archives_id,
    file_id BINARY(16) NOT NULL,
    path TEXT NOT NULL
        CHECK (length(path) > 0)
        CONSTRAINT ck_file_archives_path,
    size BIGINT NOT NULL
        CHECK (size >= 0)
        CONSTRAINT ck_file_archives_size,
    UNIQUE (file_id, path)
        CONSTRAINT uq_file_archives_file_id_path,
    FOREIGN KEY (file_id)
        REFERENCES files(id)
        CONSTRAINT fk_file_archives_file_id
);
