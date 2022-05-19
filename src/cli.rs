// NOTE: This file should contain no crate logic other than parsing CLI arguments, initializing
// logging, and passing arguments to the crate's functions.

//! Module implementing the CLI for `hoard`.
//!
//! For full help output run:
//! ```shell
//! hoard --help
//! ```
//!
//! # Basic Usage
//!
//! Set up the local files.
//! ```shell
//! hoard init
//! ```
//!
//! Create a collection of files.
//! ```shell
//! hoard collection add "my cold storage"
//! ```
//!
//! Add a DB entry for a physical disk.
//! ```shell
//! hoard disk add --label "my 4TB disk" /dev/sdb
//! ```
//!
//! Add a DB entry for one of the logical partitions on the physical disk.
//! ```shell
//! hoard partition add /dev/sdb1
//! ```
//!
//! Add a local file to the virtual "file system" of the disk pool.
//! ```shell
//! hoard file add --collection-id 72ea7d62-7b71-437c-9f64-e0e7469a4605 \
//!     /local/path/to/my/file.txt /some-dir/file.txt
//! ```
//!
//! Run an `ls`-like command against the virtual "file system" (works offline).
//! ```shell
//! hoard file ls --collection-id 1 /some-dir/
//! ```
//!
//! Run a `find`-like command against the virtual "file system" (works offline).
//! ```shell
//! hoard file find --collection-id 1 /
//! ```
//!
//! Inspect a file and get information about where it's located.
//! ```shell
//! hoard file inspect --collection-id 1 /some-dir/file.txt
//! ```
use crate::config::Config;
use crate::db::add_functions;
use crate::fs_utils::canonical_path;
use crate::manager::Manager;
use clap::Parser;
use cli_table::{
    format::{Border, Separator},
    print_stdout, TableStruct, WithTitle,
};
use regex::Regex;
use rusqlite::Connection;
use simplelog::{ColorChoice, ConfigBuilder, LevelFilter, TermLogger, TerminalMode};
use std::path::PathBuf;
use std::process::exit;
use uuid::Uuid;

#[cfg(debug_assertions)] // "dev"
lazy_static! {
    static ref CONFIG_DEFAULT_PATH: String = "./dev-config.yaml".to_string();
}

#[cfg(not(debug_assertions))] // "release"
lazy_static! {
    static ref CONFIG_DEFAULT_PATH: String = {
        directories::BaseDirs::new()
            .expect("Could not determine base dir for config")
            .config_dir()
            .join("hoard/config.yaml")
            .to_str()
            .expect("Config dir could not be made into a String")
            .to_string()
    };
}

// TODO: everything is kept private for now because I don't want to expose an actual API until I
// have error handling figured out, and also it gives us a nice lint on dead code during rapid
// prototyping :)
/// Runs the application, and calls [`process::exit`](std::process::exit) on completion. This is
/// unsafe to call from applications.
pub fn main() -> ! {
    if let Err(e) = main_inner() {
        log::error!("{}", e);
        exit(1);
    }
    exit(0);
}

fn main_inner() -> anyhow::Result<()> {
    let cli = Cli::parse();

    TermLogger::init(
        // 2 for default level as `info`
        match (2 + cli.verbose).saturating_sub(cli.quiet) {
            0 => LevelFilter::Error,
            1 => LevelFilter::Warn,
            2 => LevelFilter::Info,
            3 => LevelFilter::Debug,
            _ => LevelFilter::Trace,
        },
        ConfigBuilder::new()
            .set_time_level(LevelFilter::Off)
            .set_location_level(LevelFilter::Trace)
            .build(),
        TerminalMode::Stderr,
        ColorChoice::Auto,
    )?;

    log::warn!("`hoard` does not have a stable CLI interface. Use with caution.");

    // we have to run the init first to create the config and the dir
    if matches!(cli.command, Command::Init) {
        Manager::init(&cli.config_path)?;
    }

    let config = Config::from_path(&cli.config_path)?;
    let db_path = PathBuf::from(cli.config_path)
        .parent()
        .unwrap()
        .join(config.db().path());
    let db_path = db_path
        .to_str()
        .ok_or_else(|| anyhow!("DB path was not UTF-8"))?;
    log::debug!("Set DB path to: {db_path}");

    let conn = Connection::open(db_path)?;
    add_functions(&conn)?;
    let mut manager = Manager::new(config, conn);

    // TODO this logic is annoying but simplifies things in a few other places
    if (!cli.no_migrate || matches!(cli.command, Command::Init))
        && !matches!(cli.command, Command::Database(ref cmd) if matches!(cmd, DatabaseCmd::Migrate))
    {
        manager.db_migrate()?;
    }

    match cli.command {
        Command::Collection(cmd) => cmd.run(&mut manager),
        Command::Database(cmd) => cmd.run(&mut manager),
        Command::Disk(cmd) => cmd.run(&mut manager),
        Command::Init => Ok(()), // this was already handled
        Command::File(cmd) => cmd.run(&mut manager),
        Command::Partition(cmd) => cmd.run(&mut manager),
    }
}

fn print_table(table: TableStruct) -> anyhow::Result<()> {
    print_stdout(
        table
            // no borders (no nations, stop deportation)
            .border(Border::builder().build())
            // no separators
            .separator(Separator::builder().build()),
    )?;
    Ok(())
}

fn parse_uuid(string: &str) -> Result<Uuid, String> {
    Uuid::parse_str(string).map_err(|e| format!("{}", e))
}

fn parse_regex(string: &str) -> Result<Regex, String> {
    // new line to separate Clap's error line from the nicely formatted
    // helper string for the regex  syntax error
    Regex::new(string).map_err(|e| format!("\n{e}"))
}

/// A CLI tool for managing large data sets across many disks
#[derive(Debug, Parser)]
#[clap(name = "hoard", disable_help_subcommand = true, version)]
struct Cli {
    #[clap(subcommand)]
    command: Command,
    #[clap(long = "config", short = 'c', value_name = "PATH", default_value = &CONFIG_DEFAULT_PATH)]
    config_path: String,
    /// Increase the verbosity of logging one level (-v, -vv). Opposite of -q
    #[clap(long = "verbose", short = 'v', parse(from_occurrences))]
    verbose: usize,
    /// Decrease the verbosity of logging one level (-q, -qq). Opposite of -v
    #[clap(long = "quiet", short = 'q', parse(from_occurrences))]
    quiet: usize,
    /// Disable automatically running DB migrations
    #[clap(long = "no-migrate")]
    no_migrate: bool,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Manage the collections
    #[clap(subcommand)]
    Collection(CollectionCmd),
    /// Perform operations directly on the database
    #[clap(subcommand, name = "db")]
    Database(DatabaseCmd),
    /// Manage physical disks
    #[clap(subcommand)]
    Disk(DiskCmd),
    /// Manage files in the hoardFS
    #[clap(subcommand)]
    File(FileCmd),
    /// Initialize the local directories
    Init,
    /// Manage partitions on physical disks
    #[clap(subcommand)]
    Partition(PartitionCmd),
}

#[derive(Debug, Subcommand)]
#[clap(disable_help_subcommand = true)]
enum CollectionCmd {
    /// Add a new collection
    Add {
        /// The collection's name
        name: String,
    },
    /// List the collections
    #[clap(name = "ls")]
    List,
}

impl CollectionCmd {
    fn run(&self, manager: &mut Manager) -> anyhow::Result<()> {
        match self {
            Self::Add { name } => manager.add_collection(name),
            Self::List => print_table(manager.list_collections()?.with_title()),
        }
    }
}

#[derive(Debug, Subcommand)]
#[clap(disable_help_subcommand = true)]
enum DatabaseCmd {
    /// Run the database migrations
    ///
    /// This command ignores the global `--no-migrate` flag.
    Migrate,
    /// Vacuum the database
    Vacuum,
}

impl DatabaseCmd {
    fn run(&self, manager: &mut Manager) -> anyhow::Result<()> {
        match self {
            Self::Migrate => manager.db_migrate(),
            Self::Vacuum => manager.db_vacuum(),
        }
    }
}

#[derive(Debug, Subcommand)]
#[clap(disable_help_subcommand = true)]
enum DiskCmd {
    /// Add a new disk
    Add {
        /// The path to the disk (e.g., /dev/sdb)
        path: String,
        /// The physical label on the housing of the disk (e.g., "Secret Data 0161")
        #[clap(long = "label", value_name = "LABEL")]
        label: String,
    },
    /// List all disks
    #[clap(name = "ls")]
    List,
}

impl DiskCmd {
    fn run(&self, manager: &mut Manager) -> anyhow::Result<()> {
        match self {
            Self::Add { path, label } => manager.add_disk(path, label),
            Self::List => print_table(manager.list_disks()?.with_title()),
        }
    }
}
#[derive(Debug, Subcommand)]
#[clap(disable_help_subcommand = true)]
enum FileCmd {
    /// Add a file and copy it to the partition
    Add {
        /// The ID of the collection the file belongs to
        #[clap(long = "collection-id", short = 'c', value_name = "ID", parse(try_from_str = parse_uuid))]
        collection_id: Uuid,
        /// The ID of the partion the file will be placed on
        #[clap(long = "partition-id", short = 'p', value_name = "ID", parse(try_from_str = parse_uuid))]
        partition_id: Option<Uuid>,
        /// The path of the file on the local system
        #[clap(value_name = "SRC")]
        src_path: String,
        /// The virtual path on the hoardFS
        #[clap(parse(try_from_str = canonical_path), value_name = "DEST")]
        dest_path: PathBuf,
        /// Move the file on to the target partition instead of copying it
        #[clap(long = "move")]
        move_file: bool,
    },
    /// Find a file meeting certain criteria
    Find {
        /// The ID of the collection the file belongs to
        #[clap(long = "collection-id", short = 'c', value_name = "ID", parse(try_from_str = parse_uuid))]
        collection_id: Uuid,
        /// Minimum depth to search
        #[clap(long = "min-depth", value_name = "INT")]
        min_depth: Option<u32>,
        /// Maximum depth to search
        #[clap(long = "max-depth", value_name = "INT")]
        max_depth: Option<u32>,
        /// Regex of the name of the file
        #[clap(long = "name", parse(try_from_str = parse_regex), value_name = "REGEX")]
        name: Option<Regex>,
        /// Regex for the full path of the file
        #[clap(long = "path", parse(try_from_str = parse_regex), value_name = "REGEX")]
        path: Option<Regex>,
        /// Files and directories to search
        #[clap(value_name = "FILE", min_values = 1)]
        files: Vec<String>,
    },
    /// Inspect a file and show metadata
    Inspect {
        /// The ID of the collection the file belongs to
        #[clap(long = "collection-id", short = 'c', value_name = "ID", parse(try_from_str = parse_uuid))]
        collection_id: Uuid,
        /// The virtual path on the hoardFS
        #[clap(value_name = "FILE", parse(try_from_str = canonical_path))]
        path: PathBuf,
    },
    /// List files (similar to `ls`)
    #[clap(name = "ls")]
    List {
        /// The ID of the collection the file belongs to
        #[clap(long = "collection-id", short = 'c', value_name = "ID", parse(try_from_str = parse_uuid))]
        collection_id: Uuid,
        /// Path or glob to files on the hoardFS
        // TODO min values here doesn't work (??)
        #[clap(value_name = "FILE", min_values = 1)]
        files: Vec<String>,
        /// Include files and directories starting with `.`
        #[clap(long = "all", short = 'a')]
        all: bool,
    },
}

impl FileCmd {
    fn run(&self, manager: &mut Manager) -> anyhow::Result<()> {
        match self {
            Self::Add {
                collection_id,
                partition_id,
                src_path,
                dest_path,
                move_file,
            } => manager.add_file(
                collection_id,
                partition_id.as_ref(),
                src_path,
                dest_path,
                *move_file,
            ),
            Self::Find {
                collection_id,
                min_depth,
                max_depth,
                name,
                path,
                files,
            } => {
                for file in manager.find_files(
                    collection_id,
                    *min_depth,
                    *max_depth,
                    name.as_ref(),
                    path.as_ref(),
                    files.iter().map(|s| &**s),
                )? {
                    // TODO this should trim the leading bit of the path off
                    // e.g., `ls /foo/` should return only `bar` if `/foo/bar` exists
                    println!("{}", file.path());
                }
                Ok(())
            }
            Self::Inspect {
                collection_id,
                path,
            } => {
                let path = path.to_str().ok_or_else(|| {
                    anyhow!("Path could not be made UTF-8: {}", path.to_string_lossy())
                })?;
                println!("{}", manager.inspect_file(collection_id, path)?);
                Ok(())
            }
            Self::List {
                collection_id,
                all,
                files,
            } => {
                for file in manager.list_files(collection_id, files.iter().map(|s| &**s), *all)? {
                    // TODO this should trim the leading bit of the path off
                    // e.g., `ls /foo/` should return only `bar` if `/foo/bar` exists
                    println!("{}", file.path());
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Subcommand)]
#[clap(disable_help_subcommand = true)]
enum PartitionCmd {
    /// Add a new partition
    Add {
        /// The path to to the partition (e.g., /dev/sdb1)
        path: String,
    },
    /// List all partitions
    #[clap(name = "ls")]
    List,
}

impl PartitionCmd {
    fn run(&self, manager: &mut Manager) -> anyhow::Result<()> {
        match self {
            Self::Add { path } => manager.add_partition(path),
            Self::List => print_table(manager.list_partitions()?.with_title()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;
    use clap::Parser;

    #[test]
    fn cli_parser() {
        // TODO this seems sloppy, but is "fine" as a quick check
        match Cli::try_parse_from(["--help"]) {
            Err(err) if err.kind() == ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand => (),
            x => panic!("Unexpected result: {:?}", x),
        }
    }
}
