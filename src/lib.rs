//! # `hoard`
//!
//! A tool to allow data hoarders to split data sets across a pool of offline external hard drives.
//!
//! # Command Line Usage
//!
//! See the documentation for the [`cli`](crate::cli) module for basic usage.

#[macro_use]
extern crate anyhow;

#[cfg(feature = "cli")]
#[macro_use]
extern crate clap;

#[cfg(feature = "cli")]
#[macro_use]
extern crate cli_table;

#[cfg(test)]
#[macro_use]
extern crate maplit;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate rusqlite;

#[macro_use]
extern crate serde;

mod archive_utils;
#[cfg(feature = "cli")]
pub mod cli;
mod config;
mod db;
mod dev_utils;
mod error;
mod fs_utils;
mod hash_utils;
mod manager;
mod sync_db;
#[cfg(test)]
mod test_utils;
