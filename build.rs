use std::fs;

const MIGRATION_PATH: &str = "./src/db/sql/migrations/";

fn main() {
    // `inlcude_dir!` doesn't seem to trigger rebuilds ???
    for dir_entry in fs::read_dir(MIGRATION_PATH).unwrap() {
        let dir_entry = dir_entry.unwrap();
        let entry_path = dir_entry.path();
        let file_name = entry_path.file_name().unwrap();
        let file_name = file_name.to_str().unwrap();
        if file_name.starts_with('.') {
            continue;
        }
        println!("cargo:rerun-if-changed={}{}", MIGRATION_PATH, file_name);
    }
}
