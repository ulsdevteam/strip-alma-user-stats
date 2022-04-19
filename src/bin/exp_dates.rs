use anyhow::{Context, Ok, Result};
use std::{
    fs::{read_dir, read_to_string, DirEntry},
    io::stdout,
};

/// Reads json files created by collect_users.rs
fn main() -> Result<()> {
    let mut writer = csv::Writer::from_writer(stdout());
    writer.write_record(["Primary Id", "User Group", "Expiration Date", "Purge Date"])?;
    let mut process_file = {
        let writer = &mut writer;
        |entry: DirEntry| {
            let user_details = json::parse(&read_to_string(entry.path())?)?;
            let primary_id = user_details["primary_id"].to_string();
            if primary_id == entry.path().file_stem().unwrap().to_string_lossy() {
                writer.write_record([
                    primary_id,
                    user_details["user_group"]["value"].to_string(),
                    user_details["expiry_date"].to_string(),
                    user_details["purge_date"].to_string(),
                ])?;
            }
            Ok(())
        }
    };

    for entry in read_dir("users")? {
        if let Err(error) = entry.context("error reading file").and_then(&mut process_file) {
            eprintln!("error processing file: {}", error);
        }
    }

    Ok(())
}
