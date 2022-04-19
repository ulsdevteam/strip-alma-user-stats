use anyhow::{Context, Ok, Result};
use json::JsonValue;
use std::{
    fs::{read_dir, read_to_string, DirEntry},
    io::stdout,
};

/// Reads json files created by collect_users.rs
fn main() -> Result<()> {
    let mut writer = csv::Writer::from_writer(stdout());
    writer.write_record(["primary_id", "PRIMARYIDENTIFIER"])?;
    let mut process_file = {
        let writer = &mut writer;
        |entry: DirEntry| {
            let user_details = json::parse(&read_to_string(entry.path())?)?;
            if let JsonValue::Array(identifiers) = &user_details["user_identifier"] {
                if let Some(primary_identifier) =
                    identifiers.iter().find(|id| id["id_type"]["value"] == "PRIMARYIDENTIFIER")
                {
                    writer.write_record([
                        user_details["primary_id"].as_str().unwrap(),
                        primary_identifier["value"].as_str().unwrap(),
                    ])?;
                }
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
