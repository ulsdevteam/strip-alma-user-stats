use anyhow::Result;
use regex::Regex;
use std::{
    collections::HashSet,
    fs::File,
    io::{BufRead, BufReader},
};

fn main() -> Result<()> {
    let mut updated = 0;
    let mut errors = 0;
    let mut internal_stats = 0u64;
    let mut internal_stats_users = HashSet::new();
    let mut title_errors = 0;
    let mut identifier_errors = 0;
    let batch_regex = Regex::new(r#"Batch \d+: (\d+) users updated\. (\d+) errors\.$"#)?;
    let internal_stat_regex = Regex::new(r#"user (.+) has internal statistic: (.+)$"#)?;
    let title_error_regex = Regex::new(r#"Error Message: Given user title is not legal"#)?;
    let identifier_error_regex = Regex::new(r#"Error Message: (User with i|I)dentifier"#)?;
    for path in std::env::args().skip(1) {
        let file = BufReader::new(File::open(path)?);
        for line in file.lines().flatten() {
            if let Some(captures) = batch_regex.captures(&line) {
                updated += captures[1].parse::<u64>().unwrap();
                errors += captures[2].parse::<u64>().unwrap();
            } else if let Some(captures) = internal_stat_regex.captures(&line) {
                internal_stats += 1;
                internal_stats_users.insert(captures[1].to_string());
            }
            else if title_error_regex.is_match(&line) {
                title_errors += 1;
            }
            else if identifier_error_regex.is_match(&line) {
                identifier_errors += 1;
            }
        }
    }
    println!(
        "Total updated: {}. Total errors: {}. Total internal statistics: {} in {} users.",
        updated,
        errors,
        internal_stats,
        internal_stats_users.len()
    );
    println!("Title errors: {}. Identifier errors: {}.", title_errors, identifier_errors);

    Ok(())
}
