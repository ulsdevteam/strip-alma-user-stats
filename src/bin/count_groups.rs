use anyhow::Result;
use regex::Regex;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
};

/// Takes in the output from ident_errors_analysis.rs
fn main() -> Result<()> {
    let mut map: HashMap<String, u32> = HashMap::new();
    let regex = Regex::new(r"Group: (.*?)\.")?;
    for path in std::env::args().skip(1) {
        let file = BufReader::new(File::open(path)?);
        for line in file.lines().flatten() {
            if let Some(captures) = regex.captures(&line) {
                *map.entry(captures[1].to_string()).or_default() += 1;
            }
        }
    }
    let mut results: Vec<_> = map.into_iter().collect();
    results.sort_by_key(|(_, v)| 20000 - v);
    println!("{:?}", results);
    Ok(())
}
