use anyhow::Result;
use regex::Regex;
use std::{
    env,
    fs::File,
    io::{BufRead, BufReader},
};

/// Takes in the log output from the main program (bin.rs)
#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let alma_client = alma::Client::new(env::var("ALMA_REGION")?, env::var("ALMA_APIKEY")?);
    let error_regex = Regex::new(r#"user (.+): Alma API error:"#)?;
    let identifier_error_regex = Regex::new(r#"Error Message: ((User with i|I)dentifier.*)$"#)?;
    for path in std::env::args().skip(1) {
        let file = File::open(path)?;
        let mut lines = BufReader::new(file).lines();
        while let Some(Ok(line)) = lines.next() {
            if let Some(capture) = error_regex.captures(&line) {
                let user_primary_id = &capture[1];
                let error_line = lines.nth(2).unwrap()?;
                if let Some(capture) = identifier_error_regex.captures(&error_line) {
                    let error_message = &capture[1];
                    match alma_client.get_user_details_with_fees(user_primary_id).await {
                        Ok(alma_user) => {
                            if alma_user["fees"]["value"].as_f64().unwrap_or(0.0) > 0.0 {
                                println!("Primary id: {}. Retrieved primary id: {}. Group: {}. Fee balance: {}. Original error message: {}", user_primary_id, alma_user["primary_id"], alma_user["user_group"]["value"], alma_user["fees"]["value"], error_message);
                            }
                        }
                        Err(error) => {
                            eprintln!("Error retrieving user with primary id {}: {}", user_primary_id, error);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
