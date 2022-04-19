use anyhow::Result;
use regex::Regex;
use std::{
    env,
    fs::File,
    io::{BufRead, BufReader},
};

/// Takes in the output from ident_errors_analysis.rs and places a json file for each user in a 'users' folder
#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let alma_client = alma::Client::new(env::var("ALMA_REGION")?, env::var("ALMA_APIKEY")?);
    let error_regex = Regex::new(r"^Primary id: (.*?)\.")?;
    for path in std::env::args().skip(1) {
        let file = File::open(path)?;
        let mut lines = BufReader::new(file).lines();
        while let Some(Ok(line)) = lines.next() {
            if let Some(capture) = error_regex.captures(&line) {
                let user_primary_id = &capture[1];
                match alma_client.get_user_details_with_fees(user_primary_id).await {
                    Ok(alma_user) => {
                        if let Err(error) = File::create(format!("users/{}.json", user_primary_id))
                            .and_then(|mut file| alma_user.write_pretty(&mut file, 4))
                        {
                            eprintln!("Error writing user data to file for user {}: {}", user_primary_id, error);
                        }
                    }
                    Err(error) => {
                        eprintln!("Error retrieving user with primary id {}: {}", user_primary_id, error);
                    }
                }
            }
        }
    }
    Ok(())
}
