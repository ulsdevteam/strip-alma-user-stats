use anyhow::Result;
use log::{error, info};
use std::{
    env,
    fs::File,
    io::{BufRead, BufReader},
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    let alma_client = alma::Client::new(env::var("ALMA_REGION")?, env::var("ALMA_APIKEY")?);
    for path in env::args().skip(1) {
        let file = File::open(path)?;
        for line in BufReader::new(file).lines() {
            let user_id = line?;
            match alma::handle_user(&alma_client, &user_id).await {
                Ok(true) => info!("user {} updated.", user_id),
                Ok(false) => info!("user {} did not need updating.", user_id),
                Err(error) => error!("user {}: {:#}", user_id, error),
            }
        }
    }

    Ok(())
}
