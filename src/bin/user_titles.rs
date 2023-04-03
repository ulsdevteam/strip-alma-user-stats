use anyhow::Result;
use std::{
    env,
    fs::File,
    io::{stdout, BufRead, BufReader},
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let alma_client = alma::Client::new(env::var("ALMA_REGION")?, env::var("ALMA_APIKEY")?);
    let mut csv = csv::Writer::from_writer(stdout());
    for path in env::args().skip(1) {
        let file = File::open(path)?;
        for line in BufReader::new(file).lines() {
            let user_id = line?;
            let user = alma_client.get_user_details(&user_id).await?;
            csv.write_record([user_id, user["user_title"].to_string()])?;
        }
    }

    Ok(())
}
