use anyhow::{Error, Result};
use json::JsonValue;
use log::{error, info};
use std::{
    collections::HashSet,
    env,
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
};
use structopt::StructOpt;

mod alma;

#[derive(StructOpt)]
struct Options {
    #[structopt(short, long, default_value = "0")]
    from_offset: usize,
    #[structopt(short, long)]
    to_offset: Option<usize>,
    #[structopt(parse(from_os_str))]
    categories_file: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load from .env file if it is present
    dotenv::dotenv().ok();
    // Initialize logging
    env_logger::init();
    // Get command line arguments
    let options = Options::from_args();
    // Construct alma client
    let alma_client = alma::Client::new(env::var("ALMA_REGION")?, env::var("ALMA_APIKEY")?);
    // Pull categories from file
    let categories_file = File::open(options.categories_file)?;
    // Get each line of the file as a string and collect into HashSet
    let categories_to_remove: HashSet<_> = BufReader::new(categories_file).lines().map(|l| l.unwrap()).collect();
    // Use an Arc (Atomic Reference Counted smart pointer) to share categories set between threads
    let categories_to_remove = Arc::new(categories_to_remove);
    // Alma API page size
    const LIMIT: usize = 100;
    // Each page will get its own concurrent task, handles to which will be collected here with their offset
    let mut join_handles = Vec::new();
    // Get the first batch of user ids, along with the total user count
    let (user_ids, total_users) = alma_client.get_user_ids_and_total_count(options.from_offset, LIMIT).await?;
    // Spawn a task for the first batch
    info!("Spawning task for batch {}", options.from_offset);
    join_handles.push((
        options.from_offset,
        tokio::spawn(handle_user_batch(alma_client.clone(), categories_to_remove.clone(), user_ids)),
    ));
    // Determine the last offset for this run
    let last_offset = options.to_offset.unwrap_or(total_users / LIMIT).min(total_users / LIMIT);
    // Split up the rest of the users into batches
    for offset in (options.from_offset + 1)..=last_offset {
        let alma_client = alma_client.clone();
        let categories_to_remove = categories_to_remove.clone();
        // Spawn a task for each batch
        info!("Spawning task for batch {}", offset);
        let join_handle = tokio::spawn(async move {
            match alma_client.get_user_ids(offset, LIMIT).await {
                Ok(user_ids) => handle_user_batch(alma_client, categories_to_remove, user_ids).await,
                Err(error) => (0, vec![error.context(format!("Failed to get user ids for batch {}", offset))]),
            }
        });
        join_handles.push((offset, join_handle));
    }

    // Yield to let some tasks run
    tokio::task::yield_now().await;

    for (offset, join_handle) in join_handles {
        // Await each batch, and print any errors
        match join_handle.await {
            Ok((users_updated, errors)) => {
                info!("Batch {}: {} users updated", offset, users_updated);
                if !errors.is_empty() {
                    error!("{} errors in batch {}:", errors.len(), offset);
                    for error in errors {
                        error!("{}", error);
                    }
                }
            }
            Err(join_error) => error!("Join error for batch {}: {}", offset, join_error),
        }
    }

    Ok(())
}

async fn handle_user_batch(
    alma_client: alma::Client,
    categories_to_remove: Arc<HashSet<String>>,
    user_ids: Vec<String>,
) -> (usize, Vec<Error>) {
    let mut users_updated = 0;
    let mut errors = Vec::new();
    for user_id in user_ids {
        match handle_user(&alma_client, &*categories_to_remove, &user_id).await {
            Ok(true) => users_updated += 1,
            Ok(false) => (),
            Err(error) => errors.push(error.context(format!("error handling user {}", user_id))),
        }
    }
    (users_updated, errors)
}

async fn handle_user(alma_client: &alma::Client, categories_to_remove: &HashSet<String>, user_id: &str) -> Result<bool> {
    let mut user_details = alma_client.get_user_details(&user_id).await?;
    if strip_user_statistics_by_category(categories_to_remove, &mut user_details) {
        alma_client.update_user_details(user_id, user_details).await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

fn strip_user_statistics_by_category(categories_to_remove: &HashSet<String>, user: &mut JsonValue) -> bool {
    if let JsonValue::Array(user_statistics) = &mut user["user_statistic"] {
        let stats_count = user_statistics.len();
        // Remove the categories
        user_statistics.retain(|statistic| {
            if let Some(category) = statistic["category_type"]["value"].as_str() {
                // Retain if this category is not in the list
                !categories_to_remove.contains(category)
            } else {
                // If the category type is not present for some reason, just leave it as is
                true
            }
        });
        // If the count differs, the user was updated
        if stats_count != user_statistics.len() {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use maplit::hashset;

    #[test]
    fn test_json_strip_fn() {
        let mut user_json = json::parse(
            r#"
        {
            "user_statistic": [
                {
                    "statistic_category": {
                        "value": "RC_60",
                        "desc": "RC Libraries"
                    },
                    "category_type": {
                        "value": "RESPONSIBILITY_CENTER",
                        "desc": "Responsibility Center (RC)"
                    },
                    "statistic_note": "Libraries",
                    "segment_type": "External"
                },
                {
                    "statistic_category": {
                        "value": "FPT_FR",
                        "desc": "FPT Fulltime Regular"
                    },
                    "category_type": {
                        "value": "FULL_PART_TIME",
                        "desc": "Full or Part Time Enrollment (FPT)"
                    },
                    "statistic_note": "Fulltime-Regular",
                    "segment_type": "External"
                },
                {
                    "statistic_category": {
                        "value": "ED_62050",
                        "desc": "ED (62050) Information Technology"
                    },
                    "category_type": {
                        "value": "EMPLOYEE_DEPT",
                        "desc": "Employee Department (ED)"
                    },
                    "statistic_note": "Information Technology",
                    "segment_type": "External"
                }
            ]
        }"#,
        )
        .unwrap();
        let categories = hashset![String::from("FULL_PART_TIME")];
        let updated = strip_user_statistics_by_category(&categories, &mut user_json);
        assert!(updated);
        assert_eq!(
            user_json,
            json::parse(
                r#"
            {
                "user_statistic": [
                    {
                        "statistic_category": {
                            "value": "RC_60",
                            "desc": "RC Libraries"
                        },
                        "category_type": {
                            "value": "RESPONSIBILITY_CENTER",
                            "desc": "Responsibility Center (RC)"
                        },
                        "statistic_note": "Libraries",
                        "segment_type": "External"
                    },
                    {
                        "statistic_category": {
                            "value": "ED_62050",
                            "desc": "ED (62050) Information Technology"
                        },
                        "category_type": {
                            "value": "EMPLOYEE_DEPT",
                            "desc": "Employee Department (ED)"
                        },
                        "statistic_note": "Information Technology",
                        "segment_type": "External"
                    }
                ]
            }"#
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn test_get_user_ids_api() {
        dotenv::dotenv().ok();
        let alma_client = alma::Client::new(env::var("ALMA_REGION").unwrap(), env::var("ALMA_APIKEY").unwrap());
        let user_ids = alma_client.get_user_ids(0, 100).await.unwrap();
        assert_eq!(user_ids.len(), 100);
    }

    #[tokio::test]
    async fn test_get_user_ids_and_count_api() {
        dotenv::dotenv().ok();
        let alma_client = alma::Client::new(env::var("ALMA_REGION").unwrap(), env::var("ALMA_APIKEY").unwrap());
        let (user_ids, total) = alma_client.get_user_ids_and_total_count(0, 100).await.unwrap();
        assert_eq!(user_ids.len(), 100);
        assert!(total > 0);
    }
}
