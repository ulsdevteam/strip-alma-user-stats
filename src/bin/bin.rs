use anyhow::Result;
use log::{error, info};
use std::env;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Options {
    #[structopt(short, long, default_value = "0")]
    from_offset: usize,
    #[structopt(short, long)]
    to_offset: Option<usize>,
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
    // Alma API page size
    const LIMIT: usize = 100;
    // Get the first batch of user ids, along with the total user count
    let (user_ids, total_users) = alma_client.get_user_ids_and_total_count(options.from_offset * LIMIT, LIMIT).await?;
    // Determine the last offset for this run
    let last_offset = options.to_offset.unwrap_or(total_users / LIMIT).min(total_users / LIMIT);

    #[cfg(feature = "concurrent")]
    {
        // Each page will get its own concurrent task, handles to which will be collected here with their offset
        let mut join_handles = Vec::new();
        // Spawn a task for the first batch
        info!("Spawning task for batch {}", options.from_offset);
        join_handles.push((options.from_offset, tokio::spawn(handle_user_batch(alma_client.clone(), user_ids))));
        // Split up the rest of the users into batches
        for offset in (options.from_offset + 1)..=last_offset {
            let alma_client = alma_client.clone();
            // Spawn a task for each batch
            info!("Spawning task for batch {}", offset);
            let join_handle = tokio::spawn(async move {
                match alma_client.get_user_ids(offset * LIMIT, LIMIT).await {
                    Ok(user_ids) => handle_user_batch(&alma_client, user_ids).await,
                    Err(error) => {
                        error!("Failed to get user ids for batch {}: {:#}", offset, error);
                        (0, 0)
                    }
                }
            });
            join_handles.push((offset, join_handle));
        }

        // Yield to let some tasks run
        tokio::task::yield_now().await;

        for (offset, join_handle) in join_handles {
            // Await each batch, and print any errors
            match join_handle.await {
                Ok((users_updated, errors)) => {}
                Err(join_error) => error!("Join error for batch {}: {}", offset, join_error),
            }
        }
    }

    #[cfg(not(feature = "concurrent"))]
    {
        info!("Starting batch {}", options.from_offset);
        let (users_updated, errors) = handle_user_batch(&alma_client, user_ids).await;
        info!("Batch {}: {} users updated. {} errors.", options.from_offset, users_updated, errors);
        for offset in (options.from_offset + 1)..=last_offset {
            let (users_updated, errors) = match alma_client.get_user_ids(offset * LIMIT, LIMIT).await {
                Ok(user_ids) => {
                    info!("Starting batch {}", offset);
                    handle_user_batch(&alma_client, user_ids).await
                }
                Err(error) => {
                    error!("Failed to get user ids for batch {}: {:#}", offset, error);
                    (0, 0)
                }
            };
            info!("Batch {}: {} users updated. {} errors.", offset, users_updated, errors);
        }
    }

    Ok(())
}

async fn handle_user_batch(alma_client: &alma::Client, user_ids: Vec<String>) -> (usize, usize) {
    let mut users_updated = 0;
    let mut errors = 0;
    for user_id in user_ids {
        match alma::handle_user(alma_client, &user_id).await {
            Ok(true) => users_updated += 1,
            Ok(false) => (),
            Err(error) => {
                errors += 1;
                error!("user {}: {:#}", user_id, error);
            }
        }
    }
    (users_updated, errors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use json::JsonValue;
    use log::warn;
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
        let updated = {
            let user_id = "test";
            let categories_to_remove = &categories;
            let user_details: &mut JsonValue = &mut user_json;
            if let JsonValue::Array(user_statistics) = &mut user_details["user_statistic"] {
                let stats_count = user_statistics.len();
                // Remove the categories
                user_statistics.retain(|statistic| {
                    if let Some("Internal") = statistic["segment_type"].as_str() {
                        warn!("user {} has internal statistic: {}", user_id, statistic);
                    }
                    if let Some(category) = statistic["category_type"]["value"].as_str() {
                        // Retain if this category is not in the list
                        !categories_to_remove.contains(category)
                    } else {
                        // If the category type is not present for some reason, just leave it as is
                        true
                    }
                });
                // If the count differs, the user was updated
                stats_count != user_statistics.len()
            } else {
                false
            }
        };
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
