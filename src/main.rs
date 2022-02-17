use anyhow::{Error, Result};
use json::JsonValue;
use log::{error, info};
use std::{
    env,
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
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
    let categories_to_remove: Vec<_> = BufReader::new(categories_file).lines().map(|l| l.unwrap()).collect();
    // Leak the list of categories. This ensures that they will not be dropped until the program exits, and can be referenced from other threads
    let categories_to_remove = &*Box::leak(categories_to_remove.into_boxed_slice());
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
        tokio::spawn(handle_user_batch(alma_client.clone(), categories_to_remove, user_ids)),
    ));
    // Determine the last offset for this run
    let last_offset = options.to_offset.unwrap_or(total_users / LIMIT).min(total_users / LIMIT);
    // Split up the rest of the users into batches
    for offset in (options.from_offset + 1)..=last_offset {
        let alma_client = alma_client.clone();
        // Spawn a task for each batch
        info!("Spawning task for batch {}", offset);
        let join_handle = tokio::spawn(async move {
            let user_ids = alma_client
                .get_user_ids(offset, LIMIT)
                .await
                .map_err(|err| vec![err.context(format!("Failed to get user ids for batch {}", offset))])?;
            handle_user_batch(alma_client, categories_to_remove, user_ids).await
        });
        join_handles.push((offset, join_handle));
    }

    // Yield to let some tasks run
    tokio::task::yield_now().await;

    for (offset, join_handle) in join_handles {
        // Await each batch, and print any errors
        match join_handle.await {
            Ok(Ok(())) => (),
            Ok(Err(errors)) => {
                error!("Errors in batch {}:", offset);
                for error in errors {
                    error!("    {}", error);
                }
            }
            Err(join_error) => error!("Join error for batch {}: {}", offset, join_error),
        }
    }

    Ok(())
}

async fn handle_user_batch(
    alma_client: alma::Client,
    categories_to_remove: &'static [String],
    user_ids: Vec<String>,
) -> Result<(), Vec<Error>> {
    let mut errors = Vec::new();
    for user_id in user_ids {
        if let Err(error) = alma_client
            .update_user_details(&user_id, |user| strip_user_statistics_by_category(categories_to_remove, user))
            .await
        {
            errors.push(error.context(format!("error handling user {}", user_id)));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn strip_user_statistics_by_category(categories_to_remove: &[String], mut user: JsonValue) -> Option<JsonValue> {
    if let JsonValue::Array(user_statistics) = &mut user["user_statistic"] {
        let stats_count = user_statistics.len();
        // Remove the categories
        user_statistics.retain(|statistic| {
            if let Some(category) = statistic["category_type"]["value"].as_str() {
                // Retain if this category is not in the list
                categories_to_remove.iter().all(|c| c != category)
            } else {
                // If the category type is not present for some reason, just leave it as is
                true
            }
        });
        // If the count differs, the user was updated
        if stats_count != user_statistics.len() {
            Some(user)
        } else {
            None
        }
    } else {
        // User statistics are missing/not an array, so just leave it
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_strip_fn() {
        let user_json = json::parse(
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

        let updated = strip_user_statistics_by_category(&[String::from("FULL_PART_TIME")], user_json).unwrap();
        assert_eq!(
            updated,
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
    async fn test_get_user_ids() {
        dotenv::dotenv().ok();
        let alma_client = alma::Client::new(env::var("ALMA_REGION").unwrap(), env::var("ALMA_APIKEY").unwrap());
        let user_ids = alma_client.get_user_ids(0, 100).await.unwrap();
        assert_eq!(user_ids.len(), 100);
    }

    #[tokio::test]
    async fn test_get_user_ids_and_count() {
        dotenv::dotenv().ok();
        let alma_client = alma::Client::new(env::var("ALMA_REGION").unwrap(), env::var("ALMA_APIKEY").unwrap());
        let (user_ids, total) = alma_client.get_user_ids_and_total_count(0, 100).await.unwrap();
        assert_eq!(user_ids.len(), 100);
        assert!(total > 0);
    }
}
