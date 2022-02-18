use anyhow::{anyhow, Result};
use governor::{Jitter, Quota};
use json::JsonValue;
use log::debug;
use quick_xml::{events::Event, Reader};
use reqwest::{Response, StatusCode};
use std::{fmt, num::NonZeroU32, str, sync::Arc, time::Duration};
use thiserror::Error;

/// Client object for making Alma API calls. Uses `Arc` internally to be cheaply cloneable.
#[derive(Clone)]
pub struct Client {
    client: reqwest::Client,
    data: Arc<ClientData>,
}

struct ClientData {
    base_url: reqwest::Url,
    apikey: String,
    rate_limiter: RateLimiter,
}

type RateLimiter = governor::RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::QuantaClock,
    governor::middleware::NoOpMiddleware<governor::clock::QuantaInstant>,
>;

impl ClientData {
    pub fn new(region: impl Into<String>, apikey: impl Into<String>) -> Arc<Self> {
        let rate_limiter = RateLimiter::direct(Quota::per_second(NonZeroU32::new(10).unwrap()));
        Arc::new(Self {
            base_url: format!("https://api-{}.hosted.exlibrisgroup.com/almaws/v1/", region.into()).parse().unwrap(),
            apikey: apikey.into(),
            rate_limiter,
        })
    }
}

impl Client {
    /// Construct a new Alma client with the given region and api key.
    pub fn new(region: impl Into<String>, apikey: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            data: ClientData::new(region, apikey),
        }
    }

    async fn until_ready(&self) {
        let jitter = Jitter::up_to(Duration::from_millis(75));
        self.data.rate_limiter.until_ready_with_jitter(jitter).await;
    }

    /// Given an offset and limit, make a GET request to the `/users` endpoint,
    /// then pull out user ids and the total record count from the xml response body.
    pub async fn get_user_ids_and_total_count(&self, offset: usize, limit: usize) -> Result<(Vec<String>, usize)> {
        self.until_ready().await;
        // Construct the url for the request
        let mut url =
            self.data.base_url.join(&format!("users?order_by=primary_id&limit={}&offset={}", limit, offset))?;
        debug!("GET {}", url);
        url.query_pairs_mut().append_pair("apikey", &self.data.apikey);
        // Send the request, and get the body as a string
        let user_batch_response =
            check_error(self.client.get(url).header(reqwest::header::ACCEPT, "application/xml").send().await?)
                .await?
                .text()
                .await?;
        // Variables to hold the results
        let mut user_ids = Vec::with_capacity(limit);
        let mut total_record_count: Option<usize> = None;
        // Xml reader, and a buffer for it to use
        let mut xml_reader = Reader::from_str(&user_batch_response);
        let mut xml_buf = Vec::new();
        loop {
            // Read an xml element into the buffer
            let event = xml_reader.read_event(&mut xml_buf)?;
            match event {
                Event::Start(e) => {
                    if e.name() == b"users" {
                        // When we see the <users> element, look for the `total_record_count` attribute and save it into our variable
                        total_record_count = e.attributes().find_map(|a| {
                            a.ok().and_then(|a| {
                                if a.key == b"total_record_count" {
                                    str::from_utf8(&a.value).ok().and_then(|n| n.parse().ok())
                                } else {
                                    None
                                }
                            })
                        });
                    } else if e.name() == b"primary_id" {
                        // Drop the event so we can mutate the buffer again
                        drop(e);
                        // When we see the <primary_id> element, the text inside it is a user id
                        user_ids.push(xml_reader.read_text(b"primary_id", &mut xml_buf)?);
                    }
                }
                Event::Eof => {
                    // After reading it all, make sure we found the `total_record_count` and return
                    return Ok((
                        user_ids,
                        total_record_count.ok_or_else(|| anyhow!("failed to get total record count"))?,
                    ));
                }
                _ => {}
            }
            xml_buf.clear();
        }
    }

    /// Given an offset and limit, make a GET request to the `/users` endpoint,
    /// then pull out user ids from the xml response body.
    pub async fn get_user_ids(&self, offset: usize, limit: usize) -> Result<Vec<String>> {
        self.until_ready().await;
        // Construct the url for the request
        let mut url =
            self.data.base_url.join(&format!("users?order_by=primary_id&limit={}&offset={}", limit, offset))?;
        debug!("GET {}", url);
        url.query_pairs_mut().append_pair("apikey", &self.data.apikey);
        // Send the request, and get the body as a string
        let user_batch_response =
            check_error(self.client.get(url).header(reqwest::header::ACCEPT, "application/xml").send().await?)
                .await?
                .text()
                .await?;
        // A vector to hold the results
        let mut user_ids = Vec::with_capacity(limit);
        // Xml reader, and a buffer for it to use
        let mut xml_reader = Reader::from_str(&user_batch_response);
        let mut xml_buf = Vec::new();
        loop {
            let event = xml_reader.read_event(&mut xml_buf)?;
            match event {
                Event::Start(e) if e.name() == b"primary_id" => {
                    // Drop the event so we can mutate the buffer again
                    drop(e);
                    // When we see the <primary_id> element, the text inside it is a user id
                    user_ids.push(xml_reader.read_text(b"primary_id", &mut xml_buf)?);
                }
                Event::Eof => return Ok(user_ids),
                _ => {}
            }
            xml_buf.clear();
        }
    }

    /// Get a user's details as a JSON object
    pub async fn get_user_details(&self, user_id: &str) -> Result<JsonValue> {
        self.until_ready().await;
        // Construct the url for the request
        let mut url = self.data.base_url.join(&format!("users/{}", user_id))?;
        debug!("GET {}", url);
        url.query_pairs_mut().append_pair("apikey", &self.data.apikey);
        // Send the request, and get the body as a string
        let user_response =
            check_error(self.client.get(url).header(reqwest::header::ACCEPT, "application/json").send().await?)
                .await?
                .text()
                .await?;
        // Parse the body into a json object and return
        Ok(json::parse(&user_response)?)
    }

    /// Update a user's details with a PUT request
    pub async fn update_user_details(&self, user_id: &str, user_details: JsonValue) -> Result<()> {
        self.until_ready().await;
        // Construct the url for the request
        let mut url = self.data.base_url.join(&format!("users/{}", user_id))?;
        debug!("PUT {}", url);
        url.query_pairs_mut().append_pair("apikey", &self.data.apikey);
        // Send the updated user
        check_error(
            self.client
                .put(url)
                .body(user_details.dump())
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .send()
                .await?,
        )
        .await?;
        Ok(())
    }
}

#[derive(Debug, Error)]
#[error("Alma API error:\n Status: {status_code}\n Error Code: {error_code}\n Error Message: {error_message}")]
pub struct AlmaError {
    status_code: StatusCode,
    error_code: String,
    error_message: String,
    tracking_id: String,
}

#[derive(Debug, Error)]
pub struct AlmaErrors(Vec<AlmaError>);

impl fmt::Display for AlmaErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for error in &self.0 {
            writeln!(f, "{}", error)?;
        }
        Ok(())
    }
}

async fn check_error(response: Response) -> Result<Response> {
    let status_code = response.status();
    if status_code.is_client_error() || status_code.is_server_error() {
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|h| h.to_str().ok())
            .ok_or_else(|| anyhow!("Alma API error {} with missing content type", status_code))?;
        match content_type.split(';').next().unwrap() {
            "application/xml" => {
                let body = response.text().await?;
                let mut xml_reader = Reader::from_str(&body);
                let mut xml_buf = Vec::new();
                let mut alma_errors = Vec::new();
                loop {
                    // Read an xml element into the buffer
                    let event = xml_reader.read_event(&mut xml_buf)?;
                    match event {
                        Event::Start(e) => match e.name() {
                            b"error" => alma_errors.push(AlmaError {
                                status_code,
                                error_code: String::new(),
                                error_message: String::new(),
                                tracking_id: String::new(),
                            }),
                            b"errorCode" => {
                                drop(e);
                                alma_errors.last_mut().unwrap().error_code =
                                    xml_reader.read_text(b"errorCode", &mut xml_buf)?;
                            }
                            b"errorMessage" => {
                                drop(e);
                                alma_errors.last_mut().unwrap().error_message =
                                    xml_reader.read_text(b"errorMessage", &mut xml_buf)?;
                            }
                            b"trackingId" => {
                                drop(e);
                                alma_errors.last_mut().unwrap().tracking_id =
                                    xml_reader.read_text(b"trackingId", &mut xml_buf)?;
                            }
                            _ => {}
                        },
                        Event::Eof => return Err(anyhow!(AlmaErrors(alma_errors))),
                        _ => {}
                    }
                    xml_buf.clear();
                }
            }
            "application/json" => {
                let body = json::parse(&response.text().await?)?;
                if let JsonValue::Object(error_list) = &body["errorList"] {
                    return Err(anyhow!(AlmaErrors(
                        error_list
                            .iter()
                            .map(|(_, error)| {
                                AlmaError {
                                    status_code,
                                    error_code: error["errorCode"].to_string(),
                                    error_message: error["errorMessage"].to_string(),
                                    tracking_id: error["trackingId"].to_string(),
                                }
                            })
                            .collect()
                    )));
                } else {
                    return Err(anyhow!("Alma API error {}, couldn't parse error message from json body", status_code));
                }
            }
            _ => return Err(anyhow!("Alma API error {} with unexpected content type {}", status_code, content_type)),
        }
    } else {
        Ok(response)
    }
}
