use anyhow::{anyhow, Result};
use json::JsonValue;
use log::debug;
use quick_xml::{events::Event, Reader};
use std::{str, sync::Arc};

/// Client object for making Alma API calls. Uses `Arc` internally to be cheaply cloneable.
#[derive(Clone)]
pub struct Client {
    client: reqwest::Client,
    data: Arc<ClientData>,
}

struct ClientData {
    base_url: reqwest::Url,
    apikey: String,
}

impl ClientData {
    pub fn new(region: impl Into<String>, apikey: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            base_url: format!("https://api-{}.hosted.exlibrisgroup.com/almaws/v1/", region.into()).parse().unwrap(),
            apikey: apikey.into(),
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

    /// Given an offset and limit, make a GET request to the `/users` endpoint,
    /// then pull out user ids and the total record count from the xml response body.
    pub async fn get_user_ids_and_total_count(&self, offset: usize, limit: usize) -> Result<(Vec<String>, usize)> {
        // Construct the url for the request
        let mut url = self.data.base_url.join(&format!("users?limit={}&offset={}", limit, offset))?;
        debug!("GET {}", url);
        url.query_pairs_mut().append_pair("apikey", &self.data.apikey);
        // Send the request, and get the body as a string
        let user_batch_response = self
            .client
            .get(url)
            .header(reqwest::header::ACCEPT, "application/xml")
            .send()
            .await?
            .error_for_status()?
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
        // Construct the url for the request
        let mut url = self.data.base_url.join(&format!("users?limit={}&offset={}", limit, offset))?;
        debug!("GET {}", url);
        url.query_pairs_mut().append_pair("apikey", &self.data.apikey);
        // Send the request, and get the body as a string
        let user_batch_response = self
            .client
            .get(url)
            .header(reqwest::header::ACCEPT, "application/xml")
            .send()
            .await?
            .error_for_status()?
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
        // Construct the url for the request
        let mut url = self.data.base_url.join(&format!("users/{}", user_id))?;
        debug!("GET {}", url);
        url.query_pairs_mut().append_pair("apikey", &self.data.apikey);
        // Send the request, and get the body as a string
        let user_response = self
            .client
            .get(url.clone())
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        // Parse the body into a json object and return
        Ok(json::parse(&user_response)?)
    }

    /// Update a user's details with a PUT request
    pub async fn update_user_details(&self, user_id: &str, user_details: JsonValue) -> Result<()> {
        // Construct the url for the request
        let mut url = self.data.base_url.join(&format!("users/{}", user_id))?;
        debug!("PUT {}", url);
        url.query_pairs_mut().append_pair("apikey", &self.data.apikey);        
        // Send the updated user
        self.client
            .put(url)
            .body(user_details.dump())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}
