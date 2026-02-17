//! Search command implementation
//!
//! Search for published packages in the Store.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

const DEFAULT_STORE_API_URL: &str = "https://api.ato.run";
const ENV_STORE_API_URL: &str = "ATO_STORE_API_URL";

/// Store API package summary (from GET /v1/capsules)
#[derive(Debug, Deserialize)]
struct CapsulesResponse {
    capsules: Vec<CapsuleSummary>,
    #[serde(default, alias = "nextCursor")]
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CapsuleSummary {
    id: String,
    slug: String,
    name: String,
    description: String,
    category: String,
    #[serde(rename = "type")]
    capsule_type: String,
    price: u64,
    currency: String,
    publisher: PublisherInfo,
    #[serde(rename = "latestVersion", alias = "latest_version")]
    latest_version: String,
    downloads: u64,
    #[serde(rename = "createdAt", alias = "created_at")]
    created_at: String,
    #[serde(rename = "updatedAt", alias = "updated_at")]
    updated_at: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct PublisherInfo {
    handle: String,
    #[serde(rename = "authorDid", alias = "author_did")]
    author_did: String,
    verified: bool,
}

fn normalize_base_url(raw: &str) -> String {
    raw.trim().trim_end_matches('/').to_string()
}

fn default_store_api_url() -> String {
    std::env::var(ENV_STORE_API_URL)
        .ok()
        .map(|value| normalize_base_url(&value))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_STORE_API_URL.to_string())
}

/// Search result
#[derive(Debug, Serialize)]
pub struct SearchResult {
    pub capsules: Vec<CapsuleSummary>,
    pub total: usize,
    pub next_cursor: Option<String>,
}

/// Search for packages in the store
pub async fn search_capsules(
    query: Option<&str>,
    category: Option<&str>,
    tags: Option<&[String]>,
    limit: Option<usize>,
    cursor: Option<&str>,
    registry_url: Option<&str>,
    json_output: bool,
) -> Result<SearchResult> {
    // Resolve registry URL
    let registry = if let Some(url) = registry_url {
        normalize_base_url(url)
    } else {
        let info = default_store_api_url();
        if !json_output {
            println!("📡 Using registry: {}", info);
        }
        info
    };

    let client = reqwest::Client::new();

    // Build query parameters
    let mut url = format!("{}/v1/capsules", registry);
    let mut params = Vec::new();

    if let Some(q) = query {
        params.push(format!("q={}", urlencoding::encode(q)));
    }
    if let Some(c) = category {
        params.push(format!("category={}", urlencoding::encode(c)));
    }
    if let Some(tags) = tags {
        for tag in tags
            .iter()
            .map(|tag| tag.trim())
            .filter(|tag| !tag.is_empty())
        {
            params.push(format!("tag={}", urlencoding::encode(tag)));
        }
    }
    let limit_val = limit.unwrap_or(20).min(50);
    params.push(format!("limit={}", limit_val));
    if let Some(c) = cursor {
        params.push(format!("cursor={}", urlencoding::encode(c)));
    }

    if !params.is_empty() {
        url.push('?');
        url.push_str(&params.join("&"));
    }

    let response: CapsulesResponse = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("Failed to search capsules: {}", registry))?
        .json()
        .await
        .with_context(|| "Invalid search response")?;

    let total = response.capsules.len();

    if !json_output {
        if total == 0 {
            println!("🔍 No packages found.");
        } else {
            println!("🔍 Found {} package(s):", total);
        }

        for (index, capsule) in response.capsules.iter().enumerate() {
            println!();
            println!("{}. {} ({})", index + 1, capsule.name, capsule.slug);
            if !capsule.description.is_empty() {
                println!("   {}", capsule.description);
            }
            println!(
                "   Category: {} | Type: {} | Version: {}",
                capsule.category, capsule.capsule_type, capsule.latest_version
            );
            println!(
                "   Publisher: {}{} | Downloads: {}",
                capsule.publisher.handle,
                if capsule.publisher.verified {
                    " ✓"
                } else {
                    ""
                },
                capsule.downloads
            );
            if capsule.price == 0 {
                println!("   Price: Free");
            } else {
                println!("   Price: {} {}", capsule.price, capsule.currency);
            }
            println!("   Install: ato install {}", capsule.slug);
        }

        if let Some(ref next) = response.next_cursor {
            println!();
            println!("📄 Next cursor: {}", next);
            println!("   Continue: ato search --cursor {}", next);
        }
    }

    Ok(SearchResult {
        capsules: response.capsules,
        total,
        next_cursor: response.next_cursor,
    })
}
