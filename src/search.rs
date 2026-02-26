//! Search command implementation
//!
//! Search for published packages in the Store.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

const DEFAULT_STORE_API_URL: &str = "https://api.ato.run";
const ENV_STORE_API_URL: &str = "ATO_STORE_API_URL";

/// Store API package summary (from GET /v1/capsules)
#[derive(Debug, Deserialize)]
struct RawCapsulesResponse {
    capsules: Vec<RawCapsuleSummary>,
    #[serde(default, alias = "nextCursor")]
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawCapsuleSummary {
    id: String,
    slug: String,
    #[serde(default)]
    scoped_id: Option<String>,
    #[serde(default, rename = "scopedId")]
    scoped_id_camel: Option<String>,
    name: String,
    description: String,
    category: String,
    #[serde(rename = "type")]
    capsule_type: String,
    price: u64,
    currency: String,
    publisher: PublisherInfo,
    #[serde(rename = "latestVersion", alias = "latest_version", default)]
    latest_version: Option<String>,
    downloads: u64,
    #[serde(rename = "createdAt", alias = "created_at")]
    created_at: String,
    #[serde(rename = "updatedAt", alias = "updated_at")]
    updated_at: String,
}

#[derive(Debug, Serialize)]
pub struct CapsuleSummary {
    id: String,
    slug: String,
    scoped_id: Option<String>,
    name: String,
    description: String,
    category: String,
    #[serde(rename = "type")]
    capsule_type: String,
    price: u64,
    currency: String,
    publisher: PublisherInfo,
    #[serde(rename = "latestVersion", alias = "latest_version", default)]
    latest_version: Option<String>,
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

impl From<RawCapsuleSummary> for CapsuleSummary {
    fn from(raw: RawCapsuleSummary) -> Self {
        let scoped_id = raw.scoped_id.or(raw.scoped_id_camel);
        Self {
            id: raw.id,
            slug: raw.slug,
            scoped_id,
            name: raw.name,
            description: raw.description,
            category: raw.category,
            capsule_type: raw.capsule_type,
            price: raw.price,
            currency: raw.currency,
            publisher: raw.publisher,
            latest_version: raw.latest_version,
            downloads: raw.downloads,
            created_at: raw.created_at,
            updated_at: raw.updated_at,
        }
    }
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

    let response: RawCapsulesResponse = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("Failed to search capsules: {}", registry))?
        .json()
        .await
        .with_context(|| "Invalid search response")?;

    let capsules: Vec<CapsuleSummary> = response.capsules.into_iter().map(Into::into).collect();
    let total = capsules.len();

    if !json_output {
        if total == 0 {
            println!("🔍 No packages found.");
        } else {
            println!("🔍 Found {} package(s):", total);
        }

        for (index, capsule) in capsules.iter().enumerate() {
            println!();
            println!("{}. {} ({})", index + 1, capsule.name, capsule.slug);
            if !capsule.description.is_empty() {
                println!("   {}", capsule.description);
            }
            println!(
                "   Category: {} | Type: {} | Version: {}",
                capsule.category,
                capsule.capsule_type,
                capsule.latest_version.as_deref().unwrap_or("unknown")
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
            let scoped_id = capsule
                .scoped_id
                .clone()
                .unwrap_or_else(|| format!("{}/{}", capsule.publisher.handle, capsule.slug));
            println!("   Install: ato install {}", scoped_id);
        }

        if let Some(ref next) = response.next_cursor {
            println!();
            println!("📄 Next cursor: {}", next);
            println!("   Continue: ato search --cursor {}", next);
        }
    }

    Ok(SearchResult {
        capsules,
        total,
        next_cursor: response.next_cursor,
    })
}

#[cfg(test)]
mod tests {
    use super::RawCapsulesResponse;

    #[test]
    fn parses_capsules_response_when_latest_version_is_null() {
        let raw = r#"{
            "capsules": [{
                "id": "01TEST",
                "slug": "sample-capsule",
                "name": "sample-capsule",
                "description": "sample",
                "category": "tools",
                "type": "app",
                "price": 0,
                "currency": "usd",
                "publisher": {
                    "handle": "koh0920",
                    "authorDid": "did:key:z6Mk...",
                    "verified": true
                },
                "latestVersion": null,
                "downloads": 2,
                "createdAt": "2026-02-14 05:55:45",
                "updatedAt": "2026-02-23T05:51:55.877Z"
            }],
            "next_cursor": null
        }"#;

        let parsed: RawCapsulesResponse = serde_json::from_str(raw).expect("should parse");
        assert_eq!(parsed.capsules.len(), 1);
        assert!(parsed.capsules[0].latest_version.is_none());
    }

    #[test]
    fn parses_capsules_response_with_both_scoped_keys() {
        let raw = r#"{
            "capsules": [{
                "id": "01TEST",
                "slug": "sample-capsule",
                "scoped_id": "koh0920/sample-capsule",
                "scopedId": "koh0920/sample-capsule",
                "name": "sample-capsule",
                "description": "sample",
                "category": "tools",
                "type": "app",
                "price": 0,
                "currency": "usd",
                "publisher": {
                    "handle": "koh0920",
                    "authorDid": "did:key:z6Mk...",
                    "verified": true
                },
                "latestVersion": "1.0.0",
                "downloads": 2,
                "createdAt": "2026-02-14 05:55:45",
                "updatedAt": "2026-02-23T05:51:55.877Z"
            }],
            "next_cursor": null
        }"#;

        let parsed: RawCapsulesResponse = serde_json::from_str(raw).expect("should parse");
        assert_eq!(parsed.capsules.len(), 1);
    }
}
