//! A client for GOG Galaxy's builds/depot v2 API.

use std::io::Read;

use serde::Deserialize;

const CONTENT_SYSTEM: &str = "https://content-system.gog.com";
const CDN: &str = "https://gog-cdn-fastly.gog.com";

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("failed to decompress response: {0}")]
    Decompress(#[from] std::io::Error),
    #[error("failed to parse response: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("no builds available for this product")]
    NoBuilds,
}

/// GOG's CDN shards content by the first 4 hex chars of its hash:
/// `ab12cd...` -> `ab/12/ab12cd...`.
fn galaxy_path(hash: &str) -> String {
    if hash.contains('/') || hash.len() < 4 {
        hash.to_string()
    } else {
        format!("{}/{}/{}", &hash[0..2], &hash[2..4], hash)
    }
}

fn zlib_decompress(bytes: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut decoder = flate2::read::ZlibDecoder::new(bytes);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out)?;
    Ok(out)
}

#[derive(Debug, Deserialize)]
struct BuildsResponse {
    total_count: u64,
    items: Vec<Build>,
}

#[derive(Debug, Deserialize)]
pub struct Build {
    pub build_id: String,
    pub branch: Option<String>,
    pub link: String,
}

/// Lists available Windows builds for `game_id`, newest/mainline first
/// per GOG's own ordering (matches gogdl's default-to-`items[0]`,
/// prefer-`branch == None` selection).
pub async fn get_builds(
    http: &reqwest::Client,
    access_token: &str,
    game_id: i64,
) -> Result<Vec<Build>> {
    let url = format!("{CONTENT_SYSTEM}/products/{game_id}/os/windows/builds?generation=2");
    let response = http
        .get(&url)
        .bearer_auth(access_token)
        .send()
        .await?
        .error_for_status()?;
    let parsed: BuildsResponse = response.json().await?;
    if parsed.total_count == 0 {
        return Err(Error::NoBuilds);
    }
    Ok(parsed.items)
}

pub fn select_build(builds: &[Build]) -> Option<&Build> {
    builds
        .iter()
        .find(|build| build.branch.is_none())
        .or_else(|| builds.first())
}

#[derive(Debug, Deserialize)]
pub struct BuildMeta {
    /// GOG's own canonical install folder name for this game (what
    /// gogdl/Galaxy install under `Program Files\`) — depot files'
    /// relative paths are flat, with no game-named prefix of their own.
    #[serde(rename = "installDirectory")]
    pub install_directory: String,
    pub depots: Vec<DepotMeta>,
}

#[derive(Debug, Deserialize)]
pub struct DepotMeta {
    #[serde(rename = "osBitness", default)]
    pub os_bitness: Option<Vec<String>>,
    pub manifest: String,
}

impl DepotMeta {
    fn is_windows_compatible(&self) -> bool {
        self.os_bitness
            .as_ref()
            .is_none_or(|bitness| bitness.iter().any(|b| b == "64" || b == "32"))
    }
}

/// Fetches and parses the build manifest a `Build.link` points at
/// (zlib-compressed JSON, same as the per-depot manifest below).
pub async fn get_build_meta(
    http: &reqwest::Client,
    access_token: &str,
    link: &str,
) -> Result<BuildMeta> {
    let bytes = http
        .get(link)
        .bearer_auth(access_token)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    let decompressed = zlib_decompress(&bytes)?;
    Ok(serde_json::from_slice(&decompressed)?)
}

pub fn select_depot(meta: &BuildMeta) -> Option<&DepotMeta> {
    meta.depots
        .iter()
        .find(|depot| depot.is_windows_compatible())
}

#[derive(Debug, Deserialize)]
struct DepotManifestRoot {
    depot: DepotManifestBody,
}

#[derive(Debug, Deserialize)]
struct DepotManifestBody {
    #[serde(default)]
    items: Vec<DepotItem>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum DepotItem {
    DepotFile(DepotFile),
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
pub struct DepotFile {
    pub path: String,
    #[serde(default)]
    pub chunks: Vec<DepotChunk>,
}

#[derive(Debug, Deserialize)]
pub struct DepotChunk {
    pub md5: String,
    #[serde(rename = "compressedMd5")]
    pub compressed_md5: String,
    pub size: u64,
}

/// Fetches and parses one depot's file manifest — the CDN is public
/// content, no auth needed (same as `gamesdb.rs`'s GamesDB calls).
pub async fn get_depot_files(
    http: &reqwest::Client,
    manifest_hash: &str,
) -> Result<Vec<DepotFile>> {
    let url = format!(
        "{CDN}/content-system/v2/meta/{}",
        galaxy_path(manifest_hash)
    );
    let bytes = http
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    let decompressed = zlib_decompress(&bytes)?;
    let root: DepotManifestRoot = serde_json::from_slice(&decompressed)?;
    Ok(root
        .depot
        .items
        .into_iter()
        .filter_map(|item| match item {
            DepotItem::DepotFile(file) => Some(file),
            DepotItem::Other => None,
        })
        .collect())
}

#[derive(Debug, Deserialize)]
struct SecureLinkResponse {
    urls: Vec<SecureLinkEntry>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SecureLinkEntry {
    url: Option<String>,
    #[serde(rename = "url_format")]
    url_format: Option<String>,
    #[serde(default)]
    parameters: std::collections::HashMap<String, serde_json::Value>,
}

/// Requests the CDN endpoints usable to build chunk-download URLs for
/// this product. `path` is the base path seeded before appending each
/// chunk's own hash.
pub async fn get_secure_link(
    http: &reqwest::Client,
    access_token: &str,
    game_id: i64,
    path: &str,
) -> Result<Vec<SecureLinkEntry>> {
    let url = format!(
        "{CONTENT_SYSTEM}/products/{game_id}/secure_link?_version=2&generation=2&path={path}"
    );
    let response = http
        .get(&url)
        .bearer_auth(access_token)
        .send()
        .await?
        .error_for_status()?;
    let parsed: SecureLinkResponse = response.json().await?;
    Ok(parsed.urls)
}

/// Builds the final downloadable URL for one chunk, mirroring gogdl's
/// `_get_download_url_v2`: append the chunk's compressed-bytes hash
/// (galaxy-sharded) onto the endpoint's base path, then substitute the
/// resulting parameters into its `{placeholder}` URL template.
pub fn build_chunk_url(entry: &SecureLinkEntry, compressed_md5: &str) -> Option<String> {
    let template = entry.url_format.as_deref().or(entry.url.as_deref())?;
    let mut parameters = entry.parameters.clone();
    let appended_path = format!(
        "{}/{}",
        parameters
            .get("path")
            .and_then(|value| value.as_str())
            .unwrap_or_default(),
        galaxy_path(compressed_md5)
    );
    parameters.insert("path".to_string(), serde_json::Value::String(appended_path));

    let mut url = template.to_string();
    for (key, value) in &parameters {
        // GOG's secure_link parameters mix strings (base_url, path,
        // token) with bare JSON numbers (expires_at, dirs) — both need
        // substituting into the `{placeholder}` template, so `as_str()`
        // alone isn't enough.
        let substituted = value
            .as_str()
            .map(str::to_string)
            .or_else(|| value.as_i64().map(|n| n.to_string()))
            .or_else(|| value.as_u64().map(|n| n.to_string()));
        if let Some(value) = substituted {
            url = url.replace(&format!("{{{key}}}"), &value);
        }
    }
    Some(url)
}
