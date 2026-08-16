//! A direct client for GOG's GamesDB API, used instead of the `gog`
//! crate's `get_game_details` (which hits `embed.gog.com/account/...`
//! and rate-limits hard under the one-call-per-owned-game access
//! pattern `fetch_games` needs). GamesDB is the same catalog data
//! Heroic's launcher uses for its library, is public/unauthenticated,
//! and tolerates that access pattern.

use serde::Deserialize;

const EXTERNAL_RELEASES_URL: &str = "https://gamesdb.gog.com/platforms/gog/external_releases";

/// A GOG release as returned by `GET .../external_releases/{id}`,
/// keyed by GOG's numeric product id (the same id `gog::get_games`
/// returns for owned games).
#[derive(Debug, Deserialize)]
pub struct Release {
    pub title: Localized,
    pub summary: Option<Localized>,
    pub icon: Option<Image>,
    pub game: GameDetails,
}

#[derive(Debug, Deserialize)]
pub struct GameDetails {
    pub cover: Option<Image>,
    pub vertical_cover: Option<Image>,
    pub background: Option<Image>,
    #[serde(default)]
    pub developers: Vec<Company>,
    #[serde(default)]
    pub publishers: Vec<Company>,
    #[serde(default)]
    pub genres: Vec<Genre>,
    pub first_release_date: Option<String>,
    pub aggregated_rating: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct Company {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct Genre {
    pub name: Localized,
}

/// GamesDB text fields are keyed by locale, with `"*"` as the
/// language-agnostic fallback.
#[derive(Debug, Deserialize)]
pub struct Localized {
    #[serde(rename = "*")]
    default: Option<String>,
    #[serde(rename = "en-US")]
    en_us: Option<String>,
}

impl Localized {
    pub fn resolve(&self) -> Option<String> {
        self.en_us.clone().or_else(|| self.default.clone())
    }
}

/// A sized-image reference. `url_format` carries `{formatter}` (a
/// size/crop variant selector) and `{ext}` placeholders; substituting
/// them with nothing and `jpg` yields a valid default-size image URL.
#[derive(Debug, Deserialize)]
pub struct Image {
    url_format: String,
}

impl Image {
    pub fn resolve(&self) -> String {
        self.url_format.replace("{formatter}", "").replace("{ext}", "jpg")
    }
}

/// Parses GamesDB's `first_release_date` (e.g. `"2012-04-17T00:00:00+0000"`)
/// into a proto `Timestamp`. Returns `None` on any format surprise rather
/// than failing the whole game — this is decorative metadata.
pub fn parse_release_date(date: &str) -> Option<prost_wkt_types::Timestamp> {
    chrono::DateTime::parse_from_str(date, "%Y-%m-%dT%H:%M:%S%z")
        .ok()
        .map(|parsed| prost_wkt_types::Timestamp::from(std::time::SystemTime::from(parsed)))
}

/// Fetches a single release's catalog data by GOG product id. Blocking —
/// callers already run inside `tokio::task::spawn_blocking` alongside the
/// `gog` crate's own blocking calls.
pub fn fetch_release(client: &reqwest::blocking::Client, id: i64) -> reqwest::Result<Release> {
    client
        .get(format!("{EXTERNAL_RELEASES_URL}/{id}"))
        .send()?
        .error_for_status()?
        .json()
}
