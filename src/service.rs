//! The GOG `Store` implementation.
//!
//! `GogStoreService` is the whole plugin: it holds the in-flight login
//! challenges and the credential store, and implements every `Store` RPC
//! directly against them, following the same shape as
//! `next-plugin-egs::service::EpicGamesStoreService`.
//!
//! The `gog` crate is synchronous (built on `reqwest::blocking`), so
//! every call into it runs inside `tokio::task::spawn_blocking` rather
//! than blocking an async worker thread directly.

use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use bottles_core::{credentials::CredentialStore, error::CredentialError};
use futures_core::Stream;
use gog::{Gog, token::Token};
use next_proto::bottles::{
    common::v1::{AuthState, Game, LinkedAccount, Storefront},
    library::v1::{GameAdded, GameEvent, GameRemoved, game_event},
    plugin::v1::{
        BeginLoginRequest, Chunk, CompleteLoginRequest, GetInstallManifestRequest, InstallFile,
        InstallManifest, ListGamesRequest, ListGamesResponse, LoginChallenge,
        OAuthRedirectChallenge, RefreshSessionRequest, RevokeSessionRequest, WatchGamesRequest,
        login_challenge::Kind, plugin_server::Plugin,
    },
};
use tokio::sync::{RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, async_trait};
use uuid::Uuid;

use crate::{
    depot, error,
    gamesdb::{self, Image, Localized},
};

/// GOG Galaxy's public client ID, used by every third-party GOG client
/// (Heroic, Minigalaxy, Lutris, ...) since GOG has no per-application
/// client registration for this flow.
const GOG_CLIENT_ID: &str = "46899977096215655";

/// Where GOG redirects back to after a successful login, carrying the
/// authorization `code` as a query parameter. Not a real endpoint we
/// control — the caller is expected to capture this redirect (e.g. via
/// an embedded webview) rather than us standing up a listener for it.
const GOG_REDIRECT_URI: &str = "https://embed.gog.com/on_login_success?origin=client";

/// How often `WatchGames` polls the storefront for library changes. GOG
/// doesn't offer a push API for this, so polling is the only option.
const WATCH_GAMES_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// How long a login challenge stays claimable after `BeginLogin` issues
/// it, before `CompleteLogin` rejects it as expired.
const LOGIN_CHALLENGE_TTL: Duration = Duration::from_secs(300);

/// A login challenge issued by `BeginLogin`, awaiting `CompleteLogin`.
/// Tracked only by creation time — the challenge itself carries no other
/// state since GOG's flow is a single authorization-code exchange.
struct PendingChallenge {
    created_at: Instant,
}

/// GOG storefront plugin, serving `bottles.plugin.v1.Plugin` over gRPC.
/// One instance is created per process (see `main.rs`) and lives for
/// the plugin's lifetime.
pub struct GogStoreService<C: CredentialStore> {
    /// Login challenges issued but not yet completed, keyed by
    /// `challenge_id`. A challenge is only removed once `CompleteLogin`
    /// succeeds or it's found expired — a failed attempt (bad code,
    /// transient GOG API error) leaves it in place so the caller can
    /// retry without restarting the whole flow.
    challenges: Arc<RwLock<HashMap<String, PendingChallenge>>>,
    /// Where linked-account credentials (a serialized `gog::token::Token`)
    /// are persisted, keyed by `(profile_id, Storefront::Gog)`.
    credentials: Arc<C>,
}

impl<C: CredentialStore> GogStoreService<C> {
    pub fn new(credentials: Arc<C>) -> Self {
        Self {
            challenges: Arc::new(RwLock::new(HashMap::new())),
            credentials,
        }
    }
}

impl<C: CredentialStore + Send + Sync + 'static> GogStoreService<C> {
    /// Loads this profile's stored GOG token, verifies (and transparently
    /// refreshes, via `gog`'s own auto-refresh) it against the API, and
    /// re-persists the token in case it was refreshed. Returns the
    /// resulting `Gog` client's user data. Shared between
    /// `RefreshSession` and anywhere else that needs "is this session
    /// still good" plus the user's identity in one round trip.
    async fn verify_session(
        credentials: &C,
        profile_id: &str,
    ) -> Result<gog::gog::UserData, Status> {
        let stored = credentials
            .load(profile_id, Storefront::Gog)
            .await
            .map_err(error::credentials)?
            .ok_or_else(|| error::credentials(CredentialError::NotFound))?;

        let token: Token = serde_json::from_str(&stored).map_err(error::json)?;

        let (user, refreshed_token) = tokio::task::spawn_blocking(move || {
            let gog = Gog::new(token);
            let user = gog.get_user_data();
            let final_token = gog.token.borrow().clone();
            (user, final_token)
        })
        .await
        .map_err(|err| Status::internal(format!("GOG worker task panicked: {err}")))?;

        let user = user.map_err(error::session_invalid)?;

        let refreshed = serde_json::to_string(&refreshed_token).map_err(error::json)?;
        credentials
            .save(profile_id, Storefront::Gog, &refreshed)
            .await
            .map_err(error::credentials)?;

        Ok(user)
    }

    /// Loads this profile's stored GOG token and lists owned games,
    /// fetching each game's catalog data (title, description, cover)
    /// individually since GOG's owned-games endpoint only returns IDs.
    /// Shared between `ListGames` and the polling loop `WatchGames`
    /// spawns.
    ///
    /// Per-game data comes from GamesDB (`gamesdb::fetch_release`)
    /// rather than the `gog` crate's `get_game_details`, which hits
    /// `embed.gog.com/account/gameDetails` and rate-limits hard under
    /// one-call-per-owned-game — after a handful of requests it starts
    /// returning empty bodies instead of data. GamesDB is the same
    /// public catalog data Heroic's launcher uses and tolerates this
    /// access pattern.
    async fn fetch_games(credentials: &C, profile_id: &str) -> Result<Vec<Game>, Status> {
        let ids = Self::fetch_game_ids(credentials, profile_id).await?;

        tokio::task::spawn_blocking(move || {
            let http = reqwest::blocking::Client::new();
            Ok(ids.into_iter().map(|id| resolve_game(&http, id)).collect())
        })
        .await
        .map_err(|err| Status::internal(format!("GOG worker task panicked: {err}")))?
    }

    /// Just the owned-product-id list — the cheap half of `fetch_games`,
    /// split out so `WatchGames` can resolve each id's catalog data
    /// individually and emit it as it arrives, instead of waiting for
    /// every game in the library to resolve before sending anything.
    async fn fetch_game_ids(credentials: &C, profile_id: &str) -> Result<Vec<i64>, Status> {
        let stored = credentials
            .load(profile_id, Storefront::Gog)
            .await
            .map_err(error::credentials)?
            .ok_or_else(|| error::credentials(CredentialError::NotFound))?;

        let token: Token = serde_json::from_str(&stored).map_err(error::json)?;

        tokio::task::spawn_blocking(move || Gog::new(token).get_games().map_err(error::api))
            .await
            .map_err(|err| Status::internal(format!("GOG worker task panicked: {err}")))?
    }
}

/// Resolves one owned product id's catalog data via GamesDB. Falls back
/// to the numeric id as the title on failure rather than dropping the
/// game — see `fetch_games`' doc comment for why GamesDB and not
/// `gog::get_game_details`.
fn resolve_game(http: &reqwest::blocking::Client, id: i64) -> Game {
    match gamesdb::fetch_release(http, id) {
        Ok(release) => Game {
            id: id.to_string(),
            title: release.title.resolve().unwrap_or_else(|| id.to_string()),
            storefront: Storefront::Gog as i32,
            description: release.summary.as_ref().and_then(Localized::resolve),
            icon_url: release.icon.as_ref().map(Image::resolve),
            cover_url: release
                .game
                .vertical_cover
                .as_ref()
                .or(release.game.cover.as_ref())
                .map(Image::resolve),
            background_url: release.game.background.as_ref().map(Image::resolve),
            developer: release.game.developers.first().map(|c| c.name.clone()),
            publisher: release.game.publishers.first().map(|c| c.name.clone()),
            genres: release
                .game
                .genres
                .iter()
                .filter_map(|genre| genre.name.resolve())
                .collect(),
            release_date: release
                .game
                .first_release_date
                .as_deref()
                .and_then(gamesdb::parse_release_date),
            rating: release
                .game
                .aggregated_rating
                .map(|score| score.to_string()),
            install_state: None,
        },
        Err(err) => {
            tracing::warn!("GamesDB fetch_release({id}) failed: {err}");
            Game {
                id: id.to_string(),
                title: id.to_string(),
                storefront: Storefront::Gog as i32,
                description: None,
                icon_url: None,
                cover_url: None,
                background_url: None,
                developer: None,
                publisher: None,
                genres: Vec::new(),
                release_date: None,
                rating: None,
                install_state: None,
            }
        }
    }
}

#[async_trait]
impl<C: CredentialStore + Send + Sync + 'static> Plugin for GogStoreService<C> {
    /// Issues a new login challenge pointing at GOG's OAuth authorization
    /// URL. `profile_id` is unused — the challenge isn't scoped to a
    /// profile until `CompleteLogin`.
    async fn begin_login(
        &self,
        _request: Request<BeginLoginRequest>,
    ) -> Result<Response<LoginChallenge>, Status> {
        let challenge_id = Uuid::new_v4().to_string();

        self.challenges.write().await.insert(
            challenge_id.clone(),
            PendingChallenge {
                created_at: Instant::now(),
            },
        );

        let kind = Kind::OauthRedirect(OAuthRedirectChallenge {
            auth_url: format!(
                "https://auth.gog.com/auth?client_id={GOG_CLIENT_ID}&redirect_uri={}&response_type=code&layout=client2",
                urlencoding_embed_redirect(),
            ),
            redirect_uri: GOG_REDIRECT_URI.to_string(),
        });

        Ok(Response::new(LoginChallenge {
            challenge_id,
            error: None,
            kind: Some(kind),
        }))
    }

    /// Exchanges the authorization `code` the caller captured from the
    /// redirect to `redirect_uri` for a session, then persists it. Only
    /// peeks the challenge until the exchange actually succeeds, so a
    /// bad/expired code doesn't burn the slot and force the caller to
    /// restart with a fresh `BeginLogin`.
    async fn complete_login(
        &self,
        request: Request<CompleteLoginRequest>,
    ) -> Result<Response<LinkedAccount>, Status> {
        let req = request.into_inner();

        let created_at = {
            let challenges = self.challenges.read().await;
            let challenge = challenges
                .get(&req.challenge_id)
                .ok_or_else(error::login_challenge_not_found)?;
            challenge.created_at
        };

        if created_at.elapsed() > LOGIN_CHALLENGE_TTL {
            self.challenges.write().await.remove(&req.challenge_id);
            return Err(error::login_challenge_expired());
        }

        if req.user_input.is_empty() {
            return Err(error::authorization_code_required());
        }

        let code = req.user_input;
        let (token, user) = tokio::task::spawn_blocking(move || {
            let token = Token::from_login_code(&code).map_err(|err| err.to_string())?;
            let gog = Gog::new(token);
            let user = gog.get_user_data().map_err(|err| err.to_string())?;
            let final_token = gog.token.borrow().clone();
            Ok::<_, String>((final_token, user))
        })
        .await
        .map_err(|err| Status::internal(format!("GOG worker task panicked: {err}")))?
        .map_err(error::authorization_failed)?;

        // Login succeeded — the challenge is now spent.
        self.challenges.write().await.remove(&req.challenge_id);

        tracing::info!("Logged in as {}", user.username);

        let credentials = serde_json::to_string(&token).map_err(error::json)?;
        self.credentials
            .save(&req.profile_id, Storefront::Gog, &credentials)
            .await
            .map_err(error::credentials)?;

        Ok(Response::new(LinkedAccount {
            storefront: Storefront::Gog as i32,
            account_display_name: user.username,
            account_id: user.user_id,
            auth_state: AuthState::Active as i32,
            linked_at: None,
            last_verified_at: None,
            expires_at: None,
        }))
    }

    /// Re-authenticates with the profile's stored session, verifying it
    /// still works (and letting `gog` transparently refresh it if
    /// needed) rather than performing a fresh interactive login.
    async fn refresh_session(
        &self,
        request: Request<RefreshSessionRequest>,
    ) -> Result<Response<LinkedAccount>, Status> {
        let req = request.into_inner();
        let user = Self::verify_session(&self.credentials, &req.profile_id).await?;

        Ok(Response::new(LinkedAccount {
            storefront: Storefront::Gog as i32,
            account_display_name: user.username,
            account_id: user.user_id,
            auth_state: AuthState::Active as i32,
            linked_at: None,
            last_verified_at: None,
            expires_at: None,
        }))
    }

    /// No-op: GOG's API doesn't expose a way to invalidate a session
    /// server-side, and `AccountsService.UnlinkAccount` drops the stored
    /// credentials on its own side regardless of this call's outcome.
    async fn revoke_session(
        &self,
        _request: Request<RevokeSessionRequest>,
    ) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    /// Returns this profile's GOG library as of right now — the
    /// wire-level counterpart of `fetch_games`.
    async fn list_games(
        &self,
        request: Request<ListGamesRequest>,
    ) -> Result<Response<ListGamesResponse>, Status> {
        let req = request.into_inner();
        let games = Self::fetch_games(&self.credentials, &req.profile_id).await?;
        Ok(Response::new(ListGamesResponse { games }))
    }

    type WatchGamesStream = Pin<Box<dyn Stream<Item = Result<GameEvent, Status>> + Send + 'static>>;

    /// Polls `fetch_games` on an interval and diffs the result against
    /// the previous poll, emitting `Added`/`Removed` events. There's no
    /// push API on GOG's side to do better than this. A poll that fails
    /// (e.g. a session gone stale) is skipped rather than ending the
    /// stream — it picks back up once the session's refreshed.
    async fn watch_games(
        &self,
        request: Request<WatchGamesRequest>,
    ) -> Result<Response<Self::WatchGamesStream>, Status> {
        let profile_id = request.into_inner().profile_id;
        let credentials = self.credentials.clone();
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let mut known: HashMap<String, Game> = HashMap::new();
            let mut interval = tokio::time::interval(WATCH_GAMES_POLL_INTERVAL);

            loop {
                interval.tick().await;

                let ids = match Self::fetch_game_ids(&credentials, &profile_id).await {
                    Ok(ids) => ids,
                    Err(err) => {
                        tracing::debug!("WatchGames poll failed for {profile_id}: {err}");
                        continue;
                    }
                };

                // Resolved and reported already in a previous poll —
                // re-fetching its GamesDB data every tick would be pure
                // waste, so only ids new since last poll get resolved
                // (and streamed out) here.
                let mut seen: HashSet<String> = HashSet::with_capacity(ids.len());
                for id in ids {
                    let id_str = id.to_string();
                    seen.insert(id_str.clone());
                    if known.contains_key(&id_str) {
                        continue;
                    }

                    let game = match tokio::task::spawn_blocking(move || {
                        let http = reqwest::blocking::Client::new();
                        resolve_game(&http, id)
                    })
                    .await
                    {
                        Ok(game) => game,
                        Err(err) => {
                            tracing::warn!("GOG worker task panicked: {err}");
                            continue;
                        }
                    };

                    known.insert(id_str, game.clone());
                    let event = GameEvent {
                        event: Some(game_event::Event::Added(GameAdded { game: Some(game) })),
                    };
                    if tx.send(Ok(event)).await.is_err() {
                        return;
                    }
                }

                let removed: Vec<String> = known
                    .keys()
                    .filter(|id| !seen.contains(id.as_str()))
                    .cloned()
                    .collect();
                for game_id in removed {
                    known.remove(&game_id);
                    let event = GameEvent {
                        event: Some(game_event::Event::Removed(GameRemoved {
                            storefront: Storefront::Gog as i32,
                            game_id,
                        })),
                    };
                    if tx.send(Ok(event)).await.is_err() {
                        return;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    /// Resolves `game_id`'s current Windows build via GOG's builds/depot
    /// v2 API (`crate::depot`) — the same chunked-file-manifest system
    /// `heroic-gogdl` uses, not the legacy `embed.gog.com` installer
    /// this used to hit. No installer ever runs: every file in the
    /// response is downloaded and written directly at its relative
    /// path. `primary_executable` is left unset — GOG doesn't expose it
    /// in the manifest, only inside a `goggame-<id>.info` file that
    /// ships as one of the depot's own files (see `store.proto`'s doc
    /// comment on `InstallManifest.primary_executable`).
    async fn get_install_manifest(
        &self,
        request: Request<GetInstallManifestRequest>,
    ) -> Result<Response<InstallManifest>, Status> {
        let req = request.into_inner();
        let game_id: i64 = req
            .game_id
            .parse()
            .map_err(|_| Status::invalid_argument("game_id must be a GOG numeric product id"))?;

        let stored = self
            .credentials
            .load(&req.profile_id, Storefront::Gog)
            .await
            .map_err(error::credentials)?
            .ok_or_else(|| error::credentials(CredentialError::NotFound))?;
        let token: Token = serde_json::from_str(&stored).map_err(error::json)?;

        // The depot API needs a fresh access token; `gog`'s own client
        // transparently refreshes on any authenticated call, so make a
        // cheap one and persist the (possibly refreshed) token, same as
        // `verify_session` does.
        let refreshed = tokio::task::spawn_blocking(move || {
            let gog = Gog::new(token);
            gog.get_user_data().map_err(error::session_invalid)?;
            Ok::<_, Status>(gog.token.borrow().clone())
        })
        .await
        .map_err(|err| Status::internal(format!("GOG worker task panicked: {err}")))??;
        self.credentials
            .save(
                &req.profile_id,
                Storefront::Gog,
                &serde_json::to_string(&refreshed).map_err(error::json)?,
            )
            .await
            .map_err(error::credentials)?;

        let http = reqwest::Client::new();
        let access_token = &refreshed.access_token;

        let builds = depot::get_builds(&http, access_token, game_id)
            .await
            .map_err(|err| Status::unavailable(format!("GOG builds lookup failed: {err}")))?;
        let build = depot::select_build(&builds).ok_or_else(|| {
            Status::not_found(format!("no builds available for GOG game {game_id}"))
        })?;

        let meta = depot::get_build_meta(&http, access_token, &build.link)
            .await
            .map_err(|err| {
                Status::unavailable(format!("GOG build manifest fetch failed: {err}"))
            })?;
        let depot_meta = depot::select_depot(&meta).ok_or_else(|| {
            Status::failed_precondition(format!("no compatible depot for GOG game {game_id}"))
        })?;

        let depot_files = depot::get_depot_files(&http, &depot_meta.manifest)
            .await
            .map_err(|err| {
                Status::unavailable(format!("GOG depot manifest fetch failed: {err}"))
            })?;
        let secure_link = depot::get_secure_link(&http, access_token, game_id, "/")
            .await
            .map_err(|err| Status::unavailable(format!("GOG secure_link lookup failed: {err}")))?;

        let mut files = Vec::with_capacity(depot_files.len());
        let mut install_size_bytes = Some(0u64);
        for depot_file in depot_files {
            let mut chunks = Vec::with_capacity(depot_file.chunks.len());
            let mut file_size = Some(0u64);
            for chunk in depot_file.chunks {
                let Some(download_url) = secure_link
                    .iter()
                    .find_map(|entry| depot::build_chunk_url(entry, &chunk.compressed_md5))
                else {
                    tracing::warn!(
                        "no usable CDN endpoint for a chunk of {}, skipping",
                        depot_file.path
                    );
                    continue;
                };
                file_size = file_size.zip(Some(chunk.size)).map(|(a, b)| a + b);
                chunks.push(Chunk {
                    download_url,
                    compressed: true,
                    size_bytes: Some(chunk.size),
                    md5: Some(chunk.md5),
                });
            }
            install_size_bytes = install_size_bytes.zip(file_size).map(|(a, b)| a + b);
            files.push(InstallFile {
                relative_path: depot_file.path,
                size_bytes: file_size,
                chunks,
            });
        }

        Ok(Response::new(InstallManifest {
            version: build.build_id.clone(),
            install_size_bytes,
            files,
            install_directory: meta.install_directory,
            primary_executable: None,
            prerequisite: None,
        }))
    }
}

/// URL-encodes `GOG_REDIRECT_URI` for embedding in the auth URL's query
/// string. Small enough to hand-roll rather than pull in a URL-encoding
/// dependency for one constant.
fn urlencoding_embed_redirect() -> String {
    GOG_REDIRECT_URI
        .chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            _ => format!("%{:02X}", c as u32),
        })
        .collect()
}
