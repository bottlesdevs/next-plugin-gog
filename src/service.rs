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
    store::v1::{
        BeginLoginRequest, CompleteLoginRequest, ListGamesRequest, ListGamesResponse,
        LoginChallenge, OAuthRedirectChallenge, RefreshSessionRequest, RevokeSessionRequest,
        WatchGamesRequest, login_challenge::Kind, store_server::Store,
    },
};
use tokio::sync::{RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, async_trait};
use uuid::Uuid;

use crate::{
    error,
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

/// GOG storefront plugin, serving `bottles.store.v1.Store` over gRPC.
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

        let token: Token = serde_json::from_slice(&stored).map_err(error::json)?;

        let (user, refreshed_token) = tokio::task::spawn_blocking(move || {
            let gog = Gog::new(token);
            let user = gog.get_user_data();
            let final_token = gog.token.borrow().clone();
            (user, final_token)
        })
        .await
        .map_err(|err| Status::internal(format!("GOG worker task panicked: {err}")))?;

        let user = user.map_err(error::session_invalid)?;

        let refreshed = serde_json::to_vec(&refreshed_token).map_err(error::json)?;
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
        let stored = credentials
            .load(profile_id, Storefront::Gog)
            .await
            .map_err(error::credentials)?
            .ok_or_else(|| error::credentials(CredentialError::NotFound))?;

        let token: Token = serde_json::from_slice(&stored).map_err(error::json)?;

        tokio::task::spawn_blocking(move || {
            let gog = Gog::new(token);
            let ids = gog.get_games().map_err(error::api)?;
            let http = reqwest::blocking::Client::new();

            Ok(ids
                .into_iter()
                .map(|id| match gamesdb::fetch_release(&http, id) {
                    Ok(release) => Game {
                        id: id.to_string(),
                        title: release
                            .title
                            .resolve()
                            .unwrap_or_else(|| id.to_string()),
                        storefront: Storefront::Gog as i32,
                        description: release.summary.as_ref().and_then(Localized::resolve),
                        icon_url: release.icon.as_ref().map(Image::resolve),
                        cover_url: release
                            .game
                            .vertical_cover
                            .as_ref()
                            .or(release.game.cover.as_ref())
                            .map(Image::resolve),
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
                        }
                    }
                })
                .collect())
        })
        .await
        .map_err(|err| Status::internal(format!("GOG worker task panicked: {err}")))?
    }
}

#[async_trait]
impl<C: CredentialStore + Send + Sync + 'static> Store for GogStoreService<C> {
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

        let credentials = serde_json::to_vec(&token).map_err(error::json)?;
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
    /// server-side, and `ProfileService.UnlinkAccount` drops the stored
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

                let games = match Self::fetch_games(&credentials, &profile_id).await {
                    Ok(games) => games,
                    Err(err) => {
                        tracing::debug!("WatchGames poll failed for {profile_id}: {err}");
                        continue;
                    }
                };

                let seen: HashSet<&str> = games.iter().map(|game| game.id.as_str()).collect();

                for game in &games {
                    if !known.contains_key(&game.id) {
                        known.insert(game.id.clone(), game.clone());
                        let event = GameEvent {
                            event: Some(game_event::Event::Added(GameAdded {
                                game: Some(game.clone()),
                            })),
                        };
                        if tx.send(Ok(event)).await.is_err() {
                            return;
                        }
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
