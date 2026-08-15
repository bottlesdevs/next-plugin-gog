//! `tonic::Status` constructors for every way the GOG plugin can fail.
//!
//! Mirrors `next-plugin-egs`'s `error` module: plain functions instead of
//! an error enum, so each failure carries a gRPC status code that
//! reflects what actually went wrong instead of everything collapsing to
//! `INTERNAL`.

use bottles_core::error::CredentialError;
use tonic::Status;

/// The `challenge_id` passed to `CompleteLogin` doesn't match any
/// challenge this plugin currently has pending. Either it was never
/// issued by this process, or it was already consumed by a prior
/// (successful) `CompleteLogin` call.
pub fn login_challenge_not_found() -> Status {
    Status::not_found("GOG login challenge not found or already completed")
}

/// The challenge existed but was issued more than 5 minutes ago. The
/// caller needs to start over with a fresh `BeginLogin`.
pub fn login_challenge_expired() -> Status {
    Status::deadline_exceeded("GOG login challenge expired")
}

/// `CompleteLogin` was called with an empty `user_input` — GOG's flow
/// needs the `code` query parameter captured from the redirect back to
/// `redirect_uri`.
pub fn authorization_code_required() -> Status {
    Status::invalid_argument("GOG authorization code is required")
}

/// GOG rejected the authorization code (expired, already used, or
/// malformed). The challenge is left in place so the caller can retry
/// with a corrected code rather than having to restart the whole login
/// flow.
pub fn authorization_failed(err: impl std::fmt::Display) -> Status {
    Status::unauthenticated(format!("GOG authorization failed: {err}"))
}

/// A previously stored session no longer authenticates against GOG
/// (refresh token revoked or expired).
pub fn session_invalid(err: impl std::fmt::Display) -> Status {
    Status::unauthenticated(format!("GOG session is no longer valid: {err}"))
}

/// The `CredentialStore` failed to load or save this profile's GOG
/// credentials.
pub fn credentials(err: CredentialError) -> Status {
    Status::internal(format!("GOG credentials error: {err}"))
}

/// Stored credentials couldn't be serialized to or deserialized from
/// JSON.
pub fn json(err: serde_json::Error) -> Status {
    Status::internal(format!("GOG JSON error: {err}"))
}

/// A GOG API call failed for a reason unrelated to authentication (rate
/// limiting, a transient network error, an unexpected response shape).
pub fn api(err: impl std::fmt::Display) -> Status {
    Status::unavailable(format!("GOG API error: {err}"))
}
