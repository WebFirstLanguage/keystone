use axum::body::Bytes;
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use keystone::datastore::Datastore;
use keystone::error::Error as StoreError;
use keystone::Bucket;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{error, info};
use base64::Engine;

#[derive(Clone)]
struct AppState {
    datastore: Arc<Datastore>,
    jwt_secret: Arc<String>,
    issuer: String,
    audience: Option<String>,
    users: Arc<HashMap<String, String>>, // username -> password (basic demo auth source)
}

#[derive(Serialize, Deserialize)]
struct Claims {
    sub: String,
    exp: usize,
    iss: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    aud: Option<String>,
}

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct LoginResponse {
    token: String,
    token_type: &'static str,
    expires_in: usize,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            ApiError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::Conflict(_) => StatusCode::CONFLICT,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let msg = self.to_string();
        (status, Json(ErrorBody { error: msg })).into_response()
    }
}

#[derive(thiserror::Error, Debug)]
enum ApiError {
    #[error("unauthorized: {0}")]
    Unauthorized(&'static str),
    #[error("bad request: {0}")]
    BadRequest(&'static str),
    #[error("not found: {0}")]
    NotFound(&'static str),
    #[error("conflict: {0}")]
    Conflict(&'static str),
    #[error("internal error: {0}")]
    Internal(&'static str),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let bind_addr: SocketAddr = std::env::var("BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
        .parse()
        .expect("invalid BIND_ADDR");

    let datastore_path = std::env::var("DATASTORE_PATH").unwrap_or_else(|_| "./data/keystone.redb".to_string());
    std::fs::create_dir_all(
        std::path::Path::new(&datastore_path)
            .parent()
            .unwrap_or(std::path::Path::new(".")),
    )?;

    let datastore = Arc::new(Datastore::new(datastore_path).map_err(|e| anyhow::anyhow!("datastore init: {e}"))?);

    let jwt_secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| {
        let s = uuid_like_secret();
        eprintln!("WARN: JWT_SECRET not set; using ephemeral: {s}. Tokens will be invalid after restart.");
        s
    });

    let issuer = std::env::var("JWT_ISSUER").unwrap_or_else(|_| "keystone".to_string());
    let audience = std::env::var("JWT_AUDIENCE").ok();

    let username = std::env::var("AUTH_USERNAME").unwrap_or_else(|_| "admin".to_string());
    let password = std::env::var("AUTH_PASSWORD").unwrap_or_else(|_| "admin".to_string());
    let mut users = HashMap::new();
    users.insert(username, password);

    let state = AppState {
        datastore,
        jwt_secret: Arc::new(jwt_secret),
        issuer,
        audience,
        users: Arc::new(users),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/auth/login", post(login))
        // Protected API
        .route("/buckets", post(create_bucket))
        .route("/buckets/:name", delete(delete_bucket))
        .route("/buckets/:name/objects", get(list_objects))
        .route(
            "/buckets/:name/objects/:key",
            put(put_object).get(get_object).delete(delete_object),
        )
        .with_state(state.clone())
        // Apply auth middleware to everything except health and login
        .layer(axum::middleware::from_fn_with_state(state.clone(), auth_middleware))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .layer(TraceLayer::new_for_http());

    info!("listening on {}", bind_addr);
    axum::serve(tokio::net::TcpListener::bind(bind_addr).await?, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received");
}

fn uuid_like_secret() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
}

async fn login(State(state): State<AppState>, Json(body): Json<LoginRequest>) -> Result<impl IntoResponse, ApiError> {
    match state.users.get(&body.username) {
        Some(expected) if expected == &body.password => {
            let expiration = (Utc::now() + Duration::hours(12)).timestamp() as usize;
            let claims = Claims {
                sub: body.username,
                exp: expiration,
                iss: state.issuer.clone(),
                aud: state.audience.clone(),
            };
            let token = encode(
                &Header::new(Algorithm::HS256),
                &claims,
                &EncodingKey::from_secret(state.jwt_secret.as_bytes()),
            )
            .map_err(|_| ApiError::Internal("token encode failed"))?;
            Ok((
                StatusCode::OK,
                Json(LoginResponse { token, token_type: "Bearer", expires_in: expiration }),
            ))
        }
        _ => Err(ApiError::Unauthorized("invalid credentials")),
    }
}

async fn auth_middleware(State(state): State<AppState>, req: Request, next: Next) -> Result<Response, ApiError> {
    // Allow public routes
    let path = req.uri().path();
    if path == "/health" || path == "/auth/login" {
        return Ok(next.run(req).await);
    }

    // Expect Authorization: Bearer <token>
    let token = match req.headers().get(header::AUTHORIZATION).and_then(|h| h.to_str().ok()) {
        Some(hv) if hv.to_ascii_lowercase().starts_with("bearer ") => Some(hv[7..].trim()),
        _ => None,
    }
    .ok_or(ApiError::Unauthorized("missing bearer token"))?;

    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_aud = state.audience.is_some();
    if let Some(aud) = &state.audience {
        validation.set_audience(&[aud.as_str()]);
    }
    validation.set_issuer(&[state.issuer.as_str()]);

    decode::<Claims>(token, &DecodingKey::from_secret(state.jwt_secret.as_bytes()), &validation)
        .map_err(|_| ApiError::Unauthorized("invalid or expired token"))?;

    Ok(next.run(req).await)
}

#[derive(Deserialize)]
struct CreateBucketRequest {
    name: String,
}

async fn create_bucket(State(state): State<AppState>, Json(body): Json<CreateBucketRequest>) -> Result<impl IntoResponse, ApiError> {
    state
        .datastore
        .create_bucket(&body.name)
        .map_err(map_store_err)?;
    Ok(StatusCode::CREATED)
}

async fn delete_bucket(State(state): State<AppState>, Path(name): Path<String>) -> Result<impl IntoResponse, ApiError> {
    state.datastore.delete_bucket(&name).map_err(map_store_err)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct ListQuery {
    #[serde(default)]
    prefix: String,
}

#[derive(Serialize)]
struct ListResponse {
    keys: Vec<String>,
}

async fn list_objects(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(q): Query<ListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let bucket = state.datastore.bucket(&name).map_err(map_store_err)?;
    let keys = bucket.list(&q.prefix).map_err(map_store_err)?;
    Ok(Json(ListResponse { keys }))
}

async fn put_object(
    State(state): State<AppState>,
    Path((name, key)): Path<(String, String)>,
    bytes: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let bucket: Bucket<_> = state.datastore.bucket(&name).map_err(map_store_err)?;
    bucket.put(&key, &bytes.to_vec()).map_err(map_store_err)?;
    Ok(StatusCode::CREATED)
}

async fn get_object(
    State(state): State<AppState>,
    Path((name, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let bucket: Bucket<_> = state.datastore.bucket(&name).map_err(map_store_err)?;
    let data: Vec<u8> = bucket.get(&key).map_err(map_store_err)?;
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/octet-stream"));
    Ok((StatusCode::OK, headers, data))
}

async fn delete_object(
    State(state): State<AppState>,
    Path((name, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let bucket: Bucket<_> = state.datastore.bucket(&name).map_err(map_store_err)?;
    bucket.delete(&key).map_err(map_store_err)?;
    Ok(StatusCode::NO_CONTENT)
}

fn map_store_err(e: StoreError) -> ApiError {
    use keystone::error::Error as E;
    match e {
        E::BucketExists => ApiError::Conflict("bucket exists"),
        E::BucketNotFound => ApiError::NotFound("bucket not found"),
        E::ObjectNotFound => ApiError::NotFound("object not found"),
        E::InvalidBucketName(_) | E::InvalidObjectKey(_) => ApiError::BadRequest("invalid name or key"),
        _ => {
            error!("internal store error: {:?}", e);
            ApiError::Internal("internal error")
        }
    }
}
