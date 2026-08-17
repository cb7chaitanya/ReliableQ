//! Request ID correlation (spec sec. 13.3), mirroring
//! reliableq-api::correlation: reuse an inbound `X-Request-Id` (the
//! worker sends one derived from the job ID — see
//! reliableq-worker::execute_charge), attach it to this request's logs,
//! echo it back as a response header, and use it as the error
//! envelope's `request_id`.

use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;
use tracing::Instrument;
use uuid::Uuid;

const HEADER_NAME: &str = "x-request-id";

tokio::task_local! {
    static REQUEST_ID: String;
}

pub fn current_request_id() -> String {
    REQUEST_ID
        .try_with(Clone::clone)
        .unwrap_or_else(|_| Uuid::new_v4().to_string())
}

pub async fn middleware(mut request: Request, next: Next) -> Response {
    let request_id = request
        .headers()
        .get(HEADER_NAME)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    if let Ok(value) = HeaderValue::from_str(&request_id) {
        request.headers_mut().insert(HEADER_NAME, value.clone());
    }

    let span = tracing::info_span!("request", request_id = %request_id);
    let request_id_for_response = request_id.clone();

    let mut response = REQUEST_ID
        .scope(request_id, next.run(request).instrument(span))
        .await;

    if let Ok(value) = HeaderValue::from_str(&request_id_for_response) {
        response.headers_mut().insert(HEADER_NAME, value);
    }
    response
}
