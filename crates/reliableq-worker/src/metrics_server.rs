//! Standalone `/metrics` HTTP server for the worker process (spec sec.
//! 13.2). The worker is a separate process from the API, so it needs
//! its own Prometheus endpoint for the metrics only it can observe
//! (in-flight count, lease renewals, downstream call outcomes).

use std::net::SocketAddr;
use std::sync::OnceLock;

use axum::Router;
use axum::routing::get;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// A `metrics` global recorder can only be installed once per process;
/// this makes repeated calls (production startup, or many tests in one
/// binary) idempotent and share the same handle.
pub fn recorder_handle() -> PrometheusHandle {
    HANDLE
        .get_or_init(|| {
            PrometheusBuilder::new()
                .install_recorder()
                .expect("failed to install prometheus recorder")
        })
        .clone()
}

pub async fn serve(bind_addr: SocketAddr, handle: PrometheusHandle) -> std::io::Result<()> {
    let app = Router::new().route(
        "/metrics",
        get(move || {
            let handle = handle.clone();
            async move {
                (
                    [("content-type", "text/plain; version=0.0.4")],
                    handle.render(),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app).await
}
