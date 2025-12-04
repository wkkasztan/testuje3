use alternator_driver as dynamodb;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::client::conn::http1::SendRequest;
use hyper::{Request, Response};

use std::sync::Arc;
use tokio::sync::Mutex;

mod common;

#[tokio::test]
async fn empty_test() {
    let config = dynamodb::Config::builder()
        .endpoint_url("http://localhost:7999")
        .credentials_provider(dynamodb::config::Credentials::for_tests_with_session_token())
        .region(dynamodb::config::Region::new("eu-central-1"))
        .behavior_version(dynamodb::config::BehaviorVersion::latest())
        .build();

    let client = dynamodb::Client::from_conf(config);

    let proxy = common::start_server("localhost:7999", "localhost:8000").await;

    tokio::try_join!(
        tokio::spawn(async {
            common::run_server(proxy.0, proxy.1, proxy.2, on_request).await;
        }),
        tokio::spawn(async {
            common::make_calls(client).await;
        })
    )
    .unwrap();
}

async fn on_request(
    dynamodb_request: Request<Incoming>,
    alternator_sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
) -> Response<Full<Bytes>> {
    let (parts, body) = common::acquire_dynamodb_request(dynamodb_request).await;

    // ...

    let (parts, body) = common::acquire_alternator_response(alternator_sender, parts, body).await;

    // ...

    common::acquire_dynamodb_response(parts, body).await
}
