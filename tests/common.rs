use alternator_driver as dynamodb;
use dynamodb::client::Waiters;
use dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};

use hyper::body::{Bytes, Incoming};
use hyper::client::conn::http1 as hyper_client;
use hyper::client::conn::http1::SendRequest;
use hyper::server::conn::http1 as hyper_server;
use hyper::service::service_fn;
use hyper::{Request, Response};

use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;

use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

pub async fn acquire_dynamodb_request(
    dynamodb_request: Request<Incoming>,
) -> (http::request::Parts, Bytes) {
    let (parts, body) = dynamodb_request.into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    (parts, body)
}

pub async fn acquire_alternator_response(
    alternator_sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
    parts: http::request::Parts,
    body: Bytes,
) -> (http::response::Parts, Bytes) {
    let body = Full::new(body);
    let request = Request::from_parts(parts, body);

    let mut alternator_sender = alternator_sender.lock().await;
    let alternator_response = alternator_sender.send_request(request).await.unwrap();

    let (parts, body) = alternator_response.into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    (parts, body)
}

pub async fn acquire_dynamodb_response(
    parts: http::response::Parts,
    body: Bytes,
) -> Response<Full<Bytes>> {
    let body = Full::new(body);
    Response::from_parts(parts, body)
}

pub async fn start_server(
    listen_address: &str,
    alternator_address: &str,
) -> (
    SendRequest<Full<Bytes>>,
    hyper_client::Connection<TokioIo<TcpStream>, Full<Bytes>>,
    TcpListener,
) {
    // connect to alternator
    let stream = TcpStream::connect(alternator_address).await.unwrap();
    let stream = TokioIo::new(stream);
    let client = hyper_client::Builder::new();
    let (sender, connection) = client.handshake::<_, Full<Bytes>>(stream).await.unwrap();

    // bind listen address for dynamodb
    let listener = TcpListener::bind(listen_address).await.unwrap();

    (sender, connection, listener)
}

pub async fn run_server<F, Fut>(
    alternator_sender: SendRequest<Full<Bytes>>,
    alternator_connection: hyper_client::Connection<TokioIo<TcpStream>, Full<Bytes>>,
    dynamodb_listener: TcpListener,
    on_request: F,
) where
    F: Fn(Request<Incoming>, Arc<Mutex<SendRequest<Full<Bytes>>>>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response<Full<Bytes>>> + Send,
{
    // listen for dynamodb
    let stream = dynamodb_listener.accept().await.unwrap().0;
    let stream = TokioIo::new(stream);

    // construct service
    let alternator_sender = Arc::new(Mutex::new(alternator_sender));
    let on_request = Arc::new(on_request);

    let service = service_fn(move |request| {
        let sender = alternator_sender.clone();
        let on_request = on_request.clone();
        async move {
            let response = on_request(request, sender).await;
            Ok::<_, hyper::Error>(response)
        }
    });

    let server = hyper_server::Builder::new();
    let dynamodb_connection = server.serve_connection(stream, service);

    // run
    tokio::try_join!(
        tokio::spawn(async move {
            alternator_connection.await.unwrap();
        }),
        tokio::spawn(async move {
            dynamodb_connection.await.unwrap();
        })
    )
    .unwrap();
}

pub async fn make_calls(client: dynamodb::Client) {
    // create table
    client
        .create_table()
        .table_name("ExampleTable")
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("ExampleAttribute")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("ExampleAttribute")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    client
        .wait_until_table_exists()
        .table_name("ExampleTable")
        .wait(std::time::Duration::new(1, 0))
        .await
        .unwrap();

    let tables = client.list_tables().send().await.unwrap();
    assert!(
        tables
            .table_names
            .unwrap()
            .contains(&"ExampleTable".to_string())
    );

    // delete table
    client
        .delete_table()
        .table_name("ExampleTable")
        .send()
        .await
        .unwrap();

    client
        .wait_until_table_not_exists()
        .table_name("ExampleTable")
        .wait(std::time::Duration::new(1, 0))
        .await
        .unwrap();

    let tables = client.list_tables().send().await.unwrap();
    let table_names = tables.table_names.unwrap_or_default();
    assert!(!table_names.iter().any(|name| name == "ExampleTable"));
}
