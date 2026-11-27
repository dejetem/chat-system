use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};
use storage::{MediaStorage, S3Config, S3MediaStorage, StorageError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Clone)]
struct StoredObject {
    body: Vec<u8>,
    content_type: String,
    etag: String,
}

async fn read_http_request(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
) -> Option<(String, String, HashMap<String, String>, Vec<u8>)> {
    loop {
        if let Some(header_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let header_bytes = &buf[..header_end];
            let header_text = String::from_utf8_lossy(header_bytes).into_owned();
            let mut lines = header_text.split("\r\n");
            let request_line = lines.next()?.to_string();
            let mut parts = request_line.split_whitespace();
            let method = parts.next()?.to_string();
            let path = parts.next()?.to_string();

            let mut headers = HashMap::new();
            for line in lines {
                if let Some((k, v)) = line.split_once(':') {
                    headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
                }
            }

            let content_length = headers
                .get("content-length")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(0);

            let body_start = header_end + 4;
            while buf.len() < body_start + content_length {
                let mut tmp = [0u8; 4096];
                let n = stream.read(&mut tmp).await.ok()?;
                if n == 0 {
                    return None;
                }
                buf.extend_from_slice(&tmp[..n]);
            }

            let body = buf[body_start..body_start + content_length].to_vec();
            buf.drain(..body_start + content_length);
            return Some((method, path, headers, body));
        }

        let mut tmp = [0u8; 4096];
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

async fn write_http_response(
    stream: &mut TcpStream,
    status_line: &str,
    extra_headers: &[(&str, &str)],
    body: &[u8],
    send_body: bool,
    content_length_override: Option<usize>,
) {
    let content_len = content_length_override.unwrap_or(body.len());
    let mut head =
        format!("HTTP/1.1 {status_line}\r\nContent-Length: {content_len}\r\nConnection: close\r\n");
    for (k, v) in extra_headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes()).await;
    if send_body && !body.is_empty() {
        let _ = stream.write_all(body).await;
    }
    let _ = stream.flush().await;
}

async fn spawn_mock_s3() -> (
    String,
    Arc<Mutex<HashMap<String, StoredObject>>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let objects: Arc<Mutex<HashMap<String, StoredObject>>> = Arc::new(Mutex::new(HashMap::new()));
    let store = Arc::clone(&objects);

    let handle = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let Some((method, path, headers, body)) =
                    read_http_request(&mut stream, &mut buf).await
                else {
                    return;
                };

                // Verify AWS SigV4 headers and body digest on every request.
                let auth = headers.get("authorization").cloned().unwrap_or_default();
                let amz_sha = headers
                    .get("x-amz-content-sha256")
                    .cloned()
                    .unwrap_or_default();
                let expected_sha = hex::encode(Sha256::digest(&body));
                if !auth.starts_with("AWS4-HMAC-SHA256 Credential=test-access/")
                    || !auth
                        .contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=")
                    || !headers.contains_key("x-amz-date")
                    || amz_sha != expected_sha
                {
                    write_http_response(
                        &mut stream,
                        "403 Forbidden",
                        &[("Content-Type", "text/plain")],
                        b"invalid sigv4 headers",
                        true,
                        None,
                    )
                    .await;
                    return;
                }

                match method.as_str() {
                    "PUT" => {
                        let content_type = headers
                            .get("content-type")
                            .cloned()
                            .unwrap_or_else(|| "application/octet-stream".to_string());
                        let etag = expected_sha[..16].to_string();
                        store.lock().unwrap().insert(
                            path,
                            StoredObject {
                                body,
                                content_type,
                                etag: etag.clone(),
                            },
                        );
                        let quoted_etag = format!("\"{etag}\"");
                        write_http_response(
                            &mut stream,
                            "200 OK",
                            &[("ETag", &quoted_etag)],
                            b"",
                            true,
                            None,
                        )
                        .await;
                    }
                    "HEAD" => {
                        let maybe_obj = store.lock().unwrap().get(&path).cloned();
                        match maybe_obj {
                            Some(obj) => {
                                let quoted_etag = format!("\"{}\"", obj.etag);
                                write_http_response(
                                    &mut stream,
                                    "200 OK",
                                    &[("Content-Type", &obj.content_type), ("ETag", &quoted_etag)],
                                    b"",
                                    false,
                                    Some(obj.body.len()),
                                )
                                .await;
                            }
                            None => {
                                write_http_response(
                                    &mut stream,
                                    "404 Not Found",
                                    &[],
                                    b"",
                                    false,
                                    Some(0),
                                )
                                .await;
                            }
                        }
                    }
                    "GET" => {
                        let maybe_obj = store.lock().unwrap().get(&path).cloned();
                        match maybe_obj {
                            Some(obj) => {
                                let quoted_etag = format!("\"{}\"", obj.etag);
                                write_http_response(
                                    &mut stream,
                                    "200 OK",
                                    &[("Content-Type", &obj.content_type), ("ETag", &quoted_etag)],
                                    &obj.body,
                                    true,
                                    None,
                                )
                                .await;
                            }
                            None => {
                                write_http_response(
                                    &mut stream,
                                    "404 Not Found",
                                    &[("Content-Type", "text/plain")],
                                    b"NoSuchKey",
                                    true,
                                    None,
                                )
                                .await;
                            }
                        }
                    }
                    "DELETE" => {
                        let removed = store.lock().unwrap().remove(&path).is_some();
                        if removed {
                            write_http_response(
                                &mut stream,
                                "204 No Content",
                                &[],
                                b"",
                                true,
                                None,
                            )
                            .await;
                        } else {
                            write_http_response(
                                &mut stream,
                                "404 Not Found",
                                &[("Content-Type", "text/plain")],
                                b"NoSuchKey",
                                true,
                                None,
                            )
                            .await;
                        }
                    }
                    _ => {
                        write_http_response(
                            &mut stream,
                            "405 Method Not Allowed",
                            &[],
                            b"",
                            true,
                            None,
                        )
                        .await;
                    }
                }
            });
        }
    });

    (format!("http://{addr}"), objects, handle)
}

#[tokio::test]
async fn s3_media_storage_put_head_get_delete_with_sigv4_mock() {
    let (endpoint, objects, server_handle) = spawn_mock_s3().await;

    let s3 = S3MediaStorage::new(S3Config {
        endpoint: Some(endpoint.clone()),
        region: "us-east-1".to_string(),
        bucket: "chat-media".to_string(),
        access_key_id: "test-access".to_string(),
        secret_access_key: "test-secret".to_string(),
        force_path_style: true,
        default_presign_ttl_secs: 900,
    })
    .unwrap();

    let key = "attachments/room-10/encrypted-blob.bin";
    let ciphertext = vec![0xca, 0xfe, 0x00, 0x01, 0x42, 0x99, 0xde, 0xad];

    assert!(s3.head_object(key).await.unwrap().is_none());

    let put_meta = s3
        .put_encrypted_object(key, ciphertext.clone(), Some("application/octet-stream"))
        .await
        .unwrap();
    assert_eq!(put_meta.key, key);
    assert_eq!(put_meta.content_length, ciphertext.len() as u64);
    assert_eq!(put_meta.url, format!("{endpoint}/chat-media/{key}"));
    assert!(
        objects
            .lock()
            .unwrap()
            .contains_key(&format!("/chat-media/{key}"))
    );

    let head_meta = s3
        .head_object(key)
        .await
        .unwrap()
        .expect("expected object metadata");
    assert_eq!(head_meta.content_length, ciphertext.len() as u64);
    assert_eq!(head_meta.etag, put_meta.etag);

    let fetched = s3.get_encrypted_object(key).await.unwrap();
    assert_eq!(fetched.ciphertext, ciphertext);
    assert_eq!(fetched.meta.sha256_hex, put_meta.sha256_hex);

    assert!(s3.delete_object(key).await.unwrap());
    assert!(!s3.delete_object(key).await.unwrap());
    assert!(s3.head_object(key).await.unwrap().is_none());

    let missing_err = s3.get_encrypted_object(key).await.unwrap_err();
    assert!(matches!(missing_err, StorageError::NotFound(_)));

    server_handle.abort();
}
