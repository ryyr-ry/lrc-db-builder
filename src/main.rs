use brotli::enc::BrotliEncoderParams;
use futures::stream::{self, StreamExt};
use reqwest::{header, Client};
use rusqlite::Connection;
use serde::Deserialize;
use std::env;
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::path::Path;
use tokio::sync::mpsc;

const ORG_NAME: &str = "LRCHub";
const DB_NAME: &str = "lyrics.db";
const COMPILED_DB_NAME: &str = "lyrics.db.br";
// Abusive fetching will be mitigated since this runs locally or via Actions (internal network)
const CONCURRENCY_LIMIT: usize = 200;

#[derive(Deserialize)]
struct RepoItem {
    name: String,
}

#[derive(Deserialize)]
struct Manifest {
    candidates: Option<Vec<Candidate>>,
}

#[derive(Deserialize)]
struct Candidate {
    has_synced: Option<bool>,
    path: Option<String>,
}

struct DbRecord {
    video_id: String,
    is_synced: bool,
    lrc: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let github_token = env::var("GITHUB_TOKEN").unwrap_or_default();

    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::ACCEPT,
        header::HeaderValue::from_static("application/vnd.github.v3+json"),
    );
    headers.insert(
        header::USER_AGENT,
        header::HeaderValue::from_static("lrc-db-builder/1.0"),
    );
    if !github_token.is_empty() {
        if let Ok(val) = header::HeaderValue::from_str(&format!("token {}", github_token)) {
            headers.insert(header::AUTHORIZATION, val);
        }
    }

    let client = Client::builder()
        .user_agent("lrc-db-builder/1.0")
        .default_headers(headers)
        .build()?;

    log::info!("Fetching repository list using GraphQL from {}...", ORG_NAME);
    let mut repos = Vec::new();
    let mut has_next_page = true;
    let mut end_cursor: Option<String> = None;

    while has_next_page {
        let after_clause = match &end_cursor {
            Some(cursor) => format!(r#", after: "{}""#, cursor),
            None => "".to_string(),
        };

        let query = format!(
            r#"{{
                "query": "query {{ user(login: \"{}\") {{ repositories(first: 100{}) {{ pageInfo {{ hasNextPage endCursor }} nodes {{ name }} }} }} }}"
            }}"#,
            ORG_NAME, after_clause
        );

        let resp = client
            .post("https://api.github.com/graphql")
            .body(query)
            .send()
            .await;

        match resp {
            Ok(res) if res.status().is_success() => {
                if let Ok(data) = res.json::<serde_json::Value>().await {
                    let repos_node = &data["data"]["user"]["repositories"];
                    
                    if let Some(nodes) = repos_node["nodes"].as_array() {
                        for node in nodes {
                            if let Some(name) = node["name"].as_str() {
                                if !name.starts_with('.') {
                                    repos.push(name.to_string());
                                }
                            }
                        }
                    }

                    has_next_page = repos_node["pageInfo"]["hasNextPage"].as_bool().unwrap_or(false);
                    if has_next_page {
                        end_cursor = repos_node["pageInfo"]["endCursor"].as_str().map(|s| s.to_string());
                    }
                } else {
                    break;
                }
            }
            Ok(res) => {
                let status = res.status();
                let txt = res.text().await.unwrap_or_default();
                log::error!("GraphQL failed: HTTP {} - {}", status, txt);
                if status == 403 || status == 429 {
                    log::warn!("Rate limited. Waiting 10 seconds...");
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                    continue; // リトライ
                }
                break;
            }
            Err(e) => {
                log::error!("Error fetching from GraphQL: {}", e);
                break;
            }
        }
    }

    log::info!("Discovered {} video repositories.", repos.len());

    if Path::new(DB_NAME).exists() {
        std::fs::remove_file(DB_NAME)?;
    }

    // Producer / Consumer channel for DB insertions
    let (tx, mut rx) = mpsc::channel::<DbRecord>(5000);

    let db_thread = tokio::task::spawn_blocking(move || {
        let mut conn = Connection::open(DB_NAME).expect("Failed to open DB");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS lyrics (
                video_id TEXT PRIMARY KEY,
                is_synced BOOLEAN NOT NULL,
                lrc TEXT NOT NULL
            )",
            [],
        )
        .expect("Failed to create table");

        let mut temp_buffer = Vec::new();
        let mut count = 0;

        while let Some(record) = rx.blocking_recv() {
            temp_buffer.push(record);
            count += 1;

            if temp_buffer.len() >= 1000 {
                let tx_db = conn.transaction().unwrap();
                {
                    let mut stmt = tx_db
                        .prepare("INSERT OR REPLACE INTO lyrics (video_id, is_synced, lrc) VALUES (?1, ?2, ?3)")
                        .unwrap();
                    for r in &temp_buffer {
                        stmt.execute(rusqlite::params![r.video_id, r.is_synced, r.lrc])
                            .unwrap();
                    }
                }
                tx_db.commit().unwrap();
                temp_buffer.clear();
                log::info!("Database sync: {} inserted.", count);
            }
        }

        if !temp_buffer.is_empty() {
            let tx_db = conn.transaction().unwrap();
            {
                let mut stmt = tx_db
                    .prepare("INSERT OR REPLACE INTO lyrics (video_id, is_synced, lrc) VALUES (?1, ?2, ?3)")
                    .unwrap();
                for r in &temp_buffer {
                    stmt.execute(rusqlite::params![r.video_id, r.is_synced, r.lrc])
                        .unwrap();
                }
            }
            tx_db.commit().unwrap();
        }

        log::info!("Optimizing database with VACUUM...");
        conn.execute("VACUUM", []).unwrap();
        log::info!("Database writer finished. Total compiled records: {}", count);
    });

    log::info!(
        "Starting ultra-parallel fetch ({} concurrency) for {} videos...",
        CONCURRENCY_LIMIT,
        repos.len()
    );

    let client_ref = &client;
    let tx_ref = &tx;

    stream::iter(repos.into_iter())
        .map(|video_id| async move {
            let manifest_url = format!(
                "https://raw.githubusercontent.com/{}/{}/main/select/index.json",
                ORG_NAME, video_id
            );

            if let Ok(res) = client_ref.get(&manifest_url).send().await {
                if res.status().is_success() {
                    if let Ok(manifest_text) = res.text().await {
                        if let Ok(manifest) = serde_json::from_str::<Manifest>(&manifest_text) {
                            if let Some(candidates) = manifest.candidates {
                                if !candidates.is_empty() {
                                    let mut best = &candidates[0];
                                    for c in &candidates {
                                        if c.has_synced.unwrap_or(false) {
                                            best = c;
                                            break;
                                        }
                                    }

                                    if let Some(path) = &best.path {
                                        let encoded_path = path
                                            .split('/')
                                            .map(|p| urlencoding::encode(p).into_owned())
                                            .collect::<Vec<_>>()
                                            .join("/");

                                        let lrc_url = format!(
                                            "https://raw.githubusercontent.com/{}/{}/main/{}",
                                            ORG_NAME, video_id, encoded_path
                                        );

                                        if let Ok(lrc_res) = client_ref.get(&lrc_url).send().await {
                                            if lrc_res.status().is_success() {
                                                if let Ok(lrc_content) = lrc_res.text().await {
                                                    let is_synced =
                                                        best.has_synced.unwrap_or(false);
                                                    let _ = tx_ref
                                                        .send(DbRecord {
                                                            video_id,
                                                            is_synced,
                                                            lrc: lrc_content,
                                                        })
                                                        .await;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
        .buffer_unordered(CONCURRENCY_LIMIT)
        .collect::<Vec<()>>()
        .await;

    drop(tx);
    let _ = db_thread.await;

    log::info!("Compressing database with Brotli (Quality 11)...");
    let mut file = File::open(DB_NAME)?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;

    let mut compressed_data = Vec::new();
    let mut params = BrotliEncoderParams::default();
    params.quality = 11;

    let mut reader = Cursor::new(data.clone());
    brotli::BrotliCompress(&mut reader, &mut compressed_data, &params)?;

    let mut out_file = File::create(COMPILED_DB_NAME)?;
    out_file.write_all(&compressed_data)?;

    let orig_len = data.len() as f64 / 1_048_576.0;
    let comp_len = compressed_data.len() as f64 / 1_048_576.0;

    log::info!(
        "Compression complete. Original: {:.2} MB -> Compressed: {:.2} MB",
        orig_len, comp_len
    );

    Ok(())
}
