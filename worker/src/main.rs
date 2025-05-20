use std::path::Path;
use std::time::Duration;
use log::{debug, error, info, warn};
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, RedisError, RedisResult}; // Import RedisResult
use tokio::signal; // For graceful shutdown
use tokio::time::sleep;
use uuid::Uuid;

// Declare our modules
mod options;
mod transcoder;
mod job;

// Use specific items from our modules
use transcoder::transcode_video;
use job::{parse_job_from_map, JOB_CONSUMER_GROUP, TRANSCODING_JOB_STREAM_KEY};

// Configuration Constants
const VALKEY_URL_ENV_VAR: &str = "VALKEY_URL";
const DEFAULT_VALKEY_URL: &str = "redis://127.0.0.1:6379";
const WORKER_LOOP_DELAY_MS: u64 = 1000; // Delay if no job or error, to prevent tight loop
const STREAM_BLOCK_TIMEOUT_MS: usize = 5000; // How long to block on XREADGROUP

// Helper to get hostname or a default
fn get_consumer_hostname() -> String {
    hostname::get()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "unknown-host".to_string())
}

async fn ensure_consumer_group_exists(
    con: &mut MultiplexedConnection,
    stream_key: &str,
    group_name: &str,
) -> RedisResult<()> {
    // XGROUP CREATE <key> <groupname> <id or $> [MKSTREAM] [ENTRIESREAD entries_read]
    // Using "0" to start from the beginning of the stream for a new group.
    // MKSTREAM creates the stream if it doesn't exist.
    let result: Result<(), RedisError> = con
        .xgroup_create_mkstream(stream_key, group_name, "0")
        .await;

    match result {
        Ok(()) => {
            info!("Consumer group '{}' created (or already existed) for stream '{}'", group_name, stream_key);
            Ok(())
        }
        Err(e) => {
            // Check if the error is because the group already exists (BUSYGROUP)
            if e.to_string().contains("BUSYGROUP") {
                info!("Consumer group '{}' already exists for stream '{}'", group_name, stream_key);
                Ok(()) // Treat as success
            } else {
                error!("Failed to create consumer group '{}' for stream '{}': {}", group_name, stream_key, e);
                Err(e)
            }
        }
    }
}

async fn process_jobs_loop(
    mut con: MultiplexedConnection, // Take ownership for the loop
    stream_key: &str,
    group_name: &str,
    consumer_name: &str,
) {
    info!("Worker '{}' starting to process jobs from stream '{}', group '{}'", consumer_name, stream_key, group_name);
    // Options for XREADGROUP:
    // GROUP <group_name> <consumer_name>
    // COUNT 1 (process one message at a time)
    // BLOCK <milliseconds> (wait for messages)
    // STREAMS <stream_key> > ('>' means only new messages for this consumer)
    let read_opts = StreamReadOptions::default()
        .group(group_name, consumer_name)
        .count(1)
        .block(STREAM_BLOCK_TIMEOUT_MS);

    loop {
        
        // XREADGROUP needs two slices: one for keys, one for IDs (">" for new messages)
        let result: RedisResult<Option<StreamReadReply>> = con
            .xread_options(&[stream_key], &[">"], &read_opts)
            .await;

        match result {
            Ok(Some(reply)) => {
                if reply.keys.is_empty() {
                    // This case should ideally not happen if Some(reply) is returned and block is used,
                    // but good to be defensive. It means timeout occurred without new messages.
                    debug!("No new messages for consumer '{}', continuing.", consumer_name);
                    continue; // Loop back to wait for more messages
                }

                for stream_key_entry in reply.keys { // Should be just one stream_key_entry (our transcoding_jobs)
                    for message_entry in stream_key_entry.ids { // Should be just one message due to COUNT 1
                        let job = match parse_job_from_map(message_entry.id.clone(), message_entry.map) {
                            Ok(j) => j,
                            Err(e) => {
                                error!("Failed to parse job (ID: {}): {}. Skipping.", message_entry.id, e);
                                // Decide on error strategy:
                                // - XACK to remove malformed message?
                                // - Or let it be processed by XAUTOCLAIM later by a cleanup process?
                                // For now, we'll log and it will remain in PEL until claimed or manually ACKed.
                                continue;
                            }
                        };
                        
                        info!("[Job {}] Received. Input: '{}', Output: '{}'",
                            job.payload.job_id, job.payload.input_path, job.payload.output_path);
                        debug!("[Job {}] Options: {:?}", job.payload.job_id, job.payload.options);

                        // Clone data needed for the blocking task
                        let job_for_blocking_task = job.clone();

                        // Run the CPU-bound transcoding task in a blocking thread
                        let transcode_task_result = tokio::task::spawn_blocking(move || {
                            info!("[Job {}] Starting transcoding...", job_for_blocking_task.payload.job_id);
                            transcode_video(
                                Path::new(&job_for_blocking_task.payload.input_path),
                                Path::new(&job_for_blocking_task.payload.output_path),
                                &job_for_blocking_task.payload.options,
                            )
                        }).await; // .await here is for the JoinHandle

                        match transcode_task_result {
                            Ok(Ok(())) => { // spawn_blocking finished, transcode_video returned Ok
                                info!("[Job {}] Transcoding successful.", job.payload.job_id);
                                match con.xack::<_, _, _, i64>(stream_key, group_name, &[&job.message_id]).await {
                                    Ok(acked_count) => {
                                        if acked_count > 0 {
                                            info!("[Job {}] Successfully ACKed message_id: {} (Count: {})", job.payload.job_id, job.message_id, acked_count);
                                        } else {
                                            // This case should ideally not happen if the message ID was valid and pending for this consumer.
                                            warn!("[Job {}] ACK command for message_id {} returned 0 (message might not have been in PEL or already ACKed).", job.payload.job_id, job.message_id);
                                        }
                                    }
                                    Err(e) => error!("[Job {}] Failed to ACK message_id {}: {}", job.payload.job_id, job.message_id, e),
                                }
                            }
                            Ok(Err(io_err)) => { // spawn_blocking finished, transcode_video returned Err
                                error!("[Job {}] Transcoding failed: {}", job.payload.job_id, io_err);
                                // Not ACKing, so it remains in Pending Entries List (PEL)
                                // Another consumer or a retry mechanism might pick it up or XAUTOCLAIM.
                            }
                            Err(join_err) => { // spawn_blocking task panicked or was cancelled
                                error!("[Job {}] Transcoding task failed (panic or cancellation): {}", job.payload.job_id, join_err);
                                // Also not ACKing here.
                            }
                        }
                    }
                }
            }
            Ok(None) => {
                // This can happen if BLOCK is 0 and stream is empty, or with some other configurations.
                // With BLOCK > 0, this usually means no data within the block timeout.
                debug!("No messages received for consumer '{}' within timeout, re-checking.", consumer_name);
            }
            Err(e) => {
                error!("Error reading from Valkey stream '{}': {}. Retrying after delay.", stream_key, e);
                sleep(Duration::from_millis(WORKER_LOOP_DELAY_MS)).await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger (RUST_LOG=info, worker=debug cargo run)
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let valkey_url = std::env::var(VALKEY_URL_ENV_VAR).unwrap_or_else(|_| {
        info!("{} environment variable not set, using default: {}", VALKEY_URL_ENV_VAR, DEFAULT_VALKEY_URL);
        DEFAULT_VALKEY_URL.to_string()
    });

    let consumer_id = Uuid::new_v4().to_string();
    let hostname = get_consumer_hostname();
    let consumer_name = format!("worker-{}-{}", hostname, consumer_id);

    info!("Starting worker with consumer name: {}", consumer_name);
    info!("Connecting to Valkey at: {}", valkey_url);

    let client = redis::Client::open(valkey_url)?;
    let mut con = client.get_multiplexed_async_connection().await.map_err(|e| {
        error!("Failed to connect to Valkey: {}", e);
        e // Convert to Box<dyn Error> compatible error if needed, but client.get_... already returns RedisError
    })?;
    info!("Successfully connected to Valkey.");

    if let Err(e) = ensure_consumer_group_exists(&mut con, TRANSCODING_JOB_STREAM_KEY, JOB_CONSUMER_GROUP).await {
        error!("Could not ensure consumer group exists: {}. Exiting.", e);
        return Err(e.into());
    }
    
    // We clone the connection for the processing loop. MultiplexedConnection is designed for this.
    let processing_con = con.clone();

    tokio::select! {
        _ = process_jobs_loop(processing_con, TRANSCODING_JOB_STREAM_KEY, JOB_CONSUMER_GROUP, &consumer_name) => {
            error!("Job processing loop exited unexpectedly.");
        }
        _ = signal::ctrl_c() => {
            info!("CTRL-C received, shutting down worker '{}'.", consumer_name);
        }
    }
    
    info!("Worker {} finished.", consumer_name);
    Ok(())
}