// worker/src/main.rs
use std::collections::HashMap;
use std::fs; // For std::fs::remove_file
use std::path::{Path, PathBuf}; // Use PathBuf for owned paths
use std::time::Duration;

use log::{info, warn, error, debug};
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, RedisError, RedisResult};
use tokio::signal;
use tokio::time::sleep;
use uuid::Uuid;

// Declare our modules
mod options;
mod transcoder;
mod job;
mod s3; // New S3 module

// Use specific items from our modules
use transcoder::transcode_video;
use job::{parse_job_from_map, Job, JOB_CONSUMER_GROUP, TRANSCODING_JOB_STREAM_KEY};
use s3::{new_s3_client, download_file, upload_file};
use worker::{ensure_consumer_group_exists, get_consumer_hostname}; // S3 functions

// Configuration Constants
const VALKEY_URL_ENV_VAR: &str = "VALKEY_URL";
const DEFAULT_VALKEY_URL: &str = "redis://127.0.0.1:6379";
const WORKER_LOOP_DELAY_MS: u64 = 1000;
const STREAM_BLOCK_TIMEOUT_MS: usize = 5000;
const TEMP_DOWNLOAD_DIR: &str = "temp_downloads"; // Subdirectory for temporary files
const TEMP_OUTPUT_DIR: &str = "temp_outputs";   // Subdirectory for temporary transcoded files

async fn process_single_job(
    s3_client: &aws_sdk_s3::Client,
    job: Job, // Take ownership of job
) -> Result<(), String> { // Return a String error for easier propagation
    let job_id = job.payload.job_id.clone(); // Clone for logging after move
    info!("[Job {}] Processing. Input: s3://{}/{}, Output: s3://{}/{}",
        job_id, job.payload.input_bucket, job.payload.input_object_key,
        job.payload.output_bucket, job.payload.output_object_key);
    debug!("[Job {}] Options: {:?}", job_id, job.payload.options);

    // Define temporary local paths
    // Ensure unique filenames to avoid collisions if multiple workers run on the same host (less likely with containers)
    // Or use a job-specific subdirectory. For simplicity, just using job_id in filename.
    let temp_input_filename = format!("{}_{}", job_id, Path::new(&job.payload.input_object_key).file_name().unwrap_or_default().to_string_lossy());
    let local_input_path = PathBuf::from(TEMP_DOWNLOAD_DIR).join(&temp_input_filename);
    
    let temp_output_filename_base = Path::new(&job.payload.output_object_key).file_name().unwrap_or_default().to_string_lossy();
    let local_output_path = PathBuf::from(TEMP_OUTPUT_DIR).join(format!("{}_{}", job_id, temp_output_filename_base));

    // Ensure temp directories exist
    if !Path::new(TEMP_DOWNLOAD_DIR).exists() {
        tokio::fs::create_dir_all(TEMP_DOWNLOAD_DIR).await.map_err(|e| format!("[Job {}] Failed to create temp download dir: {}", job_id, e))?;
    }
    if !Path::new(TEMP_OUTPUT_DIR).exists() {
        tokio::fs::create_dir_all(TEMP_OUTPUT_DIR).await.map_err(|e| format!("[Job {}] Failed to create temp output dir: {}", job_id, e))?;
    }


    // 1. Download from S3
    info!("[Job {}] Downloading from s3://{}/{} to {:?}", job_id, job.payload.input_bucket, job.payload.input_object_key, local_input_path);
    download_file(s3_client, &job.payload.input_bucket, &job.payload.input_object_key, &local_input_path).await
        .map_err(|e| format!("[Job {}] S3 Download failed: {}", job_id, e))?;

    // 2. Transcode (CPU-bound, run in spawn_blocking)
    // Clone data needed for the blocking task
    let options_clone = job.payload.options.clone(); // TranscodeOptions should be Clone
    let local_input_path_clone = local_input_path.clone();
    let local_output_path_clone = local_output_path.clone();
    let job_id_clone_for_transcoding = job_id.clone();

    info!("[Job {}] Starting transcoding from {:?} to {:?}", job_id_clone_for_transcoding, local_input_path_clone, local_output_path_clone);
    let transcode_result = tokio::task::spawn_blocking(move || {
        transcode_video(
            &local_input_path_clone,
            &local_output_path_clone,
            &options_clone,
        )
    }).await;

    // Handle transcoding result
    match transcode_result {
        Ok(Ok(())) => {
            info!("[Job {}] Transcoding successful.", job_id);
            // 3. Upload to S3
            info!("[Job {}] Uploading {:?} to s3://{}/{}", job_id, local_output_path, job.payload.output_bucket, job.payload.output_object_key);
            upload_file(s3_client, &job.payload.output_bucket, &job.payload.output_object_key, &local_output_path).await
                .map_err(|e| format!("[Job {}] S3 Upload failed: {}", job_id, e))?;
            Ok(())
        }
        Ok(Err(io_err)) => { // transcode_video returned an error
            let msg = format!("[Job {}] Transcoding failed: {}", job_id, io_err);
            error!("{}", msg);
            Err(msg)
        }
        Err(join_err) => { // spawn_blocking task itself failed (e.g., panic)
            let msg = format!("[Job {}] Transcoding task failed (panic/cancellation): {}", job_id, join_err);
            error!("{}", msg);
            Err(msg)
        }
    }
    .map_err(|e| { // Convert successful transcode but failed upload also to error string
        error!("{}", e); 
        e
    })
    .and_then(|()| { // Cleanup only if all previous steps were okay
        // 4. Cleanup local files (sync is fine here, or use tokio::fs)
        debug!("[Job {}] Cleaning up temporary files: {:?}, {:?}", job_id, local_input_path, local_output_path);
        fs::remove_file(&local_input_path).map_err(|e| format!("[Job {}] Failed to delete temp input file {:?}: {}", job_id, local_input_path, e))?;
        fs::remove_file(&local_output_path).map_err(|e| format!("[Job {}] Failed to delete temp output file {:?}: {}", job_id, local_output_path, e))?;
        info!("[Job {}] Successfully processed and cleaned up.", job_id);
        Ok(())
    })
}


async fn jobs_processing_loop( // Renamed from process_jobs_loop to avoid confusion
    mut valkey_con: MultiplexedConnection,
    s3_client: aws_sdk_s3::Client, // Pass S3 client
    stream_key: &str,
    group_name: &str,
    consumer_name: &str,
) {
    info!("Worker '{}' starting to process jobs from stream '{}', group '{}'", consumer_name, stream_key, group_name);

    loop {
        let read_opts = StreamReadOptions::default()
            .group(group_name, consumer_name)
            .count(1)
            .block(STREAM_BLOCK_TIMEOUT_MS);
        
        let result: RedisResult<Option<StreamReadReply>> = valkey_con
            .xread_options(&[stream_key], &[">"], &read_opts)
            .await;

        match result {
            Ok(Some(reply)) => {
                if reply.keys.is_empty() {
                    debug!("No new messages for consumer '{}', continuing.", consumer_name);
                    continue;
                }

                for stream_key_entry in reply.keys {
                    for message_entry in stream_key_entry.ids {
                        let job = match parse_job_from_map(message_entry.id.clone(), message_entry.map) {
                            Ok(j) => j,
                            Err(e) => {
                                error!("Failed to parse job (Valkey ID: {}): {}. Skipping.", message_entry.id, e);
                                // Consider a strategy for unparseable messages (e.g., move to dead-letter queue or XACK to remove)
                                continue;
                            }
                        };
                        
                        let job_id_for_ack = job.payload.job_id.clone();
                        let message_id_for_ack = job.message_id.clone();
                        
                        // Process the job
                        // Pass a cloned S3 client if it's not Clone, or pass by reference if loop structure allows
                        // S3 client from aws-sdk-rust v1.x IS Clone.
                        match process_single_job(&s3_client, job).await {
                            Ok(()) => {
                                info!("[Job {}] Successfully processed.", job_id_for_ack);
                                match valkey_con.xack::<_, _, _, i64>(stream_key, group_name, &[&message_id_for_ack]).await {
                                    Ok(acked_count) if acked_count > 0 => 
                                        info!("[Job {}] Successfully ACKed Valkey message_id: {}", job_id_for_ack, message_id_for_ack),
                                    Ok(_) => 
                                        warn!("[Job {}] ACK command for Valkey message_id {} returned 0 or unexpected count.", job_id_for_ack, message_id_for_ack),
                                    Err(e) => 
                                        error!("[Job {}] Failed to ACK Valkey message_id {}: {}", job_id_for_ack, message_id_for_ack, e),
                                }
                            }
                            Err(processing_err_msg) => {
                                error!("[Job {}] Failed to process: {}. Valkey message {} will NOT be ACKed.", job_id_for_ack, processing_err_msg, message_id_for_ack);
                                // Message not ACKed, remains in PEL for potential retry or manual handling.
                            }
                        }
                    }
                }
            }
            Ok(None) => {
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
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let valkey_url = std::env::var(VALKEY_URL_ENV_VAR).unwrap_or_else(|_| DEFAULT_VALKEY_URL.to_string());
    let consumer_id = Uuid::new_v4().to_string();
    let hostname = get_consumer_hostname();
    let consumer_name = format!("worker-{}-{}", hostname, consumer_id);

    info!("Starting worker with consumer name: {}", consumer_name);

    // Initialize S3 client
    let s3_client = new_s3_client().await.map_err(|e| {
        error!("Failed to create S3 client: {}", e);
        // Convert S3Error to a Box<dyn std::error::Error>
        // This is a bit verbose; a custom error type for the application would be cleaner.
        Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("S3 client init error: {}", e)))
    })?;
    info!("S3 client initialized.");

    // Initialize Valkey client
    info!("Connecting to Valkey at: {}", valkey_url);
    let valkey_client = redis::Client::open(valkey_url)?;
    let mut valkey_con_for_group_check = valkey_client.get_multiplexed_async_connection().await.map_err(|e| {
        error!("Failed to connect to Valkey: {}", e);
        Box::new(e) as Box<RedisError>
    })?;
    info!("Successfully connected to Valkey.");

    if let Err(e) = ensure_consumer_group_exists(&mut valkey_con_for_group_check, TRANSCODING_JOB_STREAM_KEY, JOB_CONSUMER_GROUP).await {
        error!("Could not ensure consumer group exists: {}. Exiting.", e);
        return Err(Box::new(e));
    }
    
    let valkey_con_for_loop = valkey_client.get_multiplexed_async_connection().await?;


    tokio::select! {
        _ = jobs_processing_loop(valkey_con_for_loop, s3_client, TRANSCODING_JOB_STREAM_KEY, JOB_CONSUMER_GROUP, &consumer_name) => {
            error!("Job processing loop exited unexpectedly.");
        }
        _ = signal::ctrl_c() => {
            info!("CTRL-C received, shutting down worker '{}'.", consumer_name);
        }
    }
    
    info!("Worker {} finished.", consumer_name);
    Ok(())
}