use crate::options::TranscodeOptions;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const TRANSCODING_JOB_STREAM_KEY: &str = "transcoding_jobs";
pub const JOB_CONSUMER_GROUP: &str = "video_workers_group";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobPayload {
    pub job_id: String,
    pub input_bucket: String,      // New
    pub input_object_key: String,  // Changed from input_path
    pub output_bucket: String,     // New
    pub output_object_key: String, // Changed from output_path
    pub options: TranscodeOptions, // This should contain the actual options, not a JSON string
}

#[derive(Debug, Clone)]
pub struct Job {
    pub message_id: String, // Valkey message ID
    pub payload: JobPayload,
}

pub fn parse_job_from_map(
    message_id: String,
    fields: HashMap<String, redis::Value>,
) -> Result<Job, String> {
    let job_id: String =
        redis::from_redis_value(fields.get("job_id").ok_or("Missing job_id field")?)
            .map_err(|e| format!("Failed to parse job_id: {}", e))?;

    let options_json_str: String = redis::from_redis_value(
        fields
            .get("options_json")
            .ok_or("Missing options_json field")?,
    )
    .map_err(|e| format!("Failed to parse options_json string: {}", e))?;

    let options: TranscodeOptions = serde_json::from_str(&options_json_str)
        .map_err(|e| format!("Failed to deserialize TranscodeOptions from JSON: {}", e))?;

    let input_bucket = redis::from_redis_value(
        fields
            .get("input_bucket")
            .ok_or("Missing input_bucket field")?,
    ).map_err(|e| format!("Failed to parse input_bucket: {}", e))?;

    let input_object_key = redis::from_redis_value(
        fields
            .get("input_object_key")
            .ok_or("Missing input_object_key field")?,
    ).map_err(|e| format!("Failed to parse input_object_key: {}", e))?;

    let output_bucket = redis::from_redis_value(
        fields
            .get("output_bucket")
            .ok_or("Missing output_bucket field")?,
    ).map_err(|e| format!("Failed to parse output_bucket: {}", e))?;

    let output_object_key = redis::from_redis_value(
        fields
            .get("output_object_key")
            .ok_or("Missing output_object_key field")?,
    ).map_err(|e| format!("Failed to parse output_object_key: {}", e))?;

    Ok(Job {
        message_id,
        payload: JobPayload {
            job_id,
            options,
            input_bucket,
            input_object_key,
            output_bucket,
            output_object_key,
        },
    })
}
