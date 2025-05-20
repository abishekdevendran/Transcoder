use log::{error, info};
use redis::{aio::MultiplexedConnection, AsyncCommands, RedisError, RedisResult};

// Helper to get hostname or a default
pub fn get_consumer_hostname() -> String {
    hostname::get()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "unknown-host".to_string())
}

pub async fn ensure_consumer_group_exists(
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
            info!(
                "Consumer group '{}' created (or already existed) for stream '{}'",
                group_name, stream_key
            );
            Ok(())
        }
        Err(e) => {
            // Check if the error is because the group already exists (BUSYGROUP)
            if e.to_string().contains("BUSYGROUP") {
                info!(
                    "Consumer group '{}' already exists for stream '{}'",
                    group_name, stream_key
                );
                Ok(()) // Treat as success
            } else {
                error!(
                    "Failed to create consumer group '{}' for stream '{}': {}",
                    group_name, stream_key, e
                );
                Err(e)
            }
        }
    }
}

