use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::{Client, Error as S3Error};
use aws_sdk_s3::primitives::ByteStream;
use log::{debug, error, info};
use std::path::Path;
use std::time::Duration;
use tokio::fs::File as TokioFile;
use tokio::io::AsyncWriteExt;

// Configuration for the S3 client, specific to MinIO
const MINIO_ENDPOINT_URL_ENV_VAR: &str = "MINIO_ENDPOINT_URL";
const MINIO_REGION_ENV_VAR: &str = "MINIO_AWS_REGION"; // MinIO doesn't really use regions but SDK might need one
const DEFAULT_MINIO_ENDPOINT_URL: &str = "http://localhost:9000"; // Ensure this matches your MinIO API port
const DEFAULT_MINIO_REGION: &str = "us-east-1"; // Default fallback region


pub async fn new_s3_client() -> Result<Client, S3Error> {
    let endpoint_url = std::env::var(MINIO_ENDPOINT_URL_ENV_VAR)
        .unwrap_or_else(|_| DEFAULT_MINIO_ENDPOINT_URL.to_string());
    
    let region = std::env::var(MINIO_REGION_ENV_VAR)
        .unwrap_or_else(|_| DEFAULT_MINIO_REGION.to_string());

    // For MinIO, we need to explicitly set the endpoint URL.
    // Credentials will be picked up from standard AWS environment variables if set
    // (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN),
    // or an EC2 instance profile, etc.
    // For local MinIO with docker-compose, if these are not set in the worker's env,
    // you might need to pass them explicitly or ensure the worker's execution environment has them.
    // However, often for local MinIO, if it's on localhost and allows anonymous read for the bucket,
    // or if security is relaxed, it might work without explicit credentials.
    // For robust setup, credentials should be configured.
    // Let's assume for now that either credentials are in env or MinIO is accessible.
    
    let sdk_config = aws_config::load_from_env().await; // Loads credentials from env, ~/.aws/credentials, etc.

    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .region(aws_sdk_s3::config::Region::new(region))
        .endpoint_url(endpoint_url)
        // For MinIO, path-style addressing is often needed, especially if not using a DNS-compatible bucket name.
        .force_path_style(true) 
        .build();

    info!("Creating S3 client with endpoint: {}", s3_config.endpoint_url().unwrap_or("Not set"));
    Ok(Client::from_conf(s3_config))
}

pub async fn download_file(
    client: &Client,
    bucket: &str,
    key: &str,
    download_path: &Path,
) -> Result<(), String> {
    info!("S3: Attempting to download s3://{}/{} to {:?}", bucket, key, download_path);

    match client.get_object().bucket(bucket).key(key).send().await {
        Ok(mut resp) => {
            // Ensure parent directory for download_path exists
            if let Some(parent_dir) = download_path.parent() {
                if !parent_dir.exists() {
                    tokio::fs::create_dir_all(parent_dir).await
                        .map_err(|e| format!("Failed to create download directory {:?}: {}", parent_dir, e))?;
                }
            }

            let mut file = TokioFile::create(download_path).await
                .map_err(|e| format!("Failed to create file {:?}: {}", download_path, e))?;
            
            let mut bytes_written: u64 = 0;
            while let Some(chunk) = resp.body.try_next().await
                .map_err(|e| format!("Error reading S3 object body stream for s3://{}/{}: {}", bucket, key, e))? 
            {
                file.write_all(&chunk).await
                    .map_err(|e| format!("Failed to write to file {:?}: {}", download_path, e))?;
                bytes_written += chunk.len() as u64;
            }
            file.flush().await.map_err(|e| format!("Failed to flush file {:?}: {}", download_path, e))?;
            debug!("S3: Successfully downloaded {} bytes from s3://{}/{} to {:?}", bytes_written, bucket, key, download_path);
            Ok(())
        }
        Err(e) => {
            let err_msg = format!("S3: Failed to get object s3://{}/{}: {}", bucket, key, e);
            error!("{}", err_msg);
            Err(err_msg)
        }
    }
}

pub async fn upload_file(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_path: &Path,
) -> Result<(), String> {
    info!("S3: Attempting to upload {:?} to s3://{}/{}", upload_path, bucket, key);

    if !upload_path.exists() {
        return Err(format!("File to upload does not exist: {:?}", upload_path));
    }

    match ByteStream::from_path(upload_path).await {
        Ok(body) => {
            match client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(body)
                // .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead) // Optional: if you want uploaded objects to be public
                .send()
                .await
            {
                Ok(_) => {
                    info!("S3: Successfully uploaded {:?} to s3://{}/{}", upload_path, bucket, key);

                    // Optional: Generate and log a presigned URL for quick access (for debugging)
                    // This requires GetObject permission for the signer.
                    /*
                    match client.get_object()
                        .bucket(bucket)
                        .key(key)
                        .presigned(PresigningConfig::expires_in(Duration::from_secs(3600)).unwrap_or_default()) // 1 hour
                        .await {
                        Ok(presigned_request) => {
                            info!("S3: Presigned URL for s3://{}/{}: {}", bucket, key, presigned_request.uri());
                        }
                        Err(e) => {
                            warn!("S3: Could not generate presigned URL for s3://{}/{}: {}", bucket, key, e);
                        }
                    }
                    */
                    Ok(())
                }
                Err(e) => {
                    let err_msg = format!("S3: Failed to upload {:?} to s3://{}/{}: {}", upload_path, bucket, key, e);
                    error!("{}", err_msg);
                    Err(err_msg)
                }
            }
        }
        Err(e) => {
            let err_msg = format!("S3: Failed to create ByteStream from path {:?}: {}", upload_path, e);
            error!("{}", err_msg);
            Err(err_msg)
        }
    }
}