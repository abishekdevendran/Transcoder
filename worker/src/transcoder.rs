// worker/src/transcoder.rs
use std::path::Path;
use std::process::{Command, Stdio};
use std::io::{Error, ErrorKind}; // Or your custom error type in the future
use log::{info, warn, error, debug, trace};

// Import TranscodeOptions from the options module (sibling file)
use crate::options::TranscodeOptions; // `crate::` refers to the root of our crate

pub fn transcode_video(
    input_path: &Path,
    output_path: &Path,
    options: &TranscodeOptions,
) -> Result<(), Error> {
    if !input_path.exists() {
        let err_msg = format!("Input file not found: {:?}", input_path);
        error!("{}", err_msg);
        return Err(Error::new(ErrorKind::NotFound, err_msg));
    }

    info!(
        "Starting transcoding: {:?} -> {:?}",
        input_path, output_path
    );
    debug!("Transcoding options: {:?}", options);

    if let Some(parent_dir) = output_path.parent() {
        if !parent_dir.exists() {
            match std::fs::create_dir_all(parent_dir) {
                Ok(_) => info!("Created output directory: {:?}", parent_dir),
                Err(e) => {
                    let err_msg = format!("Failed to create output directory {:?}: {}", parent_dir, e);
                    error!("{}", err_msg);
                    // Using std::io::Error::other as per your clippy fix
                    return Err(std::io::Error::other(err_msg));
                }
            }
        }
    }

    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-y");
    cmd.arg("-i");
    cmd.arg(input_path.as_os_str());
    cmd.arg("-c:v");
    cmd.arg(&options.video_codec);

    if let Some(crf_val) = options.crf {
        if options.video_codec == "libx264" || options.video_codec == "libx265" {
            cmd.arg("-crf");
            cmd.arg(crf_val.to_string());
            if let Some(vb) = &options.video_bitrate {
                warn!("CRF value ({}) is set, video_bitrate ({}) will likely be ignored by ffmpeg for quality targeting, unless used as -maxrate.", crf_val, vb);
            }
        } else {
            warn!("CRF is specified but video codec {} might not support it. Falling back to video_bitrate if available.", options.video_codec);
            if let Some(vb) = &options.video_bitrate {
                cmd.arg("-b:v");
                cmd.arg(vb);
            } else {
                let err_msg = format!("CRF not supported for {} and no video_bitrate provided.", options.video_codec);
                error!("{}", err_msg);
                return Err(Error::new(ErrorKind::InvalidInput, err_msg));
            }
        }
    } else if let Some(vb) = &options.video_bitrate {
        cmd.arg("-b:v");
        cmd.arg(vb);
    } else {
        let err_msg = "Video quality setting missing: Neither CRF nor video_bitrate provided.".to_string();
        error!("{}", err_msg);
        return Err(Error::new(ErrorKind::InvalidInput, err_msg));
    }

    cmd.arg("-preset");
    cmd.arg(&options.preset);
    cmd.arg("-vf");
    cmd.arg(format!("scale={}", options.output_resolution));
    
    if options.video_codec == "libx264" {
        cmd.arg("-pix_fmt");
        cmd.arg("yuv420p");
    }

    cmd.arg("-c:a");
    cmd.arg(&options.audio_codec);
    cmd.arg("-b:a");
    cmd.arg(&options.audio_bitrate);
    cmd.arg(output_path.as_os_str());

    trace!("Executing FFMPEG command: {:?}", cmd);

    let output_res = cmd.stdout(Stdio::inherit())
                     .stderr(Stdio::inherit())
                     .output();
    
    match output_res {
        Ok(out) => {
            if out.status.success() {
                info!(
                    "Transcoding successful: {:?} -> {:?}",
                    input_path, output_path
                );
                Ok(())
            } else {
                let err_msg = format!(
                    "ffmpeg command failed with status: {}. Check ffmpeg output above for details.",
                    out.status
                );
                error!("{}", err_msg);
                Err(std::io::Error::other(err_msg))
            }
        }
        Err(e) => {
            let err_msg = format!("Failed to execute ffmpeg: {}", e);
            error!("{}", err_msg);
            Err(e)
        }
    }
}