// worker/src/main.rs
use std::path::Path;

// Import log macros
use log::{info, error};

// Declare the modules
mod options;
mod transcoder;

// Use specific items from our modules
use options::TranscodeOptions;
use transcoder::transcode_video;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let input_file = Path::new("sample_input.avi");
    let output_dir = Path::new("output_videos");
    
    if !output_dir.exists() {
        if let Err(e) = std::fs::create_dir_all(output_dir) {
            error!("Failed to create output directory {:?}: {}", output_dir, e);
            return;
        }
    }

    // Check for input file existence *before* trying to use it
    if !input_file.exists() {
        // If you still want the dummy file generator, it would go here.
        // For now, just error out as per your previous update.
        error!("Input file {:?} does not exist. Please create it (e.g., 'sample_input.mp4').", input_file);
        // You could also add a note here about a command to generate a dummy file if you implement one.
        // For example, by running: ffmpeg -y -f lavfi -i testsrc=duration=5:size=1280x720:rate=30 -c:v libx264 -pix_fmt yuv420p -t 5 sample_input.mp4
        return; 
    }

    // Example 1: Using default options
    let default_options = TranscodeOptions::default();
    info!("--- Running with default options (bitrate based) ---");
    match transcode_video(input_file, &output_dir.join("transcoded_default.mp4"), &default_options) {
        Ok(()) => info!("Default options transcoding finished successfully."),
        Err(e) => error!("Default options transcoding failed: {}", e),
    }
    println!("\n");

    // Example 2: Using CRF for quality
    let crf_options = TranscodeOptions {
        output_resolution: "1280x720".to_string(),
        video_codec: "libx264".to_string(),
        video_bitrate: Some("2M".to_string()),
        crf: Some(23),
        preset: "medium".to_string(),
        audio_codec: "aac".to_string(),
        audio_bitrate: "192k".to_string(),
    };
    info!("--- Running with CRF options ---");
    match transcode_video(input_file, &output_dir.join("transcoded_crf_720p.mp4"), &crf_options) {
        Ok(()) => info!("CRF options transcoding finished successfully."),
        Err(e) => error!("CRF options transcoding failed: {}", e),
    }
    println!("\n");
    
    // Example 3: WebM output with VP9 and Opus
    let webm_options = TranscodeOptions {
        output_resolution: "320x240".to_string(),
        video_codec: "libvpx-vp9".to_string(),
        video_bitrate: Some("300k".to_string()),
        crf: Some(32),
        preset: "medium".to_string(),
        audio_codec: "libopus".to_string(),
        audio_bitrate: "64k".to_string(),
    };
    info!("--- Running with WebM (VP9/Opus) options ---");
    match transcode_video(input_file, &output_dir.join("transcoded_vp9_opus.webm"), &webm_options) {
        Ok(()) => info!("WebM options transcoding finished successfully."),
        Err(e) => error!("WebM options transcoding failed: {}", e),
    }
}