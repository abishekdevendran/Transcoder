use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscodeOptions {
    pub output_resolution: String,
    pub video_codec: String,
    pub video_bitrate: Option<String>,
    pub crf: Option<u8>,
    pub preset: String,
    pub audio_codec: String,
    pub audio_bitrate: String,
}

impl Default for TranscodeOptions {
    fn default() -> Self {
        TranscodeOptions {
            output_resolution: "640x360".to_string(),
            video_codec: "libx264".to_string(),
            video_bitrate: Some("500k".to_string()),
            crf: None,
            preset: "medium".to_string(),
            audio_codec: "aac".to_string(),
            audio_bitrate: "96k".to_string(),
        }
    }
}

// We could add methods to TranscodeOptions here in the future, e.g., validation
// impl TranscodeOptions {
//     pub fn validate(&self) -> Result<(), String> {
//         // Add validation logic
//         Ok(())
//     }
// }