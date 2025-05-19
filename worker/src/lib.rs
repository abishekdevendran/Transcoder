#[derive(Debug)]
pub struct TranscodeOptions {
    pub output_resolution: String,
    pub video_bitrate: String,
    pub audio_bitrate: String,
    pub preset: String,
    pub frame_rate: u32,
}

impl Default for TranscodeOptions {
    fn default() -> Self {
        Self {
            output_resolution: "640x360".into(),
            video_bitrate: "500k".into(),
            audio_bitrate: "96k".into(),
            preset: "medium".into(),
            frame_rate: 30,
        }
    }
}

pub struct TranscodeOptionsBuilder {
    output_resolution: Option<String>,
    video_bitrate: Option<String>,
    audio_bitrate: Option<String>,
    preset: Option<String>,
    frame_rate: Option<u32>,
}

impl Default for TranscodeOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TranscodeOptionsBuilder {
    pub fn new() -> Self {
        Self {
            output_resolution: None,
            video_bitrate: None,
            audio_bitrate: None,
            preset: None,
            frame_rate: None,
        }
    }

    pub fn output_resolution(mut self, val: &str) -> Self {
        self.output_resolution = Some(val.to_string());
        self
    }

    pub fn video_bitrate(mut self, val: &str) -> Self {
        self.video_bitrate = Some(val.to_string());
        self
    }

    pub fn audio_bitrate(mut self, val: &str) -> Self {
        self.audio_bitrate = Some(val.to_string());
        self
    }

    pub fn preset(mut self, val: &str) -> Self {
        self.preset = Some(val.to_string());
        self
    }

    pub fn frame_rate(mut self, val: u32) -> Self {
        self.frame_rate = Some(val);
        self
    }

    pub fn build(self) -> TranscodeOptions {
        let default = TranscodeOptions::default();
        TranscodeOptions {
            output_resolution: self.output_resolution.unwrap_or(default.output_resolution),
            video_bitrate: self.video_bitrate.unwrap_or(default.video_bitrate),
            audio_bitrate: self.audio_bitrate.unwrap_or(default.audio_bitrate),
            preset: self.preset.unwrap_or(default.preset),
            frame_rate: self.frame_rate.unwrap_or(default.frame_rate),
        }
    }
}
