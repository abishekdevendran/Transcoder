package main

type TranscodeOptions struct {
	OutputResolution string `json:"output_resolution"`
	VideoCodec       string `json:"video_codec"`
	VideoBitrate     string `json:"video_bitrate,omitempty"` // omitempty if CRF is used
	CRF              *uint8 `json:"crf,omitempty"`           // Pointer for optional, null in JSON
	Preset           string `json:"preset"`
	AudioCodec       string `json:"audio_codec"`
	AudioBitrate     string `json:"audio_bitrate"`
}

// JobPayload is the structure for the message sent to Valkey.
type JobPayload struct {
	JobID       string           `json:"job_id"`
	InputPath   string           `json:"input_path"`
	OutputPath  string           `json:"output_path"`
	Options     TranscodeOptions `json:"options"` // This will be marshalled to options_json for the worker
}

// For Valkey stream, the worker expects 'options_json' as a string field.
// So, we'll have a slightly different structure for XADD.
type ValkeyJobMessage struct {
	JobID       string `msgpack:"job_id" json:"job_id"` // Using msgpack for redis, json for API
	InputPath   string `msgpack:"input_path" json:"input_path"`
	OutputPath  string `msgpack:"output_path" json:"output_path"`
	OptionsJSON string `msgpack:"options_json" json:"options_json"` // The JSON string of TranscodeOptions
}