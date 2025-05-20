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
type JobRequest struct {
	JobID             string           `json:"job_id,omitempty"` // Client can suggest a job_id
	InputObjectKey    string           `json:"input_object_key"`  // e.g., "videos/my_raw_movie.mp4"
	OutputObjectKey   string           `json:"output_object_key"` // e.g., "transcoded/my_movie_720p.mp4"
	Options           TranscodeOptions `json:"options"`
}

// For Valkey stream, the worker expects 'options_json' as a string field.
// So, we'll have a slightly different structure for XADD.
type ValkeyJobMessage struct {
	JobID           string `msgpack:"job_id" json:"job_id"`
	InputBucket     string `msgpack:"input_bucket" json:"input_bucket"`         // e.g., "input-videos"
	InputObjectKey  string `msgpack:"input_object_key" json:"input_object_key"`
	OutputBucket    string `msgpack:"output_bucket" json:"output_bucket"`       // e.g., "output-videos"
	OutputObjectKey string `msgpack:"output_object_key" json:"output_object_key"`
	OptionsJSON     string `msgpack:"options_json" json:"options_json"`
}