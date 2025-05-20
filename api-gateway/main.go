// video-transcoder/api-gateway/main.go
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	valkeyURLEnvVar         = "VALKEY_URL"
	defaultValkeyURL        = "redis://127.0.0.1:6379"
	transcodingJobStreamKey  = "transcoding_jobs" // Must match Rust worker
	httpServerAddr          = ":8080"
	defaultInputBucketName  = "input-videos"
	defaultOutputBucketName = "output-videos"
)

var rdb *redis.Client

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func initValkeyClient() {
	valkeyURL := getEnv(valkeyURLEnvVar, defaultValkeyURL)
	opts, err := redis.ParseURL(valkeyURL)
	if err != nil {
		log.Fatalf("Could not parse Valkey URL: %v", err)
	}
	rdb = redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Could not connect to Valkey: %v", err)
	}
	log.Println("Successfully connected to Valkey.")
}

func transcodeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var apiReq JobRequest // Changed from JobPayload
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&apiReq); err != nil {
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Validate required fields (basic example)
	if apiReq.InputObjectKey == "" || apiReq.OutputObjectKey == "" {
		http.Error(w, "input_object_key and output_object_key are required", http.StatusBadRequest)
		return
	}


	jobID := apiReq.JobID
	if jobID == "" {
		jobID = uuid.New().String()
	}

	optionsJSONBytes, err := json.Marshal(apiReq.Options)
	if err != nil {
		log.Printf("Error marshalling transcode options for job %s: %v", jobID, err)
		http.Error(w, "Error processing transcode options", http.StatusInternalServerError)
		return
	}
	optionsJSONStr := string(optionsJSONBytes)

	// Prepare the message for Valkey XADD using ValkeyJobMessage fields
	valkeyMessage := ValkeyJobMessage{
		JobID:           jobID,
		InputBucket:     defaultInputBucketName, // Using hardcoded bucket
		InputObjectKey:  apiReq.InputObjectKey,
		OutputBucket:    defaultOutputBucketName, // Using hardcoded bucket
		OutputObjectKey: apiReq.OutputObjectKey,
		OptionsJSON:     optionsJSONStr,
	}

	// For XADD with go-redis, we can pass the struct directly if its fields
	// are simple types or have `redis` tags.
	// Or, convert it to a map[string]interface{} as before.
	// Let's use a map for consistency with previous version and explicit field names.
	messageValues := map[string]interface{}{
		"job_id":            valkeyMessage.JobID,
		"input_bucket":      valkeyMessage.InputBucket,
		"input_object_key":  valkeyMessage.InputObjectKey,
		"output_bucket":     valkeyMessage.OutputBucket,
		"output_object_key": valkeyMessage.OutputObjectKey,
		"options_json":      valkeyMessage.OptionsJSON,
	}


	ctx := context.Background()
	streamID, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: transcodingJobStreamKey,
		ID:     "*",
		Values: messageValues, // Use the map
	}).Result()

	if err != nil {
		log.Printf("Error publishing job %s to Valkey: %v", jobID, err)
		http.Error(w, "Could not submit job", http.StatusInternalServerError)
		return
	}

	log.Printf("Successfully submitted job %s (Valkey ID %s). Input: %s/%s, Output: %s/%s",
		jobID, streamID, valkeyMessage.InputBucket, valkeyMessage.InputObjectKey,
		valkeyMessage.OutputBucket, valkeyMessage.OutputObjectKey)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"message":    "Job submitted successfully",
		"job_id":     jobID,
		"valkey_id": streamID,
	})
}

func main() {
	initValkeyClient()
	http.HandleFunc("/transcode", transcodeHandler)
	log.Printf("API Gateway starting on %s", httpServerAddr)
	if err := http.ListenAndServe(httpServerAddr, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}