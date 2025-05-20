// video-transcoder/api-gateway/main.go
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"
	"github.com/google/uuid" // For generating job IDs
	"github.com/redis/go-redis/v9"
)

const (
	valkeyURLEnvVar        = "VALKEY_URL"
	defaultValkeyURL       = "redis://127.0.0.1:6379"
	transcodingJobStreamKey = "transcoding_jobs" // Must match Rust worker
	httpServerAddr         = ":8080"
)

var rdb *redis.Client

// Helper function to get environment variable or default
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

	var jobReq JobPayload
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&jobReq); err != nil {
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Generate a unique job ID if not provided (or override)
	if jobReq.JobID == "" {
		jobReq.JobID = uuid.New().String()
	}

	// Convert TranscodeOptions to JSON string for the Valkey message
	optionsJSONBytes, err := json.Marshal(jobReq.Options)
	if err != nil {
		log.Printf("Error marshalling transcode options for job %s: %v", jobReq.JobID, err)
		http.Error(w, "Error processing transcode options", http.StatusInternalServerError)
		return
	}
	optionsJSONStr := string(optionsJSONBytes)

	// Prepare the message for Valkey XADD
	// Valkey streams store messages as field-value pairs.
	// The redis client for Go often accepts a map[string]interface{} or a struct.
	// For simplicity with XADD, we can pass a map.
	messageValues := map[string]interface{}{
		"job_id":       jobReq.JobID,
		"input_path":   jobReq.InputPath,
		"output_path":  jobReq.OutputPath,
		"options_json": optionsJSONStr, // This is what the Rust worker expects
	}

	ctx := context.Background()
	streamID, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: transcodingJobStreamKey,
		// MaxLen: 0, // No max length for now
		// Approx: true, // For MaxLen, makes it faster but less precise
		ID:     "*", // Auto-generate ID
		Values: messageValues,
	}).Result()

	if err != nil {
		log.Printf("Error publishing job %s to Valkey: %v", jobReq.JobID, err)
		http.Error(w, "Could not submit job", http.StatusInternalServerError)
		return
	}

	log.Printf("Successfully submitted job %s to stream %s with Valkey ID %s", jobReq.JobID, transcodingJobStreamKey, streamID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted) // 202 Accepted is good for async jobs
	json.NewEncoder(w).Encode(map[string]string{
		"message":    "Job submitted successfully",
		"job_id":     jobReq.JobID,
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