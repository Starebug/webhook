package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// TestRecordingProcessingContextCancellation demonstrates Issue #2:
// Background goroutine uses request context which gets cancelled after response,
// preventing recordings from being marked as processed.
//
// This test WILL FAIL because the recording processing goroutine uses the HTTP
// request context, which is cancelled as soon as the handler returns.
//
// Expected: recording_processed should be TRUE after processing completes
// Actual: recording_processed remains FALSE because context is cancelled
func TestRecordingProcessingContextCancellation(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	// Send a webhook with a recording URL
	body := fmt.Sprintf(`{
		"event_id":      %q,
		"call_id":       %q,
		"account_id":    %q,
		"status":        "completed",
		"duration_sec":  60,
		"recording_url": "https://recordings.example.com/%s.wav",
		"occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)

	resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post webhook: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got status %d, want 200", resp.StatusCode)
	}

	// At this point, the HTTP request context is cancelled
	// The goroutine tries to use it for database operations and fails

	// Wait for recording processing (50ms work + buffer)
	time.Sleep(200 * time.Millisecond)

	// Check if recording was marked as processed
	var processed bool
	row := st.Pool().QueryRow(ctx,
		`SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("query recording_processed: %v", err)
	}

	if !processed {
		t.Errorf("recording_processed = false, want true (context was cancelled before DB update)")
	}
}

// TestMultipleWebhooksWithRecordings demonstrates that ALL recordings fail
// to be processed, not just one.
func TestMultipleWebhooksWithRecordingsNeverProcessed(t *testing.T) {
	srv, st := testutil.NewServer(t)
	_, _, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	numWebhooks := 5
	callIDs := make([]string, numWebhooks)

	// Send multiple webhooks with recording URLs
	for i := 0; i < numWebhooks; i++ {
		eventID := fmt.Sprintf("evt_test_%d", i)
		callID := fmt.Sprintf("call_test_%d", i)
		callIDs[i] = callID

		body := fmt.Sprintf(`{
			"event_id":      %q,
			"call_id":       %q,
			"account_id":    %q,
			"status":        "completed",
			"duration_sec":  30,
			"recording_url": "https://recordings.example.com/%s.wav",
			"occurred_at":   "2026-08-13T09:12:00Z"
		}`, eventID, callID, accountID, callID)

		resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("post webhook %d: %v", i, err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("webhook %d: got status %d, want 200", i, resp.StatusCode)
		}
	}

	// Wait for all recordings to process
	time.Sleep(200 * time.Millisecond)

	// Check how many recordings were processed
	var processedCount int
	row := st.Pool().QueryRow(ctx,
		`SELECT COUNT(*) FROM calls WHERE account_id = $1 AND recording_processed = TRUE`,
		accountID)
	if err := row.Scan(&processedCount); err != nil {
		t.Fatalf("query processed count: %v", err)
	}

	if processedCount != numWebhooks {
		t.Errorf("processed %d recordings, want %d (all contexts were cancelled)",
			processedCount, numWebhooks)
	}
}

// TestRecordingWithoutURLDoesNotHangOrPanic verifies that webhooks without
// recording URLs are handled correctly (no goroutine spawned).
func TestRecordingWithoutURLDoesNotHangOrPanic(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	// Send webhook without recording URL
	body := fmt.Sprintf(`{
		"event_id":      %q,
		"call_id":       %q,
		"account_id":    %q,
		"status":        "failed",
		"duration_sec":  0,
		"occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID)

	resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post webhook: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got status %d, want 200", resp.StatusCode)
	}

	// Verify call was stored
	var status string
	row := st.Pool().QueryRow(ctx,
		`SELECT status FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&status); err != nil {
		t.Fatalf("query call: %v", err)
	}

	if status != "failed" {
		t.Errorf("status = %q, want %q", status, "failed")
	}

	// recording_processed should be FALSE (not NULL, just never set to TRUE)
	var processed bool
	row = st.Pool().QueryRow(ctx,
		`SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("query recording_processed: %v", err)
	}

	if processed {
		t.Errorf("recording_processed = true, want false (no recording URL provided)")
	}
}