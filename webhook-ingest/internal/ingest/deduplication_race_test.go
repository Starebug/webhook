package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// TestConcurrentDuplicateWebhooksRaceCondition demonstrates Issue #3:
// The check-then-act race in EventExists() + InsertEvent() allows duplicate
// events to be stored when the same event_id is posted concurrently.
//
// This test WILL FAIL (or at least show flaky behavior) because:
// 1. Two requests with same event_id arrive concurrently
// 2. Both check EventExists() and get false (race window)
// 3. Both proceed to InsertEvent()
// 4. Both insert succeeds (Issue #4: no unique constraint)
// 5. Result: duplicate events in database, double-counted stats
func TestConcurrentDuplicateWebhooksRaceCondition(t *testing.T) {
	srv, st := testutil.NewServer(t)
	_, _, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	// Same event_id will be sent concurrently
	eventID := "evt_concurrent_test"
	callID := "call_concurrent_test"

	body := fmt.Sprintf(`{
		"event_id":      %q,
		"call_id":       %q,
		"account_id":    %q,
		"status":        "completed",
		"duration_sec":  100,
		"recording_url": "https://recordings.example.com/test.wav",
		"occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID)

	// Number of concurrent requests with the same event_id
	numConcurrent := 10

	var wg sync.WaitGroup
	wg.Add(numConcurrent)

	// Channel to collect responses
	responses := make(chan int, numConcurrent)

	// Send the same event concurrently
	for i := 0; i < numConcurrent; i++ {
		go func(n int) {
			defer wg.Done()

			resp, err := http.Post(srv.URL+"/webhooks/calls",
				"application/json", strings.NewReader(body))
			if err != nil {
				t.Logf("request %d failed: %v", n, err)
				return
			}
			defer resp.Body.Close()

			responses <- resp.StatusCode
		}(i)
	}

	wg.Wait()
	close(responses)

	// All requests should succeed (200 OK)
	successCount := 0
	for code := range responses {
		if code == http.StatusOK {
			successCount++
		}
	}

	t.Logf("Successful requests: %d/%d", successCount, numConcurrent)

	// Check how many times the event was actually stored
	var eventCount int
	row := st.Pool().QueryRow(ctx,
		`SELECT COUNT(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&eventCount); err != nil {
		t.Fatalf("query event count: %v", err)
	}

	if eventCount != 1 {
		t.Errorf("stored %d copies of event %q, want exactly 1 (check-then-act race allowed duplicates)",
			eventCount, eventID)
	}

	// Check account stats - should only be incremented once
	var callCount, totalDuration int64
	row = st.Pool().QueryRow(ctx,
		`SELECT call_count, total_duration_sec FROM account_stats WHERE account_id = $1`,
		accountID)
	if err := row.Scan(&callCount, &totalDuration); err != nil {
		t.Fatalf("query account stats: %v", err)
	}

	if callCount != 1 {
		t.Errorf("account call_count = %d, want 1 (duplicate events caused double-counting)",
			callCount)
	}

	if totalDuration != 100 {
		t.Errorf("account total_duration_sec = %d, want 100 (duplicate events caused double-counting)",
			totalDuration)
	}
}

// TestConcurrentDuplicateDeliveriesWithDelay adds deliberate timing to
// maximize the chance of hitting the race window.
func TestConcurrentDuplicateDeliveriesWithDelay(t *testing.T) {
	srv, st := testutil.NewServer(t)
	_, _, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	eventID := "evt_delayed_race"
	callID := "call_delayed_race"

	body := fmt.Sprintf(`{
		"event_id":      %q,
		"call_id":       %q,
		"account_id":    %q,
		"status":        "completed",
		"duration_sec":  50,
		"occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID)

	// Send two requests with a tiny delay to hit the race window
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		resp, _ := http.Post(srv.URL+"/webhooks/calls",
			"application/json", strings.NewReader(body))
		if resp != nil {
			resp.Body.Close()
		}
	}()

	// Tiny delay to ensure first request starts EventExists check
	time.Sleep(1 * time.Millisecond)

	go func() {
		defer wg.Done()
		resp, _ := http.Post(srv.URL+"/webhooks/calls",
			"application/json", strings.NewReader(body))
		if resp != nil {
			resp.Body.Close()
		}
	}()

	wg.Wait()

	// Check for duplicates
	var eventCount int
	row := st.Pool().QueryRow(ctx,
		`SELECT COUNT(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&eventCount); err != nil {
		t.Fatalf("query: %v", err)
	}

	if eventCount > 1 {
		t.Logf("Race condition triggered: %d duplicate events stored", eventCount)
	}

	if eventCount != 1 {
		t.Errorf("stored %d events, want 1", eventCount)
	}
}

// TestSequentialDuplicatesAreProperlyIgnored verifies that the EXISTING
// duplicate detection works when requests are sequential (not concurrent).
//
// This test should PASS even with the current code, demonstrating that
// the bug only manifests under concurrent load.
func TestSequentialDuplicatesAreProperlyIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := fmt.Sprintf(`{
		"event_id":      %q,
		"call_id":       %q,
		"account_id":    %q,
		"status":        "completed",
		"duration_sec":  25,
		"occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID)

	// Send the same event 5 times sequentially
	for i := 0; i < 5; i++ {
		resp, err := http.Post(srv.URL+"/webhooks/calls",
			"application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: got status %d, want 200", i, resp.StatusCode)
		}
	}

	// Should only have one event stored
	var eventCount int
	row := st.Pool().QueryRow(ctx,
		`SELECT COUNT(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&eventCount); err != nil {
		t.Fatalf("query: %v", err)
	}

	if eventCount != 1 {
		t.Errorf("stored %d events, want 1 (sequential deduplication should work)",
			eventCount)
	}

	// Stats should only be incremented once
	var callCount int64
	row = st.Pool().QueryRow(ctx,
		`SELECT call_count FROM account_stats WHERE account_id = $1`, accountID)
	if err := row.Scan(&callCount); err != nil {
		t.Fatalf("query stats: %v", err)
	}

	if callCount != 1 {
		t.Errorf("call_count = %d, want 1", callCount)
	}
}

// TestHighConcurrencyDuplicateStorm stress tests the race condition
// with many concurrent duplicate requests.
func TestHighConcurrencyDuplicateStorm(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	srv, st := testutil.NewServer(t)
	_, _, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	eventID := "evt_stress_test"
	callID := "call_stress_test"

	body := fmt.Sprintf(`{
		"event_id":      %q,
		"call_id":       %q,
		"account_id":    %q,
		"status":        "completed",
		"duration_sec":  75,
		"occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID)

	// Send 100 concurrent requests with the same event_id
	numRequests := 100

	var wg sync.WaitGroup
	wg.Add(numRequests)

	startTime := time.Now()

	for i := 0; i < numRequests; i++ {
		go func() {
			defer wg.Done()
			resp, err := http.Post(srv.URL+"/webhooks/calls",
				"application/json", strings.NewReader(body))
			if err == nil && resp != nil {
				resp.Body.Close()
			}
		}()
	}

	wg.Wait()
	duration := time.Since(startTime)

	t.Logf("Completed %d concurrent requests in %v", numRequests, duration)

	// Check how many duplicates made it through
	var eventCount int
	row := st.Pool().QueryRow(ctx,
		`SELECT COUNT(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&eventCount); err != nil {
		t.Fatalf("query: %v", err)
	}

	if eventCount != 1 {
		t.Errorf("stored %d events, want 1 (race allowed %d duplicates through)",
			eventCount, eventCount-1)
		t.Logf("Race window exploitation rate: %.2f%%",
			float64(eventCount-1)/float64(numRequests)*100)
	}
}
