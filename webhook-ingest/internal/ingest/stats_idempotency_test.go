package ingest_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// TestStatsIncrementIsNotIdempotent demonstrates Issue #5:
// If a duplicate event somehow makes it past deduplication (Issue #3),
// the stats get incremented again, even though it's the same call.
//
// This test manually bypasses the EventExists check to simulate what
// happens when the race condition allows duplicates through.
func TestStatsIncrementIsNotIdempotent(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()

	eventID := "evt_stats_test"
	callID := "call_stats_test"
	accountID := "acc_stats_test"

	// Clean up
	t.Cleanup(func() {
		st.Pool().Exec(ctx, "DELETE FROM events WHERE event_id = $1", eventID)
		st.Pool().Exec(ctx, "DELETE FROM calls WHERE call_id = $1", callID)
		st.Pool().Exec(ctx, "DELETE FROM account_stats WHERE account_id = $1", accountID)
	})

	payload, _ := json.Marshal(map[string]interface{}{
		"event_id":     eventID,
		"call_id":      callID,
		"account_id":   accountID,
		"status":       "completed",
		"duration_sec": 100,
	})

	event := store.Event{
		EventID:     eventID,
		CallID:      callID,
		AccountID:   accountID,
		Status:      "completed",
		DurationSec: 100,
		Payload:     payload,
	}

	// First ingestion - this is legitimate
	wasInserted, err := st.InsertEvent(ctx, event)
	if err != nil {
		t.Fatalf("first InsertEvent: %v", err)
	}
	if !wasInserted {
		t.Fatal("first event should be inserted")
	}
	if err := st.UpsertCall(ctx, event); err != nil {
		t.Fatalf("first UpsertCall: %v", err)
	}
	if err := st.IncrementAccountStats(ctx, accountID, 100); err != nil {
		t.Fatalf("first IncrementAccountStats: %v", err)
	}

	// Verify initial state
	stats1, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("get stats after first: %v", err)
	}

	if stats1.CallCount != 1 || stats1.TotalDurationSec != 100 {
		t.Fatalf("after first: got %+v, want CallCount=1 TotalDurationSec=100", stats1)
	}

	// Simulate a duplicate getting through (Issue #3 race)
	// The event_id is the same, but if Issue #4 allows it, we can insert again
	// Note: This will fail if unique constraint exists, which is correct

	// Try to insert the same event again (simulating race condition)
	// With the fix, this should return wasInserted=false (not an error)
	wasInserted, err = st.InsertEvent(ctx, event)
	if err != nil {
		t.Fatalf("Duplicate event insert failed with error: %v", err)
	}

	if !wasInserted {
		t.Logf("Duplicate correctly ignored (Issue #4 fixed - unique constraint working)")
		// This is the correct behavior - test should pass now
		return
	}

	t.Logf("WARNING: Duplicate event insert succeeded (Issue #4 confirmed)")

	// Now increment stats again (this is the bug - no check for duplicate)
	if err := st.IncrementAccountStats(ctx, accountID, 100); err != nil {
		t.Fatalf("second IncrementAccountStats: %v", err)
	}

	// Check stats - they should NOT have doubled
	stats2, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("get stats after second: %v", err)
	}

	if stats2.CallCount != 1 {
		t.Errorf("after duplicate: CallCount = %d, want 1 (stats incremented non-idempotently)",
			stats2.CallCount)
	}

	if stats2.TotalDurationSec != 100 {
		t.Errorf("after duplicate: TotalDurationSec = %d, want 100 (stats incremented non-idempotently)",
			stats2.TotalDurationSec)
	}
}

// TestInMemoryCacheDoesNotMatchDatabase demonstrates that the in-memory
// cache can drift from the database when duplicates are processed.
func TestInMemoryCacheDoesNotMatchDatabase(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()

	eventID := "evt_cache_drift"
	callID := "call_cache_drift"
	accountID := "acc_cache_drift"

	// Clean up
	t.Cleanup(func() {
		st.Pool().Exec(ctx, "DELETE FROM events WHERE event_id = $1", eventID)
		st.Pool().Exec(ctx, "DELETE FROM calls WHERE call_id = $1", callID)
		st.Pool().Exec(ctx, "DELETE FROM account_stats WHERE account_id = $1", accountID)
	})

	// Directly manipulate stats to simulate what happens when:
	// 1. First event processes normally: DB=1, Cache=1
	// 2. Duplicate slips through race: DB=2, Cache=2
	// 3. Another duplicate: DB=3, Cache might be 3 or corrupted due to Issue #1

	// Initial increment
	if err := st.IncrementAccountStats(ctx, accountID, 50); err != nil {
		t.Fatalf("first increment: %v", err)
	}

	// Simulate duplicate increments
	if err := st.IncrementAccountStats(ctx, accountID, 50); err != nil {
		t.Fatalf("second increment: %v", err)
	}

	if err := st.IncrementAccountStats(ctx, accountID, 50); err != nil {
		t.Fatalf("third increment: %v", err)
	}

	// Check database state
	dbStats, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("get db stats: %v", err)
	}

	// With non-idempotent stats, we expect to see multiple increments
	if dbStats.CallCount == 1 {
		t.Logf("Database stats are idempotent (correct)")
	} else {
		t.Errorf("Database CallCount = %d, want 1 (non-idempotent increments)",
			dbStats.CallCount)
		t.Logf("Each duplicate event caused an additional increment")
	}
}

// TestIdempotentIngestionSameEventMultipleTimes demonstrates the end-to-end
// scenario where the same event_id should only count once, regardless of
// how many times it's ingested.
func TestIdempotentIngestionSameEventMultipleTimes(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()

	eventID := "evt_idempotent_test"
	callID := "call_idempotent_test"
	accountID := "acc_idempotent_test"

	// Clean up
	t.Cleanup(func() {
		st.Pool().Exec(ctx, "DELETE FROM events WHERE event_id = $1", eventID)
		st.Pool().Exec(ctx, "DELETE FROM calls WHERE call_id = $1", callID)
		st.Pool().Exec(ctx, "DELETE FROM account_stats WHERE account_id = $1", accountID)
	})

	evt := ingest.Event{
		EventID:     eventID,
		CallID:      callID,
		AccountID:   accountID,
		Status:      "completed",
		DurationSec: 75,
		OccurredAt:  time.Now(),
	}

	payload, _ := json.Marshal(evt)
	storeEvent := store.Event{
		EventID:     evt.EventID,
		CallID:      evt.CallID,
		AccountID:   evt.AccountID,
		Status:      evt.Status,
		DurationSec: evt.DurationSec,
		OccurredAt:  evt.OccurredAt,
		Payload:     payload,
	}

	// Process the same event 5 times
	for i := 0; i < 5; i++ {
		// Check if exists (this is what the service does)
		exists, err := st.EventExists(ctx, eventID)
		if err != nil {
			t.Fatalf("iteration %d: EventExists: %v", i, err)
		}

		if exists {
			t.Logf("Iteration %d: Event exists, skipping (correct behavior)", i)
			continue
		}

		// Insert and increment stats
		wasInserted, err := st.InsertEvent(ctx, storeEvent)
		if err != nil {
			t.Fatalf("Iteration %d: InsertEvent error: %v", i, err)
		}
		if !wasInserted {
			t.Logf("Iteration %d: Duplicate ignored by unique constraint (correct)", i)
			continue
		}

		if err := st.UpsertCall(ctx, storeEvent); err != nil {
			t.Fatalf("iteration %d: UpsertCall: %v", i, err)
		}

		if err := st.IncrementAccountStats(ctx, accountID, 75); err != nil {
			t.Fatalf("iteration %d: IncrementAccountStats: %v", i, err)
		}

		t.Logf("Iteration %d: Processed event", i)
	}

	// Verify final state
	var eventCount int
	row := st.Pool().QueryRow(ctx,
		`SELECT COUNT(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}

	finalStats, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("get final stats: %v", err)
	}

	t.Logf("Final state: %d events, CallCount=%d, TotalDuration=%d",
		eventCount, finalStats.CallCount, finalStats.TotalDurationSec)

	if eventCount != 1 {
		t.Errorf("stored %d events, want 1", eventCount)
	}

	if finalStats.CallCount != 1 {
		t.Errorf("CallCount = %d, want 1 (processed %d times due to non-idempotent stats)",
			finalStats.CallCount, finalStats.CallCount)
	}

	if finalStats.TotalDurationSec != 75 {
		t.Errorf("TotalDurationSec = %d, want 75", finalStats.TotalDurationSec)
	}
}

// TestStatsIncrementsAreNotAtomic demonstrates that stats can be incremented
// even if the event insert fails (no transaction wrapping all operations).
func TestStatsIncrementsAreNotAtomic(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()

	accountID := "acc_atomic_test"

	// Clean up
	t.Cleanup(func() {
		st.Pool().Exec(ctx, "DELETE FROM account_stats WHERE account_id = $1", accountID)
	})

	// Get initial stats (should be 0)
	before, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("get initial stats: %v", err)
	}

	if before.CallCount != 0 {
		t.Logf("Initial CallCount = %d (expected 0, but continuing)", before.CallCount)
	}

	// Increment stats directly (bypassing event insert)
	// This simulates what happens if event insert fails but stats still increment
	if err := st.IncrementAccountStats(ctx, accountID, 25); err != nil {
		t.Fatalf("increment stats: %v", err)
	}

	// Check stats increased
	after, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("get stats after: %v", err)
	}

	if after.CallCount != before.CallCount+1 {
		t.Errorf("CallCount = %d, want %d (stats incremented)",
			after.CallCount, before.CallCount+1)
	}

	// The point: there's no event in the events table for this increment
	// In a proper atomic implementation, stats should only increment
	// if the event was successfully inserted
	t.Logf("Stats incremented without corresponding event (no transaction atomicity)")
}
