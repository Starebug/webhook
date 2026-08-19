package stats_test

import (
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

// TestCacheConcurrentRecordRaceCondition demonstrates Issue #1:
// Race condition in Cache.Record() method when called concurrently.
//
// This test WILL FAIL with -race flag because Record() doesn't use mutex locking.
// Run with: go test -race -run TestCacheConcurrentRecordRaceCondition
//
// Expected behavior: 100 goroutines each recording 100 calls should result in
// exactly 10,000 calls counted. Without proper locking, we get lost updates
// and the count drifts.
func TestCacheConcurrentRecordRaceCondition(t *testing.T) {
	c := stats.NewCache()
	accountID := "acc_test"

	// Number of concurrent goroutines
	numGoroutines := 100
	// Number of calls each goroutine records
	callsPerGoroutine := 100
	// Duration for each call
	durationPerCall := 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Launch concurrent goroutines that all record to the same account
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < callsPerGoroutine; j++ {
				c.Record(accountID, durationPerCall)
			}
		}()
	}

	wg.Wait()

	// Verify the final counts
	got := c.Get(accountID)
	expectedCalls := int64(numGoroutines * callsPerGoroutine)
	expectedDuration := int64(numGoroutines * callsPerGoroutine * durationPerCall)

	if got.CallCount != expectedCalls {
		t.Errorf("CallCount: got %d, want %d (lost %d updates due to race)",
			got.CallCount, expectedCalls, expectedCalls-got.CallCount)
	}

	if got.TotalDurationSec != expectedDuration {
		t.Errorf("TotalDurationSec: got %d, want %d (lost %d seconds due to race)",
			got.TotalDurationSec, expectedDuration, expectedDuration-got.TotalDurationSec)
	}
}

// TestCacheConcurrentRecordMultipleAccounts demonstrates the race condition
// affects the map itself, not just individual account stats.
//
// This test shows that concurrent Record() calls can corrupt the map structure,
// potentially causing missing entries or panics.
func TestCacheConcurrentRecordMultipleAccounts(t *testing.T) {
	c := stats.NewCache()

	numAccounts := 50
	recordsPerAccount := 100

	var wg sync.WaitGroup
	wg.Add(numAccounts)

	// Each goroutine records to a different account concurrently
	for i := 0; i < numAccounts; i++ {
		accountID := i
		go func(id int) {
			defer wg.Done()
			for j := 0; j < recordsPerAccount; j++ {
				c.Record(accountIDForNum(id), 5)
			}
		}(accountID)
	}

	wg.Wait()

	// Verify all accounts were recorded
	for i := 0; i < numAccounts; i++ {
		got := c.Get(accountIDForNum(i))
		if got.CallCount != int64(recordsPerAccount) {
			t.Errorf("Account %d: got %d calls, want %d",
				i, got.CallCount, recordsPerAccount)
		}
		if got.TotalDurationSec != int64(recordsPerAccount*5) {
			t.Errorf("Account %d: got %d duration, want %d",
				i, got.TotalDurationSec, recordsPerAccount*5)
		}
	}
}

// TestCacheConcurrentReadWriteRace demonstrates that concurrent Get() and Record()
// operations race on the same account.
//
// While Get() uses RLock, Record() doesn't use Lock, causing a race.
func TestCacheConcurrentReadWriteRace(t *testing.T) {
	c := stats.NewCache()
	accountID := "acc_readwrite"

	// Pre-populate the account
	c.Record(accountID, 10)

	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 1000; i++ {
			c.Record(accountID, 1)
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 1000; i++ {
			_ = c.Get(accountID)
		}
		done <- true
	}()

	// Wait for both to complete
	<-done
	<-done

	// The test passing doesn't mean there's no race
	// Run with -race flag to detect the data race
	got := c.Get(accountID)
	if got.CallCount < 1 {
		t.Errorf("Expected at least 1 call, got %d", got.CallCount)
	}
}

func accountIDForNum(n int) string {
	return "acc_" + string(rune('A'+n))
}
