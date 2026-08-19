package store_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// TestDatabaseAllowsDuplicateEventIDs demonstrates Issue #4:
// The events table has an index on event_id but NOT a unique constraint,
// allowing duplicate event_id values to be inserted.
//
// This test WILL FAIL (both inserts succeed) because the database schema
// does not enforce uniqueness on event_id.
//
// Expected: Second insert should fail with unique constraint violation
// Actual: Both inserts succeed, creating duplicate event_id rows
func TestDatabaseAllowsDuplicateEventIDs(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()

	eventID := "evt_duplicate_test"
	callID1 := "call_dup_1"
	callID2 := "call_dup_2"
	accountID := "acc_duplicate_test"

	// Clean up test data
	t.Cleanup(func() {
		st.Pool().Exec(ctx, "DELETE FROM events WHERE event_id = $1", eventID)
		st.Pool().Exec(ctx, "DELETE FROM calls WHERE account_id = $1", accountID)
		st.Pool().Exec(ctx, "DELETE FROM account_stats WHERE account_id = $1", accountID)
	})

	// Create first event
	event1 := store.Event{
		EventID:   eventID, // Same event_id
		CallID:    callID1,
		AccountID: accountID,
		Payload:   []byte(`{"test": 1}`),
	}

	wasInserted, err := st.InsertEvent(ctx, event1)
	if err != nil {
		t.Fatalf("first insert failed: %v", err)
	}
	if !wasInserted {
		t.Fatal("first event should be inserted")
	}

	// Try to insert second event with the SAME event_id but different call_id
	event2 := store.Event{
		EventID:   eventID, // Same event_id (should be rejected)
		CallID:    callID2, // Different call_id
		AccountID: accountID,
		Payload:   []byte(`{"test": 2}`),
	}

	wasInserted, err = st.InsertEvent(ctx, event2)
	if err != nil {
		t.Fatalf("Second insert returned error: %v", err)
	}

	if !wasInserted {
		t.Logf("Second insert correctly ignored (unique constraint working)")
		// This is what we WANT to happen - test passes
		return
	}

	// If we get here, the second insert succeeded (BUG!)
	t.Errorf("Second insert succeeded - database allowed duplicate event_id %q", eventID)

	// Verify that we actually have duplicates
	var count int
	row := st.Pool().QueryRow(ctx,
		`SELECT COUNT(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("query count: %v", err)
	}

	t.Logf("Found %d rows with event_id %q in database", count, eventID)

	if count > 1 {
		t.Errorf("Database contains %d duplicate rows with event_id %q (missing UNIQUE constraint)",
			count, eventID)
	}
}

// TestDirectDuplicateInsertViaSQL bypasses the application layer and
// directly inserts duplicates via SQL to prove the schema allows it.
func TestDirectDuplicateInsertViaSQL(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()

	eventID := "evt_sql_duplicate"
	accountID := "acc_sql_test"

	// Clean up
	t.Cleanup(func() {
		st.Pool().Exec(ctx, "DELETE FROM events WHERE event_id = $1", eventID)
		st.Pool().Exec(ctx, "DELETE FROM account_stats WHERE account_id = $1", accountID)
	})

	// Insert first row directly
	_, err := st.Pool().Exec(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)`,
		eventID, "call_1", accountID, []byte(`{}`))
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}

	// Try to insert duplicate directly
	_, err = st.Pool().Exec(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)`,
		eventID, "call_2", accountID, []byte(`{}`))

	if err != nil {
		t.Logf("Duplicate insert rejected: %v", err)
		// This would be correct behavior
		return
	}

	// If we got here, duplicate was allowed
	t.Errorf("Direct SQL insert allowed duplicate event_id %q (no UNIQUE constraint)", eventID)
}

// TestIndexExistsButNotUnique verifies that an index exists on event_id,
// but confirms it's not a unique index.
func TestIndexExistsButNotUnique(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()

	// Query to check if a unique constraint exists on event_id
	var constraintName string
	row := st.Pool().QueryRow(ctx, `
		SELECT constraint_name
		FROM information_schema.table_constraints
		WHERE table_name = 'events'
		  AND constraint_type = 'UNIQUE'
		  AND constraint_name LIKE '%event_id%'
	`)

	err := row.Scan(&constraintName)
	if err == nil {
		t.Logf("Found unique constraint: %s", constraintName)
		// This is what we WANT
		return
	}

	// Check if a regular (non-unique) index exists
	var indexName string
	row = st.Pool().QueryRow(ctx, `
		SELECT indexname
		FROM pg_indexes
		WHERE tablename = 'events'
		  AND indexname LIKE '%event_id%'
	`)

	err = row.Scan(&indexName)
	if err != nil {
		t.Fatalf("No index found on event_id at all: %v", err)
	}

	t.Logf("Found non-unique index: %s", indexName)

	// Verify it's not unique by checking pg_index
	var isUnique bool
	row = st.Pool().QueryRow(ctx, `
		SELECT i.indisunique
		FROM pg_class t
		JOIN pg_index i ON t.oid = i.indrelid
		JOIN pg_class idx ON i.indexrelid = idx.oid
		WHERE t.relname = 'events'
		  AND idx.relname = $1
	`, indexName)

	if err := row.Scan(&isUnique); err != nil {
		t.Fatalf("query index uniqueness: %v", err)
	}

	if isUnique {
		t.Logf("Index %s IS unique (correct)", indexName)
	} else {
		t.Errorf("Index %s is NOT unique - this allows duplicate event_id values", indexName)
	}
}

// TestMultipleDuplicateInsertsInTransaction demonstrates that even within
// a transaction, multiple duplicate event_ids can be inserted.
func TestMultipleDuplicateInsertsInTransaction(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()

	eventID := "evt_txn_dup"
	accountID := "acc_txn_test"

	// Clean up
	t.Cleanup(func() {
		st.Pool().Exec(ctx, "DELETE FROM events WHERE event_id = $1", eventID)
		st.Pool().Exec(ctx, "DELETE FROM account_stats WHERE account_id = $1", accountID)
	})

	// Start transaction
	tx, err := st.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	// Insert same event_id multiple times in one transaction
	for i := 1; i <= 3; i++ {
		_, err := tx.Exec(ctx,
			`INSERT INTO events (event_id, call_id, account_id, payload)
			 VALUES ($1, $2, $3, $4)`,
			eventID, fmt.Sprintf("call_%d", i), accountID, []byte(`{}`))

		if err != nil {
			t.Logf("Insert %d failed: %v (constraint working)", i, err)
			return // This is correct behavior
		}
	}

	// If all inserts succeeded, commit and verify
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var count int
	row := st.Pool().QueryRow(ctx,
		`SELECT COUNT(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}

	if count > 1 {
		t.Errorf("Transaction committed %d duplicate event_id rows (no UNIQUE constraint)",
			count)
	}
}

// TestOnConflictDoNothingWorks verifies that if we add "ON CONFLICT DO NOTHING",
// it will prevent errors but requires a unique constraint to exist first.
func TestOnConflictDoNothingRequiresUniqueConstraint(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()

	eventID := "evt_on_conflict"
	accountID := "acc_conflict_test"

	// Clean up
	t.Cleanup(func() {
		st.Pool().Exec(ctx, "DELETE FROM events WHERE event_id = $1", eventID)
		st.Pool().Exec(ctx, "DELETE FROM account_stats WHERE account_id = $1", accountID)
	})

	// First insert
	_, err := st.Pool().Exec(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)`,
		eventID, "call_1", accountID, []byte(`{}`))
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}

	// Try ON CONFLICT DO NOTHING (this will fail if no unique constraint exists)
	_, err = st.Pool().Exec(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (event_id) DO NOTHING`,
		eventID, "call_2", accountID, []byte(`{}`))

	if err != nil {
		t.Logf("ON CONFLICT failed: %v", err)
		t.Logf("This is expected because there's no unique constraint on event_id")
		// The error message should mention that there's no unique constraint
		if !strings.Contains(err.Error(), "constraint") &&
			!strings.Contains(err.Error(), "conflict") {
			t.Errorf("Error doesn't mention constraint issue: %v", err)
		}
	} else {
		t.Logf("ON CONFLICT DO NOTHING succeeded (unique constraint exists)")
	}
}

// Helper function to avoid import cycle
func strings_Contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
