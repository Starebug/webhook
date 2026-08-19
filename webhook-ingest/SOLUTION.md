# Solution

## What Was Broken, and Why

**1. Race Condition in Stats Cache** (`stats/cache.go:39`)
`Record()` modified shared map without locking while `Get()` used `RLock()`. Caused lost updates and count drift under concurrent load.
**Fix:** Added `Lock()/Unlock()` in `Record()`. Made value copy in `Get()` while lock held.

**2. Context Cancellation in Goroutines** (`ingest/service.go:78`)
Background recording processing used HTTP request context, cancelled immediately after response. Recordings never marked processed, errors silently ignored.
**Fix:** Use `context.Background()` for background work, log errors properly.

**3. Check-Then-Act Race in Deduplication** (`ingest/service.go:40`)
TOCTOU vulnerability: `EventExists()` check separated from `InsertEvent()`. Concurrent requests with same `event_id` both inserted, double-counting stats.
**Fix:** Atomic insert using unique constraint + `ON CONFLICT` (see #4).

**4. Missing Unique Constraint** (`migrations/001_init.sql:10`)
`event_id` had index but no uniqueness enforcement. Database allowed duplicates.
**Fix:** Created `002_unique_event_id.sql` with unique index. Updated `InsertEvent()` to use `ON CONFLICT DO NOTHING`, returning insert status.

**5. Cache-Only Stats Endpoint** (`cmd/server/main.go:42`)
`/accounts/{id}/stats` read only from cache, which started empty on deployment. Made `account_stats` table write-only.
**Fix:** Implemented read-through cache (loads from DB on miss) + warm-up on startup.

All tests added to demonstrate each bug before fixing: `cache_race_test.go`, `context_cancellation_test.go`, `deduplication_race_test.go`, `unique_constraint_test.go`, `stats_idempotency_test.go`.

---

## Deduplication Strategy

**Chosen:** Database unique constraint + `ON CONFLICT DO NOTHING`

```sql
CREATE UNIQUE INDEX idx_events_event_id_unique ON events (event_id);
INSERT ... ON CONFLICT (event_id) DO NOTHING
```

**Why:**
- **Atomic** - Database guarantees no race window
- **Durable** - Survives restarts, works across multiple instances
- **Simple** - Single operation, no coordination needed
- **Idempotent** - Returns `RowsAffected()` to detect duplicates

**Alternatives considered:**
- **Redis SET NX** - Not durable across Redis restarts, TTL management complex
- **Application locks** - Doesn't scale horizontally
- **Distributed locks** - Over-engineered, adds latency
- **Bloom filter** - False positives, doesn't prevent actual duplicates

The provider guarantees at-least-once delivery with stable `event_id`. Database unique constraint perfectly maps to this: first delivery inserts and increments stats; retries return 0 rows affected, stats unchanged.

---

## Scaling to 10,000 Webhooks/Second

**Current bottleneck:** ~200 req/sec (3 synchronous DB writes per request)

**Changes for 10k/sec:**

**1. Async Queue** (Redis Streams/Kafka)
Return 200 immediately, workers process async. Enables horizontal scaling, batch writes.
Trade-off: Eventually consistent stats.

**2. Batch Writes**
Buffer events, flush every 100ms or 1000 events using `COPY` protocol.
10x throughput improvement.

**3. CQRS Pattern**
Write path: Webhooks → Queue → Workers → Postgres
Read path: API → Redis cache (updated by workers)
Stats endpoint never hits Postgres.

**4. Database Optimization**
- Partition `events` by `account_id` or time
- Connection pool: 100+ connections
- Only essential indexes

**5. Horizontal Scaling**
Stateless instances behind load balancer. Unique constraint prevents duplicates across instances.

**Capacity estimates:**
- Current: ~200 req/sec
- With batching: ~2,000 req/sec
- With queue + CQRS: ~10,000+ req/sec

**Progressive approach:**
- 1-2k/sec: Connection pooling + horizontal instances
- 5k/sec: Add async queue
- 10k+/sec: Full CQRS with event sourcing
