-- Add unique constraint on event_id to prevent duplicate events
-- This enables idempotent webhook delivery handling

-- Drop the existing non-unique index
DROP INDEX IF EXISTS idx_events_event_id;

-- Create a unique index instead
CREATE UNIQUE INDEX idx_events_event_id_unique ON events (event_id);
