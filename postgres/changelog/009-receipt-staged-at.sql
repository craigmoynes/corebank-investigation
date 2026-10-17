-- Historical receipt times are unknown; do not backfill them with migration time.
ALTER TABLE camel_ingest.receipt ADD COLUMN staged_at TIMESTAMPTZ;
ALTER TABLE camel_ingest.receipt ALTER COLUMN staged_at SET DEFAULT clock_timestamp();
COMMENT ON COLUMN camel_ingest.receipt.staged_at IS
    'Database time when the first successful receipt was inserted, after the clock lock; not Kafka arrival or commit time. NULL for pre-migration receipts; preserved on replay.';
