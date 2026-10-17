-- Coordinated upgrade required: drain old publishers/consumers before migration.
-- Preserve batch and receipt dates; only the clock's representation changes.
DO $$
DECLARE
    previous_date DATE;
BEGIN
    IF NOT pg_try_advisory_xact_lock(64019, 0) THEN
        RAISE EXCEPTION 'Stop Camel before migrating business-date semantics';
    END IF;
    SELECT control_date INTO previous_date FROM camel_ingest.clock WHERE singleton FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Missing Camel clock singleton';
    END IF;
    IF previous_date IS NULL THEN
        IF EXISTS (SELECT 1 FROM camel_ingest.batch) THEN
            RAISE EXCEPTION 'Uninitialized Camel clock has existing batches; reconcile before migration';
        END IF;
    ELSE
        IF previous_date < DATE '0001-01-01' OR previous_date >= DATE '9999-12-31' THEN
            RAISE EXCEPTION 'Legacy Camel clock is outside the supported range';
        END IF;
        IF (SELECT count(*) FROM camel_ingest.batch WHERE NOT closed) <> 1
           OR NOT EXISTS (SELECT 1 FROM camel_ingest.batch
                          WHERE batch_date = previous_date + 1 AND NOT closed) THEN
            RAISE EXCEPTION 'Legacy Camel clock does not match its open batch; reconcile before migration';
        END IF;
    END IF;
END $$;

ALTER TABLE camel_ingest.clock RENAME COLUMN control_date TO active_date;
UPDATE camel_ingest.clock SET active_date = active_date + 1 WHERE active_date IS NOT NULL;
COMMENT ON COLUMN camel_ingest.clock.active_date IS
    'Active business date supplied by system.batch-date; NULL until the first clock event. Migration 008 preserves the legacy open batch date.';
