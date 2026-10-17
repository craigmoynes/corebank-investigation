-- Stop publishers, Camel and core before upgrading. Legacy dates preserve the
-- previous batch-date interpretation; no historical real-world date is inferred.
DO $$ BEGIN
    IF NOT pg_try_advisory_xact_lock(64019,0) THEN
        RAISE EXCEPTION 'Stop Camel before migrating transaction effective dates';
    END IF;
    IF NOT pg_try_advisory_xact_lock(1129271881,0)
       OR NOT pg_try_advisory_xact_lock(1129271880,0) THEN
        RAISE EXCEPTION 'Stop core watcher and batch jobs before migrating transaction effective dates';
    END IF;
END $$;

ALTER TABLE core_ingest.transaction ADD COLUMN effective_date DATE;
UPDATE core_ingest.transaction SET effective_date=batch_date;
ALTER TABLE core_ingest.transaction ALTER COLUMN effective_date SET NOT NULL;
ALTER TABLE core_ingest.transaction ADD CONSTRAINT ingest_transaction_effective_date_range
    CHECK (effective_date BETWEEN DATE '0001-01-01' AND DATE '9999-12-31');

ALTER TABLE core.transaction ADD COLUMN effective_date DATE;
-- This one-time backfill preserves ledger amounts, identities, and audit times.
ALTER TABLE core.transaction DISABLE TRIGGER transaction_immutable;
UPDATE core.transaction SET effective_date=batch_date;
ALTER TABLE core.transaction ENABLE TRIGGER transaction_immutable;
ALTER TABLE core.transaction ALTER COLUMN effective_date SET NOT NULL;
ALTER TABLE core.transaction ADD CONSTRAINT transaction_effective_date_range
    CHECK (effective_date BETWEEN DATE '0001-01-01' AND DATE '9999-12-31');

CREATE OR REPLACE FUNCTION core.validate_transaction_account() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    account_start DATE;
    account_end DATE;
BEGIN
    SELECT start_date,end_date INTO account_start,account_end FROM core.account
        WHERE account_id=NEW.account_id FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown account %', NEW.account_id USING ERRCODE='23503';
    END IF;
    IF account_end IS NOT NULL THEN
        RAISE EXCEPTION 'Transactions are not permitted on end-dated account %', NEW.account_id
            USING ERRCODE='23514';
    END IF;
    IF NEW.effective_date < account_start THEN
        RAISE EXCEPTION 'Transaction effective date precedes account start date for %', NEW.account_id
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;

-- Preserve extracted records to keep existing file comparisons stable. Pending
-- Camel records use the legacy batch date until explicitly reconciled by operators.
UPDATE camel_ingest.record r SET fixed_line=r.fixed_line || to_char(r.batch_date,'YYYY-MM-DD')
FROM camel_ingest.batch b
WHERE r.batch_date=b.batch_date AND NOT b.extracted
  AND r.kind='transaction' AND char_length(r.fixed_line)=64;
