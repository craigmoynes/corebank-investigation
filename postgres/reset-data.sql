-- Development reset: deletes all application data and Spring Batch job history.
-- Stop core and Camel commands and other database writers before running this script.
-- Camel broker/group offsets must be reset consistently before replay; this SQL does not reset Kafka.
-- Requires migrations through 007. Liquibase history and schema definitions remain.
-- Input/output/error files on disk are not removed: move old output files aside
-- before reusing batch dates, or the exporter may reject conflicting contents.
-- CASCADE also truncates any additional tables referencing the listed tables.

BEGIN;

-- Children precede parents for readability. PostgreSQL truncates this entire
-- related set in one statement; separate child-first statements are insufficient
-- to satisfy TRUNCATE's foreign-key requirements without CASCADE.
TRUNCATE TABLE
    camel_ingest.record,
    camel_ingest.receipt,
    camel_ingest.batch,
    camel_ingest.publication,
    -- Spring Batch execution history.
    core_batch.batch_step_execution_context,
    core_batch.batch_job_execution_context,
    core_batch.batch_job_execution_params,
    core_batch.batch_step_execution,
    core_batch.batch_job_execution,
    core_batch.batch_job_instance,

    -- Historical output, then operational dependents and their parents.
    core_output.balance,
    core_output.batch_snapshot,
    core.transaction,
    core.relationship,
    core.balance,
    core.account,
    core.customer,
    core.batch_run,

    -- Staging tables have no foreign-key dependencies on the operational tables.
    core_ingest.transaction,
    core_ingest.relationship,
    core_ingest.account,
    core_ingest.customer,
    core_ingest.batch_receipt
RESTART IDENTITY CASCADE;

-- Spring Batch uses standalone sequences, not table-owned identity sequences.
ALTER SEQUENCE core_batch.batch_step_execution_seq RESTART WITH 1;
ALTER SEQUENCE core_batch.batch_job_execution_seq RESTART WITH 1;
ALTER SEQUENCE core_batch.batch_job_seq RESTART WITH 1;

UPDATE camel_ingest.clock SET control_date=NULL, updated_at=now();

COMMIT;
