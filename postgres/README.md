# Local PostgreSQL

Uses the [official PostgreSQL image](https://hub.docker.com/_/postgres), pinned to major version 18 and the Bookworm image variant. Minor updates can be pulled within this major version; it is not an immutable digest pin. No custom Dockerfile is needed.

## Start

From the repository root:

```sh
cp docker/.env.example docker/.env
# Edit docker/.env and replace POSTGRES_PASSWORD.
docker compose --env-file docker/.env -f docker/compose.yaml up -d --wait postgres
```

Host connection: `localhost:5432`, database `corebank`, user `postgres`, with the password in `docker/.env`. Change `POSTGRES_PORT` if port 5432 is occupied. Future Compose services use `postgres:5432`.

This initial local setup uses the bootstrap administrator. Add separate restricted application roles when implementing core and Camel. The environment file is ignored by Git. Network access is bound to localhost.

## Inspect and stop

```sh
docker compose --env-file docker/.env -f docker/compose.yaml ps
docker compose --env-file docker/.env -f docker/compose.yaml logs postgres
docker compose --env-file docker/.env -f docker/compose.yaml exec postgres psql -U postgres -d corebank
docker compose --env-file docker/.env -f docker/compose.yaml down
```

Inside psql, use `\dn` to list schemas and `\q` to exit.

## Storage and initialization

A Docker named volume preserves the database through container replacement and normal `down`. Do not add `--volumes` to `down` unless intentionally deleting the database. Database storage is separate from `data/input`, `data/output`, and `data/error`.

PostgreSQL 18 stores its cluster beneath `/var/lib/postgresql/18/docker`; the volume is mounted at `/var/lib/postgresql` as required by the official image.

The PostgreSQL image creates the database and login on an empty volume. The core
application then uses Liquibase to create schemas and tables on startup. Docker no
longer mounts application initialization SQL. Changing the environment password
does not change an existing database password.

UTF-8 database encoding and UTC timezone are configured. The application remains responsible for NFC normalization, field widths, and the topic-controlled business date. The readiness check tests whether PostgreSQL accepts connections; it does not validate application schemas or credentials.

Upgrade the PostgreSQL major version only with an explicit database migration plan.

## Liquibase migrations

Set COREBANK_DB_PASSWORD to your database password and run from the repository root:

```sh
jbang corebank/src/Core.java
```

Optional overrides: COREBANK_DB_URL (default jdbc:postgresql://localhost:5432/corebank)
and COREBANK_DB_USER (default postgres). JBang does not automatically read docker/.env.
Add `--ingest=YYYY-MM-DD` to migrate first and then stage that date's files.
A failed migration prevents the ingestion runner from starting.

Spring Boot manages Liquibase 4.29.2 through its existing dependency BOM.
The JBang launcher bundles postgres/changelog files as classpath resources:

- db.changelog-master.xml: ordered changesets.
- 001-schemas.sql: core_ingest, core, core_output, and camel_ingest.
- 002-core-ingest.sql: the four staging tables.
- 003-core-operational.sql: operational tables, audit triggers, and batch attempts.
- 004-ingestion-receipt.sql: successful four-file ingestion receipts; old dates need re-ingestion.
- 006-batch-jobs.sql: Spring Batch job/step execution metadata in core_batch.
- 005-balance-output.sql: immutable dated balances and snapshot headers; existing completed dates are not backfilled.

Liquibase stores history and checksums in public.databasechangelog, with a migration
lock in public.databasechangeloglock. Every startup checks for pending changes;
completed changesets are not rerun. Keep applied changesets and SQL files unchanged.
For each future migration, add a new SQL file and a new changeset to the master XML,
and bundle the SQL resource in Core.java, IngestionTests.java, ProcessingTests.java, GeneratorTests.java, and BatchJobTests.java using //FILES.
Use Liquibase-managed transactions; do not put BEGIN/COMMIT in the SQL files.

Fresh volumes get all changesets. Existing volumes retain both application data and
migration history across container replacement. Deleting the database volume resets
both; the next database/application startup rebuilds from the changelog.

The first two changesets use IF NOT EXISTS to adopt the previous project setup without
removing data or requiring a reset. This supports the known original schema; it does
not reconcile manually altered existing table definitions. Future changes should use
explicit migrations rather than silently ignoring conflicting objects.

Migration transactions are separate from batch ingestion. BatchIngestor still manages
its own JDBC transaction for the four-file delete-and-reload operation. There is no
Spring @Transactional boundary around ingestion at this stage.

The local default user remains postgres; dedicated application/migration roles remain
future work. Staging rows are keyed by date and source line; business-key duplicates
and unresolved references are retained for subsequent validation.

## Operational tables

Changeset 003 creates the six operational tables and database constraints/triggers.
Run the normal core launcher to apply pending migrations. Account inserts create
zero balances atomically; transactions referencing any end-dated account are rejected.
Transactions are immutable and reference a batch attempt with the same business date.
See docs/banking-core-design.md, Operational schema, for lifecycle and posting rules.
The migration creates structure only. The --process command implements staging-to-core
validation, posting, balance updates, batch-attempt tracking, and atomic output snapshots.
Migration 005 adds core_output.batch_snapshot and core_output.balance. The --output
command generates balance files from completed snapshots; it does not read live
balances or post transactions. Dates completed before migration 005 cannot be
exported because they have no historical snapshot. See the balance output contract
in docs/banking-core-design.md.

## Camel business-date migration

Migration 007 creates Camel staging, clock, and receipt tables. Migration 008
renames `camel_ingest.clock.control_date` to `active_date` and converts non-null
legacy values to the existing open batch date (old value + one day). It leaves
batch dates, source receipts, staged data, and a fresh null clock unchanged.

Both core and Camel launchers apply the shared changelog. Before starting either
updated launcher against existing data, follow the [coordinated upgrade procedure](../camel/README.md#upgrade-from-the-date-plus-one-clock):
drain old-format events, stop Camel, and update publishers and consumers together.
The SQL migration cannot inspect pending Kafka events. It rejects a running Camel
lease or inconsistent clock/batch state. `reset-data.sql` now requires migration 008.

Migration 009 adds `camel_ingest.receipt.staged_at`. New receipts default to
`clock_timestamp()` at insertion; old receipts stay NULL rather than receiving an
invented historical timestamp. Replays retain their original timestamp and date.
