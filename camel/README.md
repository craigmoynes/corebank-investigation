# Kafka and Camel banking flows

Java 21 / JBang, Apache Camel 4.18.0, PostgreSQL, and a Kafka-compatible broker.
The core remains a separate file-based application. Run commands from the repository root.
See the [project quick start](../README.md), [core guide](../corebank/README.md),
and [Docker guide](../docker/README.md) for the surrounding runtime.

## Topic contract

| Topic | Kafka key | Value |
| --- | --- | --- |
| `system.batch-date` | `clock` | Plain `YYYY-MM-DD`, **not JSON**; the new active business date |
| `input.customer` | decimal customerId | Full customer JSON |
| `input.account` | decimal accountId | Full account JSON |
| `input.relationship` | `accountId:customerId` | Full relationship JSON |
| `input.transaction` | decimal transactionId | Full transaction JSON |
| `output.balance` | `YYYY-MM-DD:accountId` | JSON containing batchDate, accountId, balance (signed cents) |

Every topic has **one partition, one replica**, `cleanup.policy=delete`, `retention.ms=-1`, and `retention.bytes=-1`.
These are single-broker development settings, with explicit unbounded retention.
Compaction is disabled so that intermediate date transitions and input changes are
not discarded. Monitor disk usage; production replication and retention need a
separate policy. Do not increase partitions: consumers explicitly reject any
partition other than zero, and upsert ordering assumes one partition per topic.

Provisioning is idempotent and verifies existing configuration. A mismatch fails
without modifying an existing topic. The route launcher also verifies topics before
starting; topic auto-creation is disabled on consumers. Do not delete/recreate
source topics while retaining staging receipts: topic/partition/offset identities
would be reused. Keep broker data, group offsets, and Camel database state together.

```sh
# Uses the existing local Redpanda listener by default; does not need a DB password.
export CAMEL_BROKERS=localhost:19092
jbang camel/src/CamelBanking.java --topics

# Supply the same database password as the core. JBang does not load docker/.env.
export COREBANK_DB_PASSWORD='your-local-database-password'
jbang camel/src/CamelBanking.java
```

Environment overrides:

| Variable | Default |
| --- | --- |
| `CAMEL_BROKERS` | `localhost:19092` |
| `CAMEL_GROUP` | `corebank-camel-v1` |
| `COREBANK_DB_URL` | `jdbc:postgresql://localhost:5432/corebank` |
| `COREBANK_DB_USER` | `postgres` |
| `COREBANK_DB_PASSWORD` | `POSTGRES_PASSWORD` fallback |
| `COREBANK_INPUT_DIRECTORY` | `data/input` |
| `COREBANK_OUTPUT_DIRECTORY` | `data/output` |
| `COREBANK_ERROR_DIRECTORY` | `data/error` |

The launcher applies the shared Liquibase changelog, including migrations 008 and 009, then
holds a database session lease to prevent a second route application for that DB.
The core watcher can run concurrently after the upgrade. Existing installations
must follow the [migration procedure](#upgrade-from-the-date-plus-one-clock) before
starting either updated application; rebuild the core image to include migrations 008 and 009. Never run a file generator or another
extractor against the same input date while Camel owns it.

Transaction format migration 010 requires a coordinated deployment of core, Camel,
and publishers. See [effective-date upgrade instructions](../corebank/README.md#upgrading-transaction-effective-dates)
for legacy files, pending Kafka events, and existing staging records.

## Input examples

Use JSON integer numbers for IDs/amounts (not strings, decimals, or floating-point
values). Producers must preserve 64-bit integers; JavaScript Number cannot represent
all supported IDs exactly. Text is normalized to NFC, measured in code points, and
space-padded to the core's existing file widths. Unknown fields, duplicate JSON
properties, tombstones, invalid enums/dates, and invalid numeric ranges are rejected.
`endDate` may be absent or null. Other fields are required. Customer blank-text
semantics remain the same as the existing core readers. Transactions require an
`effectiveDate` in `YYYY-MM-DD` format. Camel writes it into positions 65–74 of the
transaction file; core compares it with the account start date, independently of
the assigned batch date. It is part of immutable transaction content.

```json
{"customerId":1,"firstName":"José","lastName":"Doe","addressLine1":"123 Main St","city":"Toronto","province":"ON","postalCode":"A1B 2C3","country":"Canada"}
```
```json
{"accountId":2,"startDate":"2026-10-09","endDate":null,"accountType":"CHECKING"}
```
```json
{"accountId":2,"customerId":1,"type":"PRIMARY"}
```
```json
{"transactionId":3,"accountId":2,"type":"CR","amount":12345,"effectiveDate":"2026-10-09"}
```

Example on a fresh clock with a local `rpk` installation (same broker as `CAMEL_BROKERS`):

```sh
printf '%s\n' '2026-10-09' | rpk topic produce system.batch-date -k clock -X brokers=localhost:19092
printf '%s\n' '{"accountId":2,"startDate":"2026-10-09","endDate":null,"accountType":"CHECKING"}' | rpk topic produce input.account -k 2 -X brokers=localhost:19092
```

The first control opens October 9 without closing any earlier date. The next
control value `2026-10-10` closes October 9 and opens October 10. On an existing
clock, confirm the requested date is active before publishing its data. Cross-topic
producer order is **not** a batch boundary: wait until the intended inputs are
committed in PostgreSQL before sending
the next control event if you need them in the preceding batch. Even a record
produced earlier belongs to the new date if it first stages after the transition.

## Initialization and business-date transitions

The first `system.batch-date` event is the authoritative initial business date.
Deployment/setup supplies an explicit date by publishing that event and confirms
it in PostgreSQL before treating ingestion as ready. There is no wall-clock default,
startup date override, or date inferred from the first data record.

A fresh database has a null `clock.active_date` and no batches. Input routes wait
and retry without committing offsets until the first clock event atomically opens
its date and persists it. A restart resumes the stored date; setup must not resend
an old initialization date on every restart. A repeat of the current date is safe.

The next date may be any later calendar date, including a jump over weekends or
holidays. Skipped dates do not create batches. Advancing closes the prior batch and opens the new batch in one
transaction. Input can stage in the new batch while the old batch is processed.
The wall-clock cutoff is independent: advancing at 10 p.m. or 9 a.m. has the same
meaning. Assignment follows the database lock boundary, not Kafka publication time.

This release retains that staging-time boundary. It does not implement per-topic
offset barriers or guarantee original day membership when replaying a backlog into
an empty database. If that guarantee is required, define a stronger cutoff protocol
before relying on this integration for it.

## Check staging before closing a date

From the repository root, open the development database:

```sh
docker compose -f docker/compose.yaml exec postgres psql -U postgres -d corebank
```

For the quick-start batch, inspect the clock, batch state, and staged counts:

```sql
SELECT active_date FROM camel_ingest.clock;
SELECT * FROM camel_ingest.batch ORDER BY batch_date;
SELECT kind, count(*) FROM camel_ingest.record
WHERE batch_date = DATE '2026-10-17' GROUP BY kind ORDER BY kind;
```

For a fresh `--all` run, expect active date `2026-10-17`, an open October 17
batch, and ten staged records of each of the four kinds before closing that date.
For other runs, reconcile the intended business keys and records; reference
upserts and duplicate records mean counts alone are not always sufficient.
Use `\q` to exit. Broker acknowledgement alone does not confirm staging.

## Database connection pooling

Route database work uses an application-owned HikariCP pool with at most seven
connections and one minimum idle connection. Connections are returned after each
transaction and the pool closes after Camel stops. Startup migration and the
session advisory-lock lease keep dedicated, unpooled connections.

Pooling remains unchanged. Data ingestion now uses a separate bounded batching
change: at most **100 messages** from one topic/partition, with a **50 ms** flush
interval and poll timeout. Partial batches flush on the consumer thread; actual
latency includes polling, scheduling and database work. There is no asynchronous
aggregation timer or worker handoff.

Each batch borrows one connection, locks/reads the active date once, and commits
once. Receipt identities are fetched in one query; transaction conflicts are
checked in memory and with one set-based database query. Receipts and staging
rows each use one multi-row INSERT. Repeated reference keys are reduced to the
highest offset before the upsert; every distinct source still gets a receipt.
A new transaction batch therefore uses five SQL statements plus one commit,
independent of its size (up to 100). All validation and both writes roll back
together on failure. Replay preserves the original receipt date and staged time.

Only after the database commit succeeds does the same Kafka consumer thread
synchronously acknowledge the last offset of that batch. A crash between the two
commits safely replays the batch. Date controls remain single-message transactions
and share the clock lock with data batches, so all fresh records in a batch receive
one active date. Partition counts, business keys, pool settings and producer
configuration are unchanged.

## Route and recovery behavior

1. The date route persists its clock and opens/closes batches atomically. A repeated
   date is a no-op; a new date may advance to any later date. The first
   control initializes the active date without extracting any earlier dates.
2. Four data routes wait/retry without committing until the first control arrives.
   A `FOR UPDATE` lock on the clock row serializes staging and date transitions.
   The lock acquisition order defines the boundary across topics. No producer-time
   ordering or global ordering across topics is claimed.
3. Each DB transaction saves a source receipt and its original assigned date along
   with the staged record. Migration 009 adds `receipt.staged_at`, populated using
   database `clock_timestamp()` at receipt insertion after acquiring the clock lock.
   It is not Kafka arrival or commit time. Replay preserves it; pre-migration
   receipts remain NULL because their original times are unknown.
   Kafka manual commit occurs afterward on the consumer
   thread. Replay after a date transition returns the original assignment. Reference
   updates use the highest source offset for each batch/business key. Conflicting
   transaction content for the same ID in a batch is rejected; identical repeats
   are accepted. The core enforces transaction immutability across dates.
4. A one-second timer extracts the oldest closed, unextracted batch. It writes four
   UTF-8/LF fixed-width files, including empty files, then the ready trigger last.
   Temporary files are in the destination directory and final publication requires
   an atomic move. Identical files are accepted on retry; conflicting files and
   symlinks fail. The DB extraction receipt is recorded after trigger publication.
   Retained INPROGRESS/COMPLETE/ERROR markers prevent recreating a consumed trigger
   after a crash before that receipt commits. An ERROR marker remains for the core's
   existing operator-driven retry procedure. Do not remove inputs or markers while
   an extraction receipt is unresolved. Crash-leftover `.camel-*.tmp` files are
   ignored and may be removed after stopping Camel.
5. A second timer reads only final `balance_YYYYMMDD.dat` filenames. It validates
   the entire file, then synchronously publishes one JSON message per account with
   acknowledgements from all in-sync replicas and Kafka producer idempotence.
   A file hash receipt is saved only after all sends succeed. Empty files get a
   receipt and produce no messages. Files stay in place. A changed file with the
   same name is rejected; hidden temporary output files are ignored.

Balance example:

```json
{"batchDate":"2026-10-09","accountId":2,"balance":12345}
```

Broker acknowledgement and the DB publication receipt are separate commits.
A crash or failed send can redeliver part/all of a file; downstream consumers must
use `batchDate:accountId` to deduplicate. There is **no end-to-end exactly-once
claim**. Balance files are loaded into memory for whole-file validation; this first
implementation targets development datasets. Input extraction streams DB rows.

Unexpected route errors latch further processing in that application until restart;
the process remains alive and logs the first failure. Initial-clock waiting is the
only expected retry without this latch. Correct infrastructure/file problems and
restart. A permanently invalid Kafka record cannot be edited in place: stop Camel,
inspect the failed topic/partition/offset and its business effect, and explicitly
resolve/quarantine it before adjusting the consumer group position. A replacement
record appended behind the poison record alone will not unblock the partition.
There is no automatic dead-letter/skip policy. Previously committed records and
already-running exchanges remain committed. Retain receipts while offsets can be
replayed. Database loss, topic recreation, and retention cleanup require coordinated
operator recovery; they are not automated.

## Logging

Application logs use the `banking` category and default to INFO with the bundled
Logback backend. INFO records initial/advanced batch dates, closed batches,
and extraction/publication start and completion. Completion includes counts and
elapsed milliseconds and is logged only after the database receipt commits.
Repeated dates, empty polls and already-published files do not generate INFO logs.

Each input type also uses Camel's built-in throughput logger under
`banking.ingest.<kind>`. Every 60 seconds it reports the interval message count,
running total and processing rate at INFO, including zero-count idle intervals.
This is a logging heartbeat, not a Kafka health check. A message
is counted only after database staging and Kafka manual acknowledgement succeed.
These are processed-message counts, including successful duplicate replays and
reference updates, not counts of unique staged rows. Counters reset on restart,
may span business dates, and are not durable audit totals. Payloads are not logged.

Enable per-message DEBUG logs without enabling framework DEBUG logging:

```sh
jbang camel/src/CamelBanking.java --debug
```

The `--debug` flag sets `banking.log.level=DEBUG` before logging initializes.
It can also be combined with `--topics`. For manual configuration, use
`jbang -Dbanking.log.level=DEBUG camel/src/CamelBanking.java`.

JBang bundles `camel/logback.xml` automatically; launch commands are unchanged.
The configuration reuses Spring Boot 3.4.4 console defaults, matching the core
and generators: timestamp with timezone, level, PID, application name (`camel`),
thread, logger and exception formatting. Spring Boot supplies logging resources
only; Camel does not start a Spring application context. Console colours follow
Boot’s terminal detection. Framework logging remains at INFO with `--debug`.

DEBUG records persisted input assignments, Kafka batch offset acknowledgement,
and individual balance sends. Input logs identify topic/partition/offset and
assigned batch date; balance logs identify file and one-based record number.
Payloads and balances are not included in these application messages.
Staging and offset acknowledgement are separate events. A balance send log means
that send succeeded; only the file completion log confirms the publication receipt
committed. Partial sends can therefore appear again during recovery.

The first unexpected route failure logs ERROR with route/source context and the
exception. Camel's redundant exhausted-exchange logging is disabled; blocked
timers skip further work until restart. Kafka/framework diagnostics may still
report failed deliveries. No WARN is emitted for normal idle polling or repeats.

## Upgrade from the date-plus-one clock

Migration 008 changes `clock.control_date` to `clock.active_date` and adds one day
to each non-null legacy value. For example, old control October 9 becomes active
October 10. Existing batches, staged records, source receipts, and output dates
remain unchanged. A fresh null clock stays null; migration does not initialize it.

1. Stop clock and input publishers. With the old Camel version still running,
   drain all pending clock and input events and verify their PostgreSQL receipts
   and consumer positions against the paused topics' end offsets. Broker send
   acknowledgement alone is insufficient. Let closed batches finish processing.
2. Stop Camel and the core watcher. Preserve the database, Kafka topics/group
   positions, and input/output/error files together for recovery. Do not reset the
   group, delete receipts, or recreate topics as part of this upgrade.
3. Deploy the updated publishers, Camel, and rebuilt core image together. Either
   updated application applies migration 008 on startup. The migration refuses
   a running Camel application lease, a missing clock, or a clock inconsistent
   with its open batch; investigate the state rather than bypassing the checks.
4. Verify `active_date` equals the previously open batch date. Resume Camel/core,
   then publishers using the new convention. The next event is the next active
   date; generators supplying both flags must use matching `--date` and
   `--batch-date` values. Do not send a fresh initialization date on this restart.

The plain date payload cannot identify legacy semantics. Unprocessed old clock
messages **must be drained before upgrading**; migration cannot detect or translate
Kafka backlog. A legacy event whose receipt already exists replays safely with its
original assignment. An unreceipted legacy date might be rejected or might look
like a valid new-convention date, so rejection is not a compatibility mechanism.
If the old consumer cannot drain the backlog, stop and reconcile it explicitly
before upgrading. Do not run old binaries against the migrated schema or roll back
only the application; recovery requires restoring a coordinated pre-upgrade state.

Migration 007 stores `clock`, `batch`, `receipt`, `record`, and `publication` in
`camel_ingest`. The four named record types are exposed as views over `record`;
shared storage keeps assignment, source identity, and file-format validation in one
implementation. No changes are made to core business posting logic.

## Validation

The test suite requires a **disposable** database named `corebank_test` and a
separate broker. It clears Camel state and core business tables. Do not point it at
the development broker. The broker must be fresh for each run because the test
uses the actual production topic names and replays from earliest offsets.

```sh
# This script starts isolated containers and removes them even after test failure.
sh camel/tests/run.sh
```

It covers strict input contracts, clock persistence, repeated/backward dates and forward jumps,
legacy clock migration and lease/state guards, source replay after rollover,
reference upserts, transaction conflicts, racing input/clock assignment, row-lock
coordination, empty/conflicting files, extraction crash recovery, core
reader/posting/output round-trip, failed broker publication, actual Camel Kafka
consumers/producers, and route restart. Existing core tests remain available as
documented in the [core guide](../corebank/README.md#tests).

References: [Camel Kafka manual commits](https://camel.apache.org/components/4.18.x/kafka-component.html),
[Redpanda local development](https://docs.redpanda.com/streaming/current/get-started/quick-start/).

## Generate Kafka input

The transport-neutral `corebank.generator.Generate` prepares a read-only database
snapshot and streams typed field values to a single-use sink. `FileGenerator`
writes the existing fixed-width format; `banking.KafkaGenerator` serializes those
same fields as JSON with exact 64-bit numbers and the topic keys above. Reference
pools and used IDs still occupy memory; message payloads are not accumulated.

From the repository root, with migrated database credentials exported and topics
provisioned as above:

```sh
# Publish only the active business date; no --date or database connection needed.
jbang camel/src/KafkaGenerator.java --batch-date=2026-10-17

# Fresh clock only: initialize October 17 and publish ten records of each type.
jbang camel/src/KafkaGenerator.java --date=2026-10-17 --all --batch-date=2026-10-17

# No clock event; use the date already active in Camel.
jbang camel/src/KafkaGenerator.java --date=2026-10-17 --transactions=1000

# Bare type flags mean ten, explicit counts override --all.
jbang camel/src/KafkaGenerator.java --date=2026-10-17 --customers=100 --accounts --seed=123 --max-pending=500
```

`--date` controls generated account start dates, transaction effective dates, and eligibility and is required only
when selecting data record types. `--batch-date` alone publishes the literal
`system.batch-date` value without starting Spring or connecting to the database.
With data selected, it must equal `--date`.
It is acknowledged before record generation/publication begins. Acknowledgement
is not confirmation that Camel processed the clock: ordering across topic
consumers remains unchanged. On an initialized system, publish the date separately,
confirm it is active in PostgreSQL, and then publish data while no other publisher
advances the clock. Otherwise data may stage under the prior date even though the
clock was sent first. Without this option no control event is sent; `--date` does
not force batch assignment or verify Camel's clock.
A standalone control opens the supplied date as-is. If `--date` is also supplied
without data types, it is unused. Supported dates are 0001-01-01 through 9999-12-31;
the final supported date can open and repeat but cannot advance within this range.

`--brokers` overrides `CAMEL_BROKERS` (default `localhost:19092`). Repeat
`--topic customer=custom.customer` or `--topic batch-date=custom.clock` to override
topics; configure matching consumers separately. The producer uses partition zero,
`acks=all`, idempotence, and at most `--max-pending` outstanding record futures
(default 1000). It checks every future before reporting success. Failures exit
nonzero; already accepted records cannot be rolled back, and reruns can duplicate
records. Database settings, `--db-url`, `--db-user`, resources and seed conventions
match the file generator. It never runs migrations or writes database rows.

Tests without a broker:

```sh
jbang camel/tests/KafkaGeneratorTests.java
jbang camel/tests/KafkaGeneratorCliTests.java
```

These exercise delayed/failed control acknowledgement, final data acknowledgement,
bounded sends, data failure propagation, keys, exact long JSON and topic overrides.
The core generator integration suite also checks all four JSON types against the
file output through Camel's strict decoder.

To include the real-broker delivery test, set `CAMEL_TEST_BROKERS` to a disposable
broker. The test creates and removes a uniquely named topic, verifies one control
plus 25 data messages, and uses one partition for both to inspect publication order
without implying any ordering guarantee across the production topics:

```sh
CAMEL_TEST_BROKERS=localhost:39092 jbang camel/tests/KafkaGeneratorTests.java
```

## Profiling socket reads

Use the [JFR profiling guide and socket-read parser](performance/README.md) to report database read durations
by operation, with thread/time filters and CSV or JSON export. It analyses saved
recordings without changing the running application.
