# Kafka and Camel banking flows

Java 21 / JBang, Apache Camel 4.18.0, PostgreSQL, and a Kafka-compatible broker.
The core remains a separate file-based application. Run commands from the repository root.

## Topic contract

| Topic | Kafka key | Value |
| --- | --- | --- |
| `system.batch-date` | `clock` | Plain `YYYY-MM-DD`, **not JSON**; assigned business date is this date + one day |
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

The launcher applies the shared Liquibase changelog, including migration 007, then
holds a database session lease to prevent a second route application for that DB.
The existing core watcher can run concurrently. The running core image need not be
restarted merely to use Camel; rebuild it before its next deployment so that its
bundled changelog includes migration 007. Never run a file generator or another
extractor against the same input date while Camel owns it.

## Input examples

Use JSON integer numbers for IDs/amounts (not strings, decimals, or floating-point
values). Producers must preserve 64-bit integers; JavaScript Number cannot represent
all supported IDs exactly. Text is normalized to NFC, measured in code points, and
space-padded to the core's existing file widths. Unknown fields, duplicate JSON
properties, tombstones, invalid enums/dates, and invalid numeric ranges are rejected.
`endDate` may be absent or null. Other fields are required. Customer blank-text
semantics remain the same as the existing core readers.

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
{"transactionId":3,"accountId":2,"type":"CR","amount":12345}
```

Example with a local `rpk` installation (same broker as `CAMEL_BROKERS`):

```sh
printf '%s\n' '2026-10-08' | rpk topic produce system.batch-date -k clock -X brokers=localhost:19092
printf '%s\n' '{"accountId":2,"startDate":"2026-10-09","endDate":null,"accountType":"CHECKING"}' | rpk topic produce input.account -k 2 -X brokers=localhost:19092
```

The first control opens October 9. The next control value `2026-10-09` closes
October 9 and opens October 10. Cross-topic producer order is **not** a batch
boundary: wait until the intended inputs are staged/acknowledged before sending
the next control event if you need them in the preceding batch. Even a record
produced earlier belongs to the new date if it first stages after the transition.

## Route and recovery behavior

1. The date route persists its clock and opens/closes batches atomically. A repeated
   date is a no-op; a new date must advance exactly one calendar day. The first
   control initializes the active date without extracting any earlier dates.
2. Four data routes wait/retry without committing until the first control arrives.
   A `FOR UPDATE` lock on the clock row serializes staging and date transitions.
   The lock acquisition order defines the boundary across topics. No producer-time
   ordering or global ordering across topics is claimed.
3. Each DB transaction saves a source receipt and its original assigned date along
   with the staged record. Kafka manual commit occurs afterward on the consumer
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

It covers strict input contracts, clock persistence, repeated/backward/skipped dates,
source replay after rollover, reference upserts, transaction conflicts, row-lock
coordination, empty/conflicting files, extraction crash recovery, core
reader/posting/output round-trip, failed broker publication, actual Camel Kafka
consumers/producers, and route restart. Existing core tests remain available as
documented in the repository README.

References: [Camel Kafka manual commits](https://camel.apache.org/components/4.18.x/kafka-component.html),
[Redpanda local development](https://docs.redpanda.com/streaming/current/get-started/quick-start/).
