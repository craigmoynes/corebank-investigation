# Corebank Investigation

A Java banking batch and Apache Camel integration learning project.

The project explores daily banking batches with a clear separation of responsibilities:
**corebank** processes fixed-width files and owns banking rules and balances;
**Camel** bridges Kafka records to input files and publishes balance output;
**Kafka** carries input events, the batch-date clock, and balance results. File and
Kafka generators share record-generation logic. PostgreSQL stores core data,
batch history, and Camel staging/receipts.

## Layout

- `corebank/`: banking core, sample file generator, and tests — [core guide](corebank/README.md).
- `camel/`: Kafka routes, topic provisioning, Kafka generator, and tests — [Camel guide](camel/README.md).
- `postgres/`: database schemas and Liquibase migrations — [database guide](postgres/README.md).
- `docker/`: runtime and test containers — [Docker guide](docker/README.md).
- `data/input/`: fixed-width inputs and ready/in-progress triggers.
- `data/output/`: balance files and completion markers.
- `data/error/`: failure markers and archived diagnostics.
- `docs/`: [design and file specifications](docs/banking-core-design.md).

Runtime data is ignored by Git; directory placeholders are tracked.

## Getting started

Run commands from the repository root. Use Docker Compose
for PostgreSQL and core, plus Java 21 and JBang for the generators and Camel.
The Kafka path also requires an existing Kafka-compatible broker (default
`localhost:19092`).

Existing installations using the old date-plus-one convention must follow the
[clock migration procedure](camel/README.md#upgrade-from-the-date-plus-one-clock)
before starting updated core or Camel applications.

Existing transaction files/publishers also need the [effective-date upgrade](corebank/README.md#upgrading-transaction-effective-dates)
when adopting migration 010.

### Build and run the core

```sh
cp docker/.env.example docker/.env
# Edit docker/.env and set POSTGRES_PASSWORD.
docker compose -f docker/compose.yaml build core
docker compose -f docker/compose.yaml up -d core
docker compose -f docker/compose.yaml logs -f core
```

Compose starts PostgreSQL, waits for its health check, and launches core with
`--watch`. Core applies migrations, then scans `data/input/` every second for
`batch_YYYYMMDD.trg`. Publish all four complete input files before the trigger.
A successful batch writes `balance_YYYYMMDD.dat` and a COMPLETE marker to
`data/output/`; a handled failure writes an ERROR marker to `data/error/` and
stops the watcher. Stop following logs with Ctrl-C; the containers keep running.

Before using host JBang commands, export the same password configured in
`docker/.env` (JBang does not load that file):

```sh
export COREBANK_DB_PASSWORD='your-local-database-password'
```

If you changed `POSTGRES_PORT`, also set `COREBANK_DB_URL` to the matching host
JDBC URL. Wait for core startup/migrations before generating data. Individual
stages and complete batches can also be run manually; see the
[core guide](corebank/README.md) for commands, required files, recovery, and tests.
See the [Docker guide](docker/README.md) for image details and container operations.

### Generate and execute Kafka input

With the core watcher running, provision topics and start Camel in another terminal
with the same exported database credentials:

```sh
jbang camel/src/CamelBanking.java --topics
jbang camel/src/CamelBanking.java
```

For a fresh Camel clock, explicitly initialize October 17:

```sh
jbang camel/src/KafkaGenerator.java --batch-date=2026-10-17
```

Confirm `camel_ingest.clock.active_date` is `2026-10-17` and that its batch is open
using the [staging checks](camel/README.md#check-staging-before-closing-a-date).
The first event opens that date without closing or processing an earlier batch.
Input waits until initialization; restarting Camel preserves the persisted date.

Then generate input for October 17. `--all` defaults to
**10 customers, 10 accounts, 10 relationships, and 10 transactions** (40 data records
total). Override individual counts with options
such as `--transactions=100`:

```sh
jbang camel/src/KafkaGenerator.java --date=2026-10-17 --all
```

The clock value names the active business date. Wait until Camel has staged
all intended records for October 17 before advancing the clock; generator success
confirms broker acknowledgement, not Camel staging. Inspect Camel's staged records
and batch state as described in the [Camel guide](camel/README.md).
Then close October 17 and open October 18:

```sh
jbang camel/src/KafkaGenerator.java --batch-date=2026-10-18
```

Camel extracts the closed date's four files and publishes its trigger. Core
processes the batch; Camel publishes the resulting balances to `output.balance`.
For an existing clock, use its active date and advance to any later date. See
[Kafka generation and ordering](camel/README.md#generate-kafka-input) for options
and the [Camel runbook](camel/README.md) for contracts, state, and recovery.

### Business dates and a weekend cutoff

A clock event names the **new active business date**. It closes the previously
active batch without changing the date already assigned to its records. Any later
date is allowed; repeating the current date leaves batch state unchanged, and a
new backward-date event is rejected. Skipped dates do not create empty batches.

For example, assume October 15 is already active. These cutoff times are chosen
by the operator; the application does not schedule them automatically:

| Actual cutoff time | Date sent | Previously active | Batch closed and processed | New active date |
| --- | --- | --- | --- | --- |
| Friday October 16, 2026, 00:01 | `2026-10-16` | October 15 | October 15 (Thursday) | October 16 |
| Saturday October 17, 2026, 00:01 | `2026-10-17` | October 16 | October 16 (Friday) | October 17 |
| Monday October 19, 2026, 00:01 | `2026-10-19` | October 17 | October 17 (weekend activity) | October 19 |

Everything first staged after Saturday's transition and before Monday's transition
belongs to **October 17**, including Sunday's activity. Monday's cutoff produces
`customer_20261017.dat`, `account_20261017.dat`, `relationship_20261017.dat`,
`transaction_20261017.dat`, and `batch_20261017.trg`. Core processes that batch,
updates balances, and writes `balance_20261017.dat`. No October 18 batch is created.
Balances change when core successfully processes the batch, not when Kafka receives
a record or when the clock event is merely published. New input can stage under
October 19 while the October 17 batch processes.

Transactions also carry an `effectiveDate`, separate from their batch date.
For example, an October 18 transaction against an account opened October 18 is
valid in the October 17 weekend batch. Eligibility compares effective date with
account start date; balances still update only during batch processing.
The generators use `--date` for account start dates and transaction effective dates.
For Kafka generation, that flag does not change the active batch date.

`camel_ingest.receipt.staged_at` records the database insertion time of the first
successful receipt, separately from its `batch_date`. Replay preserves both.
This is not Kafka arrival time or transaction commit time; older receipts have
NULL timestamps because their original staging times were not recorded.
Membership still follows the shared database lock: input published before cutoff
but staged afterward belongs to the new batch. Confirm intended input is staged
before advancing the date.

On a fresh database, the first event only opens its supplied date; there is no
previous batch to close. Restarting preserves the current date.

### Generate and execute file input

Run this standalone walkthrough in a separate fresh environment with its own
database and input/output/error directories, with Camel stopped and its clock
uninitialized. Start the core watcher in that environment, generate all four input
files, then publish the ready trigger only if generation succeeds:

```sh
jbang corebank/src/FileGenerator.java --date=2026-10-18 --all &&
  touch data/input/batch_20261018.trg
```

The generator does not create a trigger. Core processes the files and writes
`data/output/balance_20261018.dat` plus the COMPLETE marker.

The Kafka walkthrough above leaves October 18 open in Camel. Stopping Camel
preserves that batch, so do not run this file example against the same database
and directories after the Kafka walkthrough. For an existing file-only environment,
choose an unused date after previously completed batches. Do not generate files
for a date owned by Camel. See the
[file generator guide](corebank/README.md#generate-sample-input-files) for counts,
seeds, reference requirements, and custom directories.
