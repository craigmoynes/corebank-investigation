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

For a fresh Camel clock, generate input for October 17. `--all` defaults to
**10 customers, 10 accounts, 10 relationships, and 10 transactions** (40 records
total, plus the batch-date control event). Override individual counts with options
such as `--transactions=100`:

```sh
jbang camel/src/KafkaGenerator.java --date=2026-10-17 --all --batch-date=2026-10-16
```

The clock value opens the following business day. Wait until Camel has staged
all intended records for October 17 before advancing the clock; generator success
confirms broker acknowledgement, not Camel staging. Inspect Camel's staged records
and batch state as described in the [Camel guide](camel/README.md).
Then close October 17 and open October 18:

```sh
jbang camel/src/KafkaGenerator.java --batch-date=2026-10-17
```

Camel extracts the closed date's four files and publishes its trigger. Core
processes the batch; Camel publishes the resulting balances to `output.balance`.
For an existing clock, use its active date and advance one day at a time. See
[Kafka generation and ordering](camel/README.md#generate-kafka-input) for options
and the [Camel runbook](camel/README.md) for contracts, state, and recovery.

### Generate and execute file input

Use this as an alternative to Kafka input. Stop Camel before using the file path,
and choose an unused date after previously completed batches. With the core
watcher running, generate all four input files, then publish the ready trigger
only if generation succeeds:

```sh
jbang corebank/src/FileGenerator.java --date=2026-10-18 --all &&
  touch data/input/batch_20261018.trg
```

The generator does not create a trigger. Core processes the files and writes
`data/output/balance_20261018.dat` plus the COMPLETE marker. Do not generate
files for a date owned by Camel. See the
[file generator guide](corebank/README.md#generate-sample-input-files) for counts,
seeds, reference requirements, and custom directories.
