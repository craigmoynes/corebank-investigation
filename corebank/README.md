# Corebank

The file-based banking core validates and stages inputs, posts transactions, saves
balance snapshots, and exports fixed-width balance files. Camel and Kafka remain
outside the core; see the [project quick start](../README.md),
[Camel guide](../camel/README.md), and [Docker guide](../docker/README.md).

Run all commands below from the repository root, not this directory.

## Phases and files

| Phase | Required input | Result |
| --- | --- | --- |
| Ingest | All four dated `.dat` files in `data/input/` | Atomic staging load in `core_ingest` |
| Process | Successful ingestion receipt and staged records | Operational updates and immutable dated balance snapshot |
| Output | Completed batch with a saved snapshot | `data/output/balance_YYYYMMDD.dat` |

For October 9, 2026, the required input files are `customer_20261009.dat`,
`account_20261009.dat`, `relationship_20261009.dat`, and `transaction_20261009.dat`.
All four must exist, even when some are empty. The watcher additionally requires
`data/input/batch_20261009.trg`, published only after the data files are complete.
Manual batch and stage commands do not require a trigger.

Directories are configured in `corebank/application.yaml`; override them with
`COREBANK_INPUT_DIRECTORY`, `COREBANK_OUTPUT_DIRECTORY`, and
`COREBANK_ERROR_DIRECTORY`, or the corresponding `--corebank.*-directory` options.
The watcher uses the error directory for failure markers and archived diagnostics.
See the [design and file contracts](../docs/banking-core-design.md) for field specifications.

## Run the core and reader tests

With PostgreSQL running and `COREBANK_DB_PASSWORD` (or its fallback,
`POSTGRES_PASSWORD`) set in your environment, from the repository root:

```sh
jbang corebank/src/Core.java
jbang corebank/tests/ReaderTests.java
```

All four file readers and their support classes live in `corebank/src/corebank/readers/`
(package `corebank.readers`). The readers are reusable components, not startup ingestion jobs.
Create it with `CustomerFileReader.create(path)`, open it with a Spring Batch
`ExecutionContext`, read until it returns null, and close it in a finally block.
All readers use 19-position IDs and enforce values from 1 through Long.MAX_VALUE.
Use AccountFileReader.create(path) for the 47-position account format.

RelationshipFileReader.create(path) reads 47-position records;
TransactionFileReader.create(path) reads 64-position records, including a
20-position nonnegative-cent amount. All amounts must fit in a Java long.
The reader suite currently contains 62 tests.

## Monitor trigger files

```sh
jbang corebank/src/Core.java --watch
# Or start the rebuilt core service in the background:
docker compose -f docker/compose.yaml up -d --build core
docker compose -f docker/compose.yaml logs -f core
```

The monitor scans immediately at startup and polls every second. It picks the
oldest ready filename, runs one batch at a time, then scans again. Publish all four
complete `.dat` files before publishing the empty ready trigger last. The trigger
is the readiness signal: missing input files immediately produce an error, with no
waiting for files to arrive. All four files are required for monitored retries too;
manual `--batch`/`--output` retain their existing recovery behavior.

| State | File |
| --- | --- |
| Ready | `data/input/batch_20260101.trg` |
| Running | `data/input/batch_20260101.INPROGRESS.trg` |
| Finished, including balance export | `data/output/batch_20260101.COMPLETE.trg` |
| Handled failure | `data/error/batch_20260101.ERROR.trg` |

The ready file is atomically renamed to INPROGRESS before validation or execution.
COMPLETE preserves the trigger contents; ERROR contains the timestamp, phase, and
exception details, including failed job steps and nested/suppressed exceptions.
Terminal files are published using a temporary file in the destination directory,
then INPROGRESS is removed, supporting separate input/output/error mounts. A crash
between these operations may leave both markers. Only exact ready names are
consumed; temporary files and terminal marker names are ignored.

On any handled failure the monitor finalizes ERROR and exits nonzero. Later ready
triggers remain untouched. An earlier ERROR blocks later dates across restarts.
Correct the files/problem, create a new ready trigger for the failed date, then
restart the core service. A successful retry archives the old ERROR under
`data/error/archive/`; repeated failures also preserve earlier diagnostics there.
Original `.dat` files are retained. Identical duplicate COMPLETE markers are accepted;
conflicting ones are preserved and cause an error.

A database advisory lock permits only one monitor per database. Existing
INPROGRESS files cause startup scanning to stop for investigation. Ordinary failures
must produce ERROR, but forced termination, JVM failure, or inability to write the
error directory/remove the source can leave INPROGRESS. Logs explicitly identify
finalization failure. Shutdown allows an active batch up to 25 seconds to finish;
forced termination may require reconciling Spring Batch and business states before
retrying. Never simply rename an active trigger while its process is still running.

Filesystem lifecycle tests (no database required):

```sh
jbang corebank/tests/TriggerMonitorTests.java
```

## Run the complete batch job

```sh
jbang corebank/src/Core.java --batch=2026-10-09
# Or, after rebuilding the image:
docker compose -f docker/compose.yaml run --rm core --batch=2026-10-09
```

`dailyBankingJob` runs `ingest` → `process` → `output` synchronously. The business
date is its sole identifying parameter; there is no timestamp or run-ID incrementer.
Spring Batch 5.2.2 (managed by the existing Boot BOM) stores job/step history in the
new `core_batch` schema, created by Liquibase migration 006. `core.batch_run` remains
the business posting audit; job completion additionally requires successful output.
A failed job exits nonzero. Repeating an already completed job is a successful no-op;
use `--output` to regenerate a subsequently deleted file.

Retry the same `--batch` command after correcting a failure:

- Missing/malformed input: fix the files; ingestion runs again.
- Failed posting: fix the inputs; ingestion reloads them and processing retries.
- Failed output after successful posting: ingestion checks business completion and
  skips reading files; the completed process step is skipped and export retries.
- Posting committed before its step completion was recorded: the processor's
  completed-date check prevents double posting when that step is retried.

The ingest step is restartable even after step success so corrected inputs can be
reloaded. Its business-state check prevents replacing a completed batch. Export's
existing identical-file check also covers publication before step metadata commits.
Tasklets use a resourceless transaction manager because existing services explicitly
own their JDBC transactions. Spring Batch metadata uses a separate JDBC transaction
manager; metadata and business commits are not one atomic transaction.

A dedicated database session lock allows one `--batch` invocation at a time across
instances. A concurrent launch fails promptly. An earlier unfinished job blocks a
later date, including an earlier output failure. `STARTING`, `STARTED`, `STOPPING`,
and `UNKNOWN` job states require operator investigation, as does a core `RUNNING`
attempt. A process crash may leave these states even though its locks are released.
There is no automatic abandoned-job recovery. Confirm the old process has ended,
inspect business and step history, and reconcile both before restarting; never mark
committed business posting as failed. Retain job metadata with its business data.

Individual `--ingest`, `--process`, and `--output` commands remain available for
manual diagnosis/recovery. They do not participate in the job-level ordering guard;
do not run them concurrently with `--batch`. The `--watch` mode invokes this job from trigger files; no time-based scheduler is configured. Choose exactly one command per invocation.

## Individual stages

Stop the watcher before manual batch or stage execution.

### Stage a batch in PostgreSQL

Start PostgreSQL and set
`COREBANK_DB_PASSWORD` to your local database password (or set `POSTGRES_PASSWORD`
as its fallback). Optional overrides:
`COREBANK_DB_URL` (default `jdbc:postgresql://localhost:5432/corebank`) and
`COREBANK_DB_USER` (default `postgres`). JBang does not automatically read docker/.env.

From the repository root:

```sh
jbang corebank/src/Core.java --ingest=2026-10-09
```

This requires customer_20261009.dat, account_20261009.dat,
relationship_20261009.dat, and transaction_20261009.dat in data/input.
Override the directory with `--corebank.input-directory=/path/to/input`.
All four files must exist; empty files are accepted. Publish complete files before
running and do not modify them during ingestion. The command exits after loading.
Liquibase runs automatically before ingestion. Running without `--ingest`, `--process`, `--output`, `--batch`, or `--watch` connects
to PostgreSQL, applies pending migrations, and exits without loading files.
See [database migration setup](../postgres/README.md#liquibase-migrations).

Each row in core_ingest records batch_date, source_file, and source_line.
All four loads commit together; any parsing or database failure rolls back the load.
A rerun atomically replaces that date's staged rows and retains other dates.
Same-date loads are serialized using a PostgreSQL transaction advisory lock.
JDBC inserts execute in groups of 500 within one transaction.
Duplicate business keys are retained for processing validation, not silently overwritten.
This command only stages data. The separate --process command validates cross-record
references, updates core tables, posts balances, and saves output snapshots. The --output
command writes balance files. Use `--watch` for trigger monitoring. Camel Kafka ingestion, closed-date extraction, and balance publication are implemented; see [Camel setup](../camel/README.md).

### Process a staged batch

From the repository root, with the database environment configured as above:

```sh
jbang corebank/src/Core.java --ingest=2026-10-09
jbang corebank/src/Core.java --process=2026-10-09
```

Run ingestion and processing as separate commands. Processing validates the date's
staging records, upserts customers/accounts/relationships, inserts new immutable
transactions, and adds credits/subtracts debits from balances in integer cents.
New accounts receive their zero balance from the existing database trigger.
Omitted reference records remain unchanged; negative balances are allowed.

All operational changes, the dated balance snapshot, and successful batch completion commit together. On an
error they roll back, and the attempt is recorded as FAILED with zero applied
counts and a diagnostic. Correct the inputs, re-ingest, and retry a failed date.
Duplicate business keys within a file fail the whole batch. Transaction IDs are
globally unique: identical account/type/amount replays are skipped, even on later
dates; conflicting contents fail. Corrections require a new reversing transaction.
Any end date, or a start date after the batch date, prohibits new transactions.
The final net balance must fit signed BIGINT cents; overflow fails the batch.

A retry of a COMPLETED date returns its stored counts without applying changes.
Re-ingesting a RUNNING or COMPLETED date is rejected. Process dates in ascending
order; an unprocessed date earlier than the latest completed date is rejected.
Counts include applied reference upserts and newly inserted transactions only.
Processing is serialized across application instances and holds the same date
lock as ingestion across attempt creation and posting. These locks coordinate
application commands, not arbitrary manual SQL writers.

Migration 004 adds a receipt written atomically with successful ingestion, including
four empty files. **Re-ingest dates staged before this migration before processing
for the first time.** Existing staging data is retained, but no receipt is inferred.
Do not manually modify staging after ingestion.

A process crash can leave a durable RUNNING attempt. New processing is then blocked
pending operator investigation; automatic crash recovery is not implemented. Verify
the original process/connection has ended and inspect the attempt before marking
an abandoned RUNNING row FAILED with completed_at and an explanatory error_message.
A COMPLETED attempt must never be changed to FAILED. Retry after recovery.


### Export balance output

Processing saves dated balances in `core_output.balance`. Export a completed batch
with a separate command:

```sh
jbang corebank/src/Core.java --output=2026-10-09
```

This writes `data/output/balance_20261009.dat`. Override the directory with
`--corebank.output-directory=/path/to/output`. Use --ingest, --process, and --output
separately, in that order, or use --batch to coordinate them; processing does not automatically write a file.

Include new accounts even when their balance is zero and
there are no transactions. Include existing accounts only when the batch posts
new transactions, including zero-amount or net-zero activity. Skipped transaction
replays and reference-only updates do not qualify an existing account for output.
Each qualifying account appears once with its final balance in cents.

Records contain a 19-position account ID followed by a 20-position signed balance,
both right-aligned and space-padded: 39 positions plus LF, UTF-8 without a BOM.
Negative balances use a minus sign; zero and positive values have no sign. There
are no headers, delimiters, or decimal points. See the
[balance output contract](../docs/banking-core-design.md#balance-output-contract--confirmed-2026-10-09).

Rows are sorted by account ID. An empty snapshot produces a zero-byte file.
The exporter streams the saved snapshot to a temporary file in the output directory,
then atomically publishes the final name. Filesystems without atomic moves fail
without exposing a partial final file. An identical existing file is a no-op;
a different existing file or a symlink is rejected and preserved. After reviewing
and moving a conflicting file aside, retry --output. Concurrent application exports
for the same date are serialized; external filesystem writers are not coordinated.

File failures do not undo posting or change COMPLETED status. Retry --output to
regenerate a missing file from the same historical snapshot, even after later dates
have processed. Normal failures remove temporary files; a process crash can leave
hidden `.balance_*.tmp` files, which are not completed output. Automatic cleanup of
crash leftovers is not implemented.

Migration 005 adds immutable snapshot rows and a header recording each snapshot's
batch/run identity and row count, including zero-row snapshots. **Dates completed
before migration 005 have no snapshot and cannot be exported.** They are not
backfilled from live balances, and processing retries do not repost or recreate
historical snapshots. Pending or failed batches also cannot be exported.

## Generate sample input files

From the repository root, with the operational migrations applied and
`COREBANK_DB_PASSWORD` (or `POSTGRES_PASSWORD`) exported:

```sh
# Ten records of every type, including references to this batch's new entities.
jbang corebank/src/Generate.java --date=2026-10-09 --all

# Existing eligible accounts only; other three files are empty.
jbang corebank/src/Generate.java --date=2026-10-10 --transactions=100

# Counts are independently selectable; a bare flag means 10.
jbang corebank/src/Generate.java --date=2026-10-11 --customers=20 --accounts=30 --relationships

# Explicit values override --all defaults (including zero).
jbang corebank/src/Generate.java --date=2026-10-12 --all --transactions=0 --seed=123 --output=/tmp/corebank-samples
```

All four `.dat` files are created; unselected types are zero-byte files. At least
one type or `--all` must be selected. Counts must be nonnegative. Existing dated
files and trigger files cause failure rather than replacement. Output defaults to
`corebank.input-directory` from the shared `corebank/application.yaml` (normally
`data/input`). A configuration-only Spring Boot context binds the same Config record
as the core, including external application YAML, profiles, and environment overrides.
It does not scan core components or enable auto-configuration, so no core startup
runner, datasource, or Liquibase migration runs. Picocli handles arguments first;
--help and invalid arguments do not start Spring. Like the core, it does not
automatically load Docker .env files. Database environment conventions remain: COREBANK_DB_URL,
COREBANK_DB_USER (falling back to POSTGRES_USERNAME), and COREBANK_DB_PASSWORD
(falling back to POSTGRES_PASSWORD). Explicit --db-url/--db-user/--output options override the corresponding Spring settings.
Use standard Spring environment settings (such as SPRING_PROFILES_ACTIVE and
SPRING_CONFIG_ADDITIONAL_LOCATION) for profile or external YAML selection.
Run --help for options. Generation does not migrate, insert, or update database data.

The generator reads a consistent read-only PostgreSQL snapshot. Customers/accounts
created within the batch join the reference pools; relationships avoid existing
core account/customer pairs. Transactions use only accounts whose end_date is null
and start_date is on/before the requested date, plus newly generated accounts.
Relationships may refer to any existing account; the end-date prohibition applies
to transactions. Missing references or insufficient unused pairs fail before data
files are written. Customer data uses bounded synthetic examples, including Unicode.
Amounts are 1–100,000 cents, with randomly selected CR/DR types.

Positive random long IDs are checked against both core and core_ingest IDs and
against IDs generated in the current run. The date and seed determine the random
stream: matching date, seed, database snapshot, and generator version reproduce
output. IDs are not reserved in the database, so pending files and concurrent
changes cannot be guaranteed collision-free. Generate/ingest sequentially and
revalidate references at posting. All reference IDs are currently loaded into memory;
this utility targets development datasets, not production-scale databases.

Files are completed in a temporary subdirectory before publication. Publication
of four files is not a single atomic filesystem operation: start ingestion only
after successful completion, and do not run simultaneous generators for the same
output/date. No batch trigger is created automatically. A crash during publication
may leave a subset of completed files that must be reviewed before retrying.

### Generator name and street lists

Edit `corebank/src/resources/firstnames.txt`, `lastnames.txt`, and `streets.txt`.
Each UTF-8 file contains one value per line. Blank lines and lines beginning with
`#` are ignored. Names allow up to 10 NFC-normalized Unicode code points; street
names allow 15, leaving space for the generated house number in the 20-position
address field. Invalid or empty lists fail generation with a clear error; values
are never truncated. JBang bundles these files as classpath resources for both
the generator and its test launcher. Changes to the lists can change seeded output.

### Shared file and Kafka generation

`corebank.generator.Generate` now owns transport-independent record generation.
`corebank.generator.FileGenerator` handles file publication; the existing
`BatchGenerator` API and `corebank/src/Generate.java` command remain compatible.
The explicit file entry point has identical arguments:

```sh
jbang corebank/src/FileGenerator.java --date=2026-10-17 --all
```

For JSON publication to the existing Camel topics, use
`camel/src/KafkaGenerator.java`. It supports the same date, seed and per-type
counts, optional acknowledged batch-date publication, and bounded asynchronous
sends. See [Kafka generator usage and ordering](../camel/README.md#generate-kafka-input).
The generator database suite includes a seventh test comparing every generated
record type across file and JSON serialization.

## Tests

Run database suites sequentially against the disposable database only.

### Database integration tests

Use the temporary PostgreSQL instance defined in `docker/compose.test.yaml`.
It exposes database `corebank_test` on `localhost:55432`, with user `postgres`
and password `test_only`. From the repository root:

```sh
export COREBANK_TEST_DB_URL=jdbc:postgresql://localhost:55432/corebank_test
export COREBANK_TEST_DB_USER=postgres
export COREBANK_TEST_DB_PASSWORD=test_only

cd docker
docker compose -f compose.test.yaml up -d --wait
jbang ../corebank/tests/IngestionTests.java
jbang ../corebank/tests/ProcessingTests.java
jbang ../corebank/tests/BatchJobTests.java
jbang ../corebank/tests/GeneratorTests.java
docker compose -f compose.test.yaml down
cd ..
```

Run the final `down` command even if the tests fail. The database uses temporary
in-memory storage; its contents are discarded when the container stops.

The test launcher applies the same Liquibase changelog before running tests.
It reads `COREBANK_TEST_DB_URL`, `COREBANK_TEST_DB_USER` (defaults to `postgres`),
and `COREBANK_TEST_DB_PASSWORD` directly from the environment. Unlike the main
application, the tests do not fall back to `POSTGRES_PASSWORD`; the test Compose
file also uses the fixed password `test_only`. JBang does not automatically load
`docker/.env`.

**Use only a disposable test database: these tests delete all rows in its four
core_ingest tables.** Six staging tests cover field mapping, reruns, date isolation,
rollback after a JDBC insert group, missing/empty files, duplicate retention, and
maximum long amounts. Existing reader tests remain in ReaderTests.java.

Six additional operational-schema tests cover initial balances and rollback, audit timestamps, relationships and references, end-dated accounts, immutable transactions, and batch retries. They run through the same IngestionTests.java launcher against the disposable test database.

### Batch orchestration integration tests

Against a disposable PostgreSQL database only:

```sh
COREBANK_TEST_DB_URL=jdbc:postgresql://localhost:55432/corebank_test \
COREBANK_TEST_DB_PASSWORD=test_only jbang corebank/tests/BatchJobTests.java
```

Nine tests cover full posting/output, duplicate jobs, empty/missing input, corrected
posting retries, export-only recovery, posting/metadata reconciliation, concurrent
launch protection, chronological failure blocking, and abandoned execution handling.
They erase application data and `core_batch` job history. They are not packaged in
the runtime image.

### Processing integration tests

With the disposable test database and COREBANK_TEST_DB_* variables above:

```sh
jbang corebank/tests/ProcessingTests.java
```

This launcher migrates the database and runs 20 tests covering posting,
replays, upserts, invalid batches, failed retries, empty ingestion, overflow, snapshot
selection/rollback, historical output, exact formatting, file conflicts/recovery,
legacy snapshot rejection, and concurrent exports.
**It truncates all operational, output snapshot, and staging tables; use only a disposable database.**

### Generator integration tests

Against a **disposable database only** (these tests truncate operational, output snapshot, and staging tables):

```sh
COREBANK_TEST_DB_URL=jdbc:postgresql://localhost:55432/corebank_test \
COREBANK_TEST_DB_PASSWORD=test_only jbang corebank/tests/GeneratorTests.java
```

The launcher applies Liquibase first. Six tests cover self-contained generation
through the real readers/ingestor, existing-account eligibility, empty files,
missing references, relationship exhaustion, reproducibility, overwrite protection,
and Unicode formatting/argument validation.
