# Corebank Investigation — Design

Status: PostgreSQL Compose configuration, a Java 21 / Spring Boot 3.4.4 application launched with JBang, YAML configuration, and initial banking records are implemented. Spring Batch readers for all four input record types and a Unicode-aware tokenizer are implemented and tested. PostgreSQL staging tables and explicit command-line ingestion are implemented. Operational tables are implemented through Liquibase. Batch business validation, posting, output snapshots, and balance-file generation are implemented; trigger monitoring is implemented; Camel topic provisioning, ingestion, closed-date extraction, and balance publication are implemented. Updated 2026-10-17.

## Purpose and scope

Create a development and learning project, eventually in a new repository under `~/code`, with:

- A small Java banking core driven by fixed-width batch files.
- A database supporting staging, operational data, and batch output snapshots.
- Redpanda and Redpanda Console.
- Apache Camel routes developed using JBang and Camel MCP tooling.
- A later opportunity to learn Kubernetes with kind and experiment with Apache Pulsar.

The user authorized creating `~/code/corebank-investigation`. The repository contains the agreed scaffold and design document. PostgreSQL Compose configuration is now included. The core now starts Spring Boot and binds directory settings from application.yaml to a Config record.

## Planned repository layout

```text
corebank-investigation/
  corebank/       # Java banking core
  camel/          # Future Camel routes and integration configuration
  postgres/       # Database schema definitions and migrations
  docker/         # Docker Compose and container configuration
  data/
    input/        # Host-mounted core inputs and trigger files
    output/       # Host-mounted balance outputs
    error/        # Host-mounted error artifacts
  docs/           # Design and file specifications
```

This layout has been created as an initial scaffold. Docker Compose is the agreed initial runtime. PostgreSQL is the planned database. Camel and topic implementation details are deferred for now; the earlier integration design below remains future context.

## Confirmed data conventions

| Item | Convention |
| --- | --- |
| IDs | Positive Java `long` (1 through Long.MAX_VALUE); database `BIGINT`; 19 file positions |
| Money | Integer cents; Java `long` and database `BIGINT`; transaction amount field width 20 |
| Example amount | `12345` represents 123.45 currency units |
| Core input and output | Fixed-width files |
| Field padding | Spaces |
| Encoding | UTF-8 |
| Line endings | LF (`\n`) |
| Type field widths | 8 for account type; 9 for relationship type; 6 for transaction type |
| Customer text widths | 10 each for firstname, lastname, city, country; 20 for addressLine1; 2 for province; 7 for postalCode |
| Date field representation | ISO 8601 calendar date: `YYYY-MM-DD` |
| Filename date representation | `YYYYMMDD` |
| Internal date types | Java `LocalDate` and database `DATE` are the proposed implementation |

ID fields use 19 positions and prohibit zero and negative values. Transaction amounts occupy 20 positions. Balance output uses a 19-position account ID and a 20-position signed balance in cents, both right-aligned and space-padded. Transaction amounts are nonnegative; CR adds and DR subtracts during posting. Field widths count Unicode code points after NFC normalization, not UTF-8 bytes. Reject overlength values rather than truncating them. Transaction type uses the codes `CR` (credit) and `DR` (debit), space-padded to the agreed six-character field width. Input alignment is not enforced by the readers; the generator writes text/dates left-aligned and numbers right-aligned. The account reader represents an absent endDate with ten spaces.

## Confirmed Unicode and text file contract

- Encode files as UTF-8 without a byte-order mark (BOM).
- Use LF (`\n`) line endings.
- Normalize text to Unicode NFC before measuring fields or writing records.
- Measure and parse fixed-width fields by Unicode code points, not bytes or Java UTF-16 code units.
- Pad fields with ordinary U+0020 spaces.
- Reject values exceeding their field width; never silently truncate them.
- Reject embedded newlines, tabs, and other control characters in field values.
- Apply the same contract in the core and future Camel file generation/parsing.

For example, NFC-normalized `José` occupies four code points but five UTF-8 bytes. Record byte lengths may therefore vary even though field positions are fixed in code points. Code points are not necessarily visual characters for every writing system. Java implementations must use code-point-aware counting and slicing rather than assuming `String.length()` counts code points.

The ten-position first name, last name, city, and country fields remain deliberately short. addressLine1 allows 20 positions. Values exceeding their respective field widths produce validation errors. A future legacy interface requiring fixed byte offsets would need a separate, explicit contract change.

## Banking records

| Record | Fields | Allowed values / notes |
| --- | --- | --- |
| Account | `accountId`, `startDate`, `endDate`, `accountType` | `endDate` optional; AccountType values `SAVINGS`, `CHECKING` supersede A1/A2/A3 and are written in an 8-position field |
| Customer | `customerId`, `firstName`, `lastName`, `addressLine1`, `city`, `province`, `postalCode`, `country` | All address fields are required and non-null; postalCode width 7; ID width 19 |
| Relationship | `accountId`, `customerId`, `type` | RelationshipType values `PRIMARY`, `SECONDARY` replace R1/R2/R3 and are written in a 9-position field |
| Transaction | `transactionId`, `accountId`, `type`, `amount`, `effectiveDate` | Types `CR` (credit), `DR` (debit); amount in cents, width 20; reader accepts 0 through Long.MAX_VALUE |

`accountid`, `customerid`, and `transactionid` use the ID convention above. Relationships are keyed by (account_id, customer_id), with one type per pair.

### Implementation and contract checks

- Records are nested in BankingRecords in the corebank package. corebank/src/Core.java is the default-package JBang launcher; CoreApplication.java and Config.java reside in corebank/src/corebank. Production sources are under corebank/src; test sources remain under corebank/tests/corebank.
- Config uses Spring @ConfigurationProperties with the corebank prefix. JBang includes corebank/application.yaml through //FILES application.yaml=../application.yaml; defaults use data/input, data/output, and data/error relative to the launch directory.
- Customer addressLine1, city, province, postalCode, and country are required and must not be null. The convenience constructor that omitted these fields has been removed. The Customer compact constructor now enforces these five non-null requirements with Objects.requireNonNull. Whether blank or whitespace-only strings are rejected must also be made explicit in validation rules.
- Account and relationship files use the full enum names, space-padded to 8 and 9 positions respectively. These widths supersede the original five-position fields. Customer postalCode occupies 7 positions. Widths count NFC-normalized Unicode code points; overlength values must be rejected, never truncated.
- Keep this specification synchronized with explicit user decisions. Record observed implementation changes separately from unresolved contract decisions, and flag contradictions rather than silently relaxing validation.

## Input file readers — implemented

- Spring Batch 5.2.2 (managed by the existing Spring Boot 3.4.4 BOM) supplies FlatFileItemReader. The readers use spring-batch-infrastructure; the Batch starter, job repository, and database-backed jobs are deferred.
- CodePointLineTokenizer normalizes NFC before checking exact record length and slicing by Unicode code point. It rejects control characters, BOMs, Unicode line/paragraph separators, and invalid surrogates.
- CustomerFileReader.create(path) returns a new reader. The caller opens it with an ExecutionContext, reads until null, and closes it in a finally block. The --ingest command invokes this reader through BatchIngestor; a Spring Batch job remains deferred.
- Customer widths in order: ID 19, firstName 10, lastName 10, addressLine1 20, city 10, province 2, postalCode 7, country 10. Total customer record width is 88 positions. The reader and tests now use the production width exclusively.
- Files use strict UTF-8 decoding and LF separators. CRLF, BOMs, malformed UTF-8, blank records, and short/long records fail. Empty files are accepted; missing files fail. A final record without a trailing LF is accepted by the current reader.
- Field padding is stripped when mapping to Customer. Alignment is not enforced pending the contract decision. All-space address fields currently become empty strings, not null; blank-address rejection remains undecided. Shared FileReaderSupport validates unsigned decimal digits and a value from 1 through Long.MAX_VALUE. Zero, negative IDs, explicit plus signs, and overflow are rejected by all four readers. Record constructors do not yet enforce this invariant for directly constructed objects.
- Parsing failures include the resource and line number through Spring Batch. No skip policy is configured. BatchProcessor checks duplicate business keys and cross-record references after ingestion.
- JUnit Jupiter 5.11.4 tests run through the matching JUnit Platform console standalone 1.11.4, explicitly pinned because the standalone artifact is not version-managed by the Boot BOM. Run from the repository root: `jbang corebank/tests/ReaderTests.java`.
- Sixty-two tests cover all four input record types, both account types, optional end dates, strict calendar dates, production-width IDs including Long.MAX_VALUE, zero/negative IDs and overflow, Unicode normalization and boundaries, invalid records, UTF-8, and diagnostics.
- AccountFileReader.create(path) uses fields at positions 1–19 (accountId), 20–29 (startDate), 30–39 (endDate), and 40–47 (accountType). startDate is required; endDate is either ten spaces or YYYY-MM-DD. BatchProcessor validates date ordering and account eligibility for new transactions. Both readers share strict UTF-8/LF handling through FileReaderSupport.

### Relationship and transaction layouts

| Record | Field | Positions (inclusive) | Width |
| --- | --- | --- | --- |
| Relationship | accountId | 1–19 | 19 |
| Relationship | customerId | 20–38 | 19 |
| Relationship | type | 39–47 | 9 |
| Transaction | transactionId | 1–19 | 19 |
| Transaction | accountId | 20–38 | 19 |
| Transaction | type | 39–44 | 6 |
| Transaction | amount | 45–64 | 20 |
| Transaction | effectiveDate | 65–74 | 10 |

RelationshipFileReader.create(path) and TransactionFileReader.create(path) share the same UTF-8/NFC/LF handling and positive-ID validation as the existing readers. Relationship types are PRIMARY/SECONDARY; transaction types are CR/DR. Exact record widths are 47 and 74 code points respectively.

The user selected a 20-position transaction amount field. This does not expand the Java long numeric range: values above Long.MAX_VALUE are rejected. Transaction inputs use nonnegative cents, accepting zero and rejecting signs and decimal fractions. Posting adds CR amounts and subtracts DR amounts; resulting balances may be negative. Direct record constructors still permit values that the file readers reject.

Readers validate individual records only. BatchIngestor stages records; BatchProcessor validates cross-record references and duplicate business keys, then posts operational updates. Relationships are unique by account/customer pair.

## Database boundaries

PostgreSQL is the planned database.

| Schema | Responsibility |
| --- | --- |
| `camel_ingest` | Continuously stage topic records enriched with an assigned batch date |
| `core_ingest` | Stage and validate records read from the core's input files |
| `core` | Hold operational customers, accounts, relationships, transactions, and balances |
| `core_output` | Hold balance snapshots with a batch date, used to generate output files |

Proposed table names in `camel_ingest` and `core_ingest`: `customer`, `account`, `relationship`, `transaction`. Use a `batch_date` column in shared Camel staging tables rather than separate tables for each date.

Confirmed: `core` must contain a balance table. Proposed minimum fields are `accountid` and `balance` in cents. `core_output.balance` must include the batch date; proposed minimum fields are `batchDate`, `accountid`, and `balance`.

## Core file interface

For business date `2026-10-09`, input files are:

```text
customer_20261009.dat
account_20261009.dat
relationship_20261009.dat
transaction_20261009.dat
batch_20261009.trg
```

The core `--watch` monitor starts the batch when `batch_YYYYMMDD.trg` arrives in input. Empty contents are sufficient; the filename supplies the date. See the trigger lifecycle below.

The implemented core flow (--ingest, then --process, then --output):

1. Reads the matching data files into `core_ingest`.
2. Validates the staged records.
3. Merges customers, accounts, and relationships into existing `core` data, creating or updating records.
4. Processes transactions and maintains `core.balance` with each new account starting at zero. Balances may be positive or negative; daily credits and debits change the balance.
5. Writes a dated balance snapshot into `core_output`.
6. Generates `balance_YYYYMMDD.dat` from that snapshot.

### Balance output contract — confirmed 2026-10-09

The only requested business output is `balance_YYYYMMDD.dat`, containing exactly
`accountid` and the final account `balance` in integer cents for the batch date.
Processing commits output snapshots with the batch. The separate --output command generates the file from a completed snapshot.

Include each qualifying account once:

- Accounts newly created by the batch, including zero-balance accounts with no transactions.
- Existing accounts with at least one newly posted transaction in the batch, including zero-amount transactions and activity that nets to zero.
- Exclude existing accounts with no newly posted transactions, even if their reference data was updated. Identical transaction replays skipped by processing do not count as new activity.

Newly created zero-balance accounts are the explicit exception to the transaction-activity requirement. An account both created and transacted in the batch appears once with its final balance. The output amount is the closing balance, not the batch's net transaction amount.

| Field | Positions (inclusive) | Width | Representation |
| --- | --- | --- | --- |
| accountid | 1–19 | 19 | Positive decimal ID, right-aligned, space-padded |
| balance | 20–39 | 20 | Signed integer cents, right-aligned, space-padded |

Each record is 39 positions followed by LF, encoded as UTF-8 without a BOM.
Negative balances use a leading minus immediately before the digits; zero and
positive balances have no sign. Do not write decimal points, thousands separators,
headers, or delimiters. The balance field accommodates the full signed Java long /
PostgreSQL BIGINT range, including `-9223372036854775808`.

The snapshot captures these selected accounts and their final balances by batch
date, committing with posting and COMPLETED status. The exporter reads the saved
snapshot, allowing file retries without reposting transactions or reading balances
changed by later batches. Rows are ordered by account ID; an empty snapshot produces
a zero-byte file.

Customer, account, and relationship inputs are full-record updates; partial updates are not supported. Input files must have at most one record per business key per batch date. Future Camel staging must therefore upsert by batch date and business key before extraction. Distinct transactions remain distinct by `transactionid`, not by account ID. The relationship business key is (account_id, customer_id).

Currency scope is CAD/USD, using integer cents. No currency field or foreign-exchange behaviour has been specified; decide whether a run uses one configured currency or accounts carry a currency before supporting both simultaneously. Implemented posting adds credits and subtracts debits, using nonnegative transaction amounts.

The core has no direct dependency on Redpanda or Camel. It communicates through files and its database schemas.

## Host directories and mounts

Confirmed: `input/`, `output/`, and `error/` must exist on the host machine and all three must be mounted into the core container.

| Host directory | Proposed core container path | Purpose |
| --- | --- | --- |
| `data/input/` | `/data/input` | Batch data and trigger files |
| `data/output/` | `/data/output` | Balance output files |
| `data/error/` | `/data/error` | Error artifacts; exact contents and handling to be defined |

The host directories live under the repository’s `data/` directory. Containerised Camel needs access to the input/output directories it uses. Camel running on the host through JBang can use the host paths directly.

## Camel ingestion and business-date clock

Source topics contain real-time records. A separate control topic carries dates. The final topic name is `system.batch-date` (confirmed 2026-10-16).

Confirmed assignment rule:

**Assigned batch date = the active business date supplied by the latest applied control event.**

Updated 2026-10-17: this supersedes the original date-plus-one convention.

This is driven by the control message, not by the host's current date.

| Event | Intended result |
| --- | --- |
| First control event is `2026-10-09` | Open `2026-10-09`; no previous batch is closed |
| Data messages arrive | Camel stages them in `camel_ingest` with `batch_date = 2026-10-09` |
| Next control event is `2026-10-10` | Close `2026-10-09` and open `2026-10-10` atomically |
| Further data messages arrive | Camel stages them with `batch_date = 2026-10-10` |

Camel continuously writes to database tables. It does not continuously append directly to the final core input files. Once a day, a separate extraction step selects a closed batch date and generates the four fixed-width input files.

Recommended interpretation, accepted as the working design: assign the batch date when Camel stages the record. A delayed record staged after the date transition belongs to the new batch, even if it was produced earlier. Different topics do not provide one global arrival order; the implementation must define and coordinate the staging/date-transition boundary explicitly.

Recommended recovery behaviour:

- Persist the active business date across restarts.
- Initialize only from an explicit first clock event; wait before assigning data records.
- Never reset the clock on restart or infer it from wall time or a data record.
- Treat a repeated control date as a no-op.
- Preserve each record's original batch assignment when retrying.
- Retain source topic, partition, and offset as ingestion identity and traceability metadata.

Implemented policies and the serialization boundary are described in the 2026-10-16 section below.

## Daily extraction and return flow

```text
Real-time data topics + date control topic
                    |
                    v
         Camel date enrichment and staging
                    |
                    v
          camel_ingest (batch_date)
                    |
          Daily closed-date extraction
                    |
                    v
 Host input/: four .dat files, then batch_YYYYMMDD.trg
                    |
                    v
              Java banking core
       core_ingest -> core -> core_output
                    |
                    v
       Host output/: balance_YYYYMMDD.dat
                    |
                    v
          Camel reads and publishes
                    |
                    v
              Balance output topic
```

Implementation assumption: a one-second timer extracts the oldest closed, unextracted date after a transition. It never extracts the currently open date.

Suggested file publication protocol:

1. Generate files under temporary names.
2. Finish and publish all four input data files.
3. Create the batch trigger last.
4. Generate the balance output under a temporary name and publish its final name only when complete.

Camel reads the completed balance file and publishes per-account JSON to `output.balance`, keyed by `batchDate:accountId`. See the implementation assumptions below.

## Proposed reliability defaults — not all confirmed

- Commit core changes and the output snapshot in one database transaction.
- Regenerate a failed output file from `core_output` without reposting transactions.
- Track batches and source line numbers for diagnostics and reruns.
- Prevent duplicate transaction posting and duplicate staging on message redelivery.
- Use a stable identity such as batch date plus account ID for published balances; consumers must handle redelivery.
- Require all four data files, allowing empty files where there are no records.
- Reject invalid batches before updating operational data.
- Treat omitted customer/account records as unchanged rather than deleted.

Database commits, filesystem publication, and broker acknowledgements are separate operations. Recovery must be designed explicitly; no end-to-end exactly-once guarantee has been established.

## Development and infrastructure direction

Docker Compose is confirmed for the initial implementation. Later stages remain proposed:

1. Docker Compose for the database, Java core, Redpanda, and Console.
2. Camel routes run locally through JBang, using Camel MCP for development assistance.
3. Move the same application to a single-node kind cluster to learn Kubernetes and `kubectl`.
4. Add an Apache Pulsar integration experiment while preserving the core file interface.

Camel MCP is development tooling; it is not required in the runtime banking data path. Swarm was discussed but is not the recommended intermediate step because the learning goal is Kubernetes. Neither Camel nor Pulsar requires Kubernetes.

For kind, shared file access and persistent database/broker storage need explicit design. Compose host mounts do not automatically become Kubernetes storage configuration.

## Open decisions

- Core currently uses Java 21, Spring Boot 3.4.4, and JBang. Future version upgrades remain separate decisions; JUnit console tests run through ReaderTests.java.
- Input field alignment enforcement and blank/whitespace-only customer address validation. Optional endDate uses ten spaces in the implemented account reader. Null customer address fields are prohibited.
- Currency configuration or per-account currency; no FX requirement currently defined.
- Explicit deletion semantics; omitted reference records currently remain unchanged.
- Error-directory contents and error artifact publication; missing files, invalid records, and duplicate-key rejection are already handled by ingestion/processing.
- Recovery of abandoned RUNNING attempts, corrections to completed dates, retention, and file archival. Failed dates can already be re-ingested/retried; completed-date processing retries are no-ops.

The initial Camel decisions are recorded below and in `camel/README.md`; production retention/replication, dead-letter handling, and automated disaster recovery remain deferred.

## Reference documentation

- [Docker Compose](https://docs.docker.com/compose/)
- [Redpanda and Console single-broker Compose example](https://docs.redpanda.com/labs/docker-compose/single-broker/)
- [kind quick start](https://kind.sigs.k8s.io/docs/user/quick-start/)
- [Camel CLI / JBang](https://camel.apache.org/manual/camel-jbang.html)
- [Camel MCP server](https://camel.apache.org/manual/camel-jbang-mcp.html)
- [Camel Kafka component](https://camel.apache.org/components/4.18.x/kafka-component.html)
- [Camel Pulsar component](https://camel.apache.org/components/4.14.x/pulsar-component.html)

These links supported the design discussion; select and verify compatible versions when implementation starts.

## Staging ingestion — implemented 2026-10-09

`--ingest=YYYY-MM-DD` explicitly loads all four dated inputs through the existing
readers into core_ingest using JDBC and the PostgreSQL driver managed by the Boot BOM.
Liquibase applies postgres/changelog/db.changelog-master.xml on startup before ingestion. Database connection
settings live under corebank.database with COREBANK_DB_URL, COREBANK_DB_USER,
and COREBANK_DB_PASSWORD environment overrides.

Working staging policy: require all four files, accept empty files, reject parsing
errors, and atomically replace all rows for the requested date. Preserve prior rows
on failure and serialize same-date loads through a transaction advisory lock.
Rows retain source filename and line; business-key duplicates and unresolved
references are preserved for BatchProcessor validation. Staging identity is date plus
source line within each record-type table; processing enforces relationship uniqueness
by (account_id, customer_id). Inserts are grouped in 500-row JDBC batches, with a single commit
for all four files. Files must remain unchanged during the load.

This is an explicit staging command, not yet a Spring Batch job or trigger watcher.
It does not mark batches processed, post balances, or generate outputs. Same-date
correction rules after operational posting remain unresolved. Six integration tests
against PostgreSQL supplement the existing 62 reader tests.

## Liquibase schema lifecycle — implemented 2026-10-09

Spring Boot initializes Liquibase before command-line runners. The existing Boot BOM
supplies Liquibase 4.29.2; JBang bundles the master XML and referenced SQL resources.
The Docker initialization mount and manual migration workflow are replaced by this
single changelog. Initial changesets create schemas and staging tables, adopting the
known existing objects using IF NOT EXISTS. History, checksums, and locks live in
public.databasechangelog and public.databasechangeloglock and persist with the volume.
New migrations are appended as new changesets; applied scripts must not be edited.
Running the core without --ingest, --process, or --output connects to the database, migrates, and exits.
Migration failure prevents ingestion. Liquibase handles migration transactions;
BatchIngestor continues to explicitly manage its separate JDBC ingestion transaction.

## Operational schema — implemented 2026-10-09

This section supersedes earlier open relationship-key and end-date decisions.
Liquibase changeset 003 creates core.customer, core.account, core.relationship,
core.transaction, core.balance, and core.batch_run. Types and field limits match
staging. All tables have created_at and updated_at TIMESTAMPTZ processing timestamps;
business dates remain DATE values. Update triggers preserve created_at and refresh
updated_at. Immutable transactions retain their insertion timestamps.

Relationships are keyed by (account_id, customer_id), with one PRIMARY or SECONDARY
type per pair. Multiple primary customers per account are allowed; no exclusivity
rule has been requested. Foreign keys do not cascade deletes. Account end dates
must be on or after start dates.

Any non-null account end_date prohibits new transactions, even when the end date
is in the future or the supplied batch date precedes it. An insert trigger enforces
this while holding a row lock that conflicts with concurrent account closure.
Closing an account does not invalidate its historical transactions.

Account insertion automatically creates a zero balance in the same transaction.
A database trigger implements this; BatchProcessor relies on it and does not also insert the initial balance. Balances are signed
BIGINT cents. Transactions use nonnegative BIGINT amounts and CR/DR direction.
Transaction IDs are globally unique; updates/deletes are rejected. Corrections must
use a new reversing transaction. BatchProcessor compares existing contents before treating a replay as a no-op and rejects conflicts.

Each batch_run row represents an attempt with RUNNING, COMPLETED, or FAILED status,
start/finish timestamps, four nonnegative record counts, and optional failure text.
Counts describe successfully applied records, not raw staging rows. Failed attempts
can be retried; only one RUNNING or COMPLETED attempt is allowed per business date.
Transactions reference both the run ID and its matching business date. BatchProcessor
creates the attempt first, commits operational changes and successful completion
together, and records failure after rolling back operational changes. Crash recovery
and correction of completed dates remain future workflow decisions.

This migration does not post staged records, update balances on transaction insert,
or populate batch runs. BatchProcessor implements those actions in the application.
Migration 005 adds core_output.balance snapshots and batch_snapshot headers; the output contract is described above.

## Development data generator — implemented 2026-10-09

Generate.java provides a separate JBang/picocli command. It reads operational
reference data using a read-only repeatable-read transaction, then creates selected
record types and empty files for other types. Per-type defaults are 10 when selected;
--all selects every type and explicit counts override its defaults. See README for
usage and limitations. Transactions exclude every end-dated account and accounts
starting after the business date. Generated accounts start on the business date.
All output follows the NFC/code-point/UTF-8/LF contract; text/dates are left-aligned
with spaces on the right, and numbers are right-aligned.
Generation validates records through integration tests with existing readers.
It emits no trigger and performs no migrations or database writes.

## Core processing — implemented 2026-10-09

This section supersedes earlier statements that validation/posting are unimplemented.
BatchProcessor, invoked with --process=YYYY-MM-DD, validates staged business keys
and references, upserts customers/accounts/relationships, inserts new transactions,
and updates balances using CR as addition and DR as subtraction. Transactions are
immutable: existing IDs with identical account/type/amount/effective-date are no-ops regardless of
incoming batch date; differing contents fail. Duplicate keys within staging fail.
Reference omissions do not delete operational rows. New transaction accounts must
have no end date and must start on or before the transaction effective date. Negative balances and
zero amounts are allowed. Numeric aggregation checks the final signed BIGINT balance
without overflowing intermediate totals. The account trigger initializes balances.

A committed RUNNING attempt precedes the operational transaction. All upserts,
transaction inserts, balances, output snapshots, applied counts, and COMPLETED status commit together.
Errors roll back and mark the attempt FAILED separately. Failed counts stay zero.
Completed-date retries return original counts without posting; completed dates
cannot be re-ingested. Failed dates may be corrected/re-ingested/retried. Unprocessed
dates older than the latest completed date are rejected to prevent stale upserts.
A stale RUNNING attempt blocks new processing until operator investigation/recovery.
Automatic crash recovery and completed-date correction remain deferred.

A global session advisory lock serializes processors. A session date lock shares
the ingestor's transaction-lock namespace and persists across attempt commits,
preventing concurrent staging replacement. These locks require cooperating writers.
Migration 004 records successful ingestion atomically, including all-empty batches.
Pre-migration staging is preserved but must be re-ingested to receive a receipt.
ProcessingTests.java runs 20 PostgreSQL integration tests against a disposable DB,
including snapshot and output coverage. Output snapshots commit with posting;
file export is a separate command. The `--watch` monitor invokes the full job.


## Balance snapshots and export — implemented 2026-10-09

Migration 005 creates `core_output.batch_snapshot` (batch date, run ID, record count,
creation timestamp) and `core_output.balance` (batch date, account ID, signed cents).
The header distinguishes an empty completed snapshot from a missing historical
snapshot. Update/delete triggers protect both tables against mutation.

BatchProcessor captures newly created account IDs before upserts, combines them
with accounts having transactions newly inserted for this run, and snapshots the
final balances. Replayed transactions do not qualify existing accounts. Snapshot
creation and the batch's operational changes roll back together on failure.
Completed-date retries return stored counts without changing the original snapshot.

`--output=YYYY-MM-DD` requires a completed batch with a snapshot. It streams sorted
rows to a temporary file in the configured output directory, flushes it, and uses
an atomic move to publish `balance_YYYYMMDD.dat`. Unsupported atomic moves fail;
there is no non-atomic fallback. An identical existing regular file is a no-op;
conflicting files and symlinks are preserved and rejected. Concurrent application
exporters for a date use a database advisory lock across writing and publication.
External filesystem writers are not coordinated by that lock.

File failure leaves posting and COMPLETED status intact. Retry output independently;
later processing cannot change the saved balances. Normal failures clean temporary
files, but a process crash may leave hidden `.balance_*.tmp` files for operator
cleanup. Final filenames alone identify completed output. This protocol does not
provide broker delivery tracking or an end-to-end exactly-once guarantee.

Previously completed batches have no snapshot: migration 005 deliberately does not
infer historical balances or new-account membership. Export rejects those dates,
and completed-date processing retries remain no-ops. No automatic backfill is
provided. New empty batches do have a snapshot header and can export an empty file.

The processing test suite covers account selection, net-zero and zero-amount
activity, replay exclusion, snapshot rollback, stable historical regeneration,
full signed-long formatting, empty output, missing/legacy snapshots, file failures
and conflicts, immutability, and concurrent exports. Tests use a disposable database
and truncate output tables along with operational and staging tables.

## Core container — implemented 2026-10-09

`docker/corebank.Dockerfile` builds the existing Core.java entry point using
JBang portable export and runs the resulting JAR plus dependency directory in a
Java 21 JRE image as a non-root user. Tests and development tooling are excluded
from the final image. Compose provides PostgreSQL connectivity and all three host
data mounts. The `core` service now defaults to `--watch` and waits for healthy PostgreSQL.
Explicit stage or batch arguments to `compose run --rm core` override monitoring.
Automatic restart is disabled so failures require correction.

## Spring Batch orchestration — implemented 2026-10-09

`--batch=YYYY-MM-DD` executes `dailyBankingJob` with ingest, process, and output
tasklets. Migration 006 installs the Spring Batch 5.2.2 PostgreSQL metadata schema
under `core_batch`. Job identity is the business date. Repeated completed jobs are
no-ops; explicit output remains available for file regeneration. Ingest runs on
restart only when business posting is not completed, allowing corrected files after
validation failure. Existing posting and export idempotency reconcile successful
business/file operations with a failed metadata update.

Business services retain their dedicated connections and explicit transactions;
Spring Batch uses separate transactional JDBC metadata and resourceless tasklet
boundaries. `core.batch_run` describes posting, while job completion includes export.
One session advisory lock serializes full jobs, and unresolved earlier jobs block
later dates. Active/unknown metadata and abandoned core attempts require operator
review. Manual stage commands remain available but must not race with the job.
Trigger monitoring is implemented below; time-based scheduling and automatic crash recovery remain future work.

## Trigger lifecycle — implemented 2026-10-09

Core polls for exact `batch_YYYYMMDD.trg` filenames in ascending order, including
startup backlog. A single monitor lease spans scanning and terminal publication.
It claims a trigger by renaming it to `batch_YYYYMMDD.INPROGRESS.trg` in input,
requires all four inputs immediately, and invokes the existing batch job.
Success publishes `batch_YYYYMMDD.COMPLETE.trg` in output. Handled failure publishes
`batch_YYYYMMDD.ERROR.trg` in error with timestamp, phase and full exception details.
Both paths remove INPROGRESS after terminal publication. Cross-mount publication
uses a destination-local temporary file and atomic rename, then source deletion.

Failure stops the monitor and an earlier ERROR blocks later dates on restart.
Corrected dates are retried by publishing a new ready trigger. Previous error
reports are archived on replacement or successful retry. A forced crash or file
finalization failure can leave INPROGRESS; encountering one stops monitoring for
operator review. No automatic business/metadata crash-state recovery is attempted.


## Camel flows — implemented 2026-10-16

This section supersedes earlier statements that Camel is future work. Topic naming
was confirmed during implementation: `system.batch-date`, `input.customer`,
`input.account`, `input.relationship`, `input.transaction`, and `output.balance`.
The initial date-plus-one rule was superseded on 2026-10-17 by active-business-date
semantics. Migration 008 preserves the existing open batch and historical assignments.
See the [initialization and upgrade runbook](../camel/README.md#upgrade-from-the-date-plus-one-clock).

Working implementation assumptions for previously open decisions:

- One partition and one replica per topic for the existing local Redpanda broker.
  Delete policy with unbounded time/size retention preserves controls and replays.
  Provisioning verifies existing settings without silently reconfiguring topics.
- Plain ISO date values keyed `clock`; full-record JSON inputs keyed by business ID
  (account/customer pair for relationships); per-account balance JSON keyed by date
  and account. IDs and cents remain 64-bit integers.
- Migration 007 adds a durable clock, batches, source receipts, staged records, and
  output publication hashes. Four named views expose the staged record types over
  shared storage. Validation produces the existing fixed-width Unicode contract.
- Clock-row locking serializes date changes with record assignment. Dates repeat or
  advance to any later date; new backward controls fail and skipped dates create no batches. An original source replay is
  recognized before date validation and keeps its original assignment. New records
  wait for initialization. Reference upserts use the latest source offset, while
  conflicting transaction IDs within a date fail.
- The oldest closed date is extracted on a timer. All four files are atomically
  published individually before the ready trigger; matching files and retained
  lifecycle markers support recovery across DB/filesystem commit gaps.
- Balance files are validated in full, published with synchronous broker acks, then
  receipted by filename/hash. A crash can redeliver stable keys. Consumers deduplicate;
  no end-to-end exactly-once guarantee is made. Empty output produces no messages.
- Unexpected failures block this route application until operator correction and
  restart. Automatic poison-message skipping and abandoned-state recovery remain
  deferred. One launcher per database is enforced by a session advisory lock.

Full contracts, configuration, limitations, and tests are in
[the Camel runbook](../camel/README.md). The tests use a disposable PostgreSQL DB and
Redpanda broker, including the existing core's reader/posting/export implementations.

Receipt migration 009 adds a database insertion timestamp (`staged_at`), preserved
on replay and NULL for historical receipts. See the [weekend cutoff example](../README.md#business-dates-and-a-weekend-cutoff)
for business-date assignment across a forward jump.

Migration 010 adds required transaction effective dates independently of batch dates.
Effective dates must not precede account opening; any account end date still rejects
new transactions. Legacy database rows use their old batch date as the compatibility
value. See the [effective-date upgrade procedure](../corebank/README.md#upgrading-transaction-effective-dates).
