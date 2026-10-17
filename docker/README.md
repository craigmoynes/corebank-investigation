# Docker runtime

The core image uses JBang 0.142.0 in a Java 21 build stage to export a portable
application JAR and its dependency directory. The final Java 21 JRE image runs as
user `10001` and contains the application, YAML configuration, migrations, and
required libraries. It does not contain JBang, a compiler, source files, or tests.
The Dockerfile-specific ignore file excludes tests, credentials, and runtime data
from the build context. Rebuild after changing application code or migrations.

For existing installations, follow the [business-date upgrade procedure](../camel/README.md#upgrade-from-the-date-plus-one-clock)
before starting a rebuilt image: core startup also applies migration 008.

Run commands from the repository root. First configure the
local password and start PostgreSQL:

```sh
cp docker/.env.example docker/.env
# Edit docker/.env and set POSTGRES_PASSWORD.
docker compose -f docker/compose.yaml up -d --wait postgres
```

Then build and start the core:

```sh
docker compose -f docker/compose.yaml build core
# Start the trigger monitor (and PostgreSQL).
docker compose -f docker/compose.yaml up -d core
# Stop monitoring before running manual stages.
docker compose -f docker/compose.yaml stop core
# Run each stage in order; stop if any command fails.
docker compose -f docker/compose.yaml run --rm core --ingest=2026-10-09
docker compose -f docker/compose.yaml run --rm core --process=2026-10-09
docker compose -f docker/compose.yaml run --rm core --output=2026-10-09
```

Compose loads `docker/.env` and starts PostgreSQL if needed, waiting for its health
check before launching core. The container connects to `postgres:5432`; the host
port override does not affect this connection. Input, output, and error directories
are mounted at `/data/input`, `/data/output`, and `/data/error`. On Linux, ensure
these host directories permit the container UID 10001 to read inputs and write
outputs/errors, or use `docker compose run --user "$(id -u):$(id -g)" --rm core ...`.

Core defaults to `--watch` in Compose and starts with ordinary `compose up -d`.
It remains running until stopped or a trigger fails. Automatic restart is disabled
so a bad batch does not cause a restart loop. Explicit commands passed to
`compose run --rm core --batch=...` override the watcher command. Stop the monitor
before manual batch/stage commands. Tests remain separate development commands
using the disposable database and are not packaged in the image.

## Operations and storage

```sh
docker compose -f docker/compose.yaml ps
docker compose -f docker/compose.yaml logs -f core
docker compose -f docker/compose.yaml down
```

Normal `down` preserves the PostgreSQL named volume and host data directories.
Adding `--volumes` deletes the database volume. See [PostgreSQL setup](../postgres/README.md)
for connection settings, initialization, migrations, and persistence.

The development Compose file runs PostgreSQL and core only. Camel runs through
JBang and uses an existing Kafka-compatible broker; see the [Camel guide](../camel/README.md).
Host JBang processes need exported database credentials and the host database port;
they do not automatically load `docker/.env`.

## Test containers

- `compose.test.yaml` provides disposable PostgreSQL on port 55432 for the
  [core integration suites](../corebank/README.md#database-integration-tests).
- `compose.camel-test.yaml` provides isolated infrastructure for
  `sh camel/tests/run.sh`; the script handles startup and cleanup.

See the [core guide](../corebank/README.md) for trigger lifecycle, full-batch execution,
individual stages, and recovery. Stop the watcher before manual execution.
