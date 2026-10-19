#!/bin/sh
# Run from any directory. Uses only the disposable Camel test stack.
set -eu
cd "$(dirname "$0")/.."
command -v jbang >/dev/null
command -v docker >/dev/null
if [ -n "${TEST_PYTHON:-}" ]; then
    python="$TEST_PYTHON"
elif [ -x .env/bin/python ]; then
    python=.env/bin/python
else
    python=python3
fi
command -v "$python" >/dev/null
logs=$(mktemp -d "${TMPDIR:-/tmp}/corebank-tests.XXXXXX")
printf 'Suite logs: %s\n' "$logs"
# This project shares port 55432 with compose.test.yaml. Do not run test stacks concurrently.
cleanup() { docker compose -f docker/compose.camel-test.yaml down -v; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
cleanup
docker compose -f docker/compose.camel-test.yaml up -d --wait
export COREBANK_TEST_DB_URL=jdbc:postgresql://localhost:55432/corebank_test
export COREBANK_TEST_DB_USER=postgres
export COREBANK_TEST_DB_PASSWORD=test_only
export CAMEL_TEST_BROKERS=localhost:39092
export PYTHONDONTWRITEBYTECODE=1
failed=0
run() {
    name=$1
    shift
    printf 'Running %s ... ' "$name"
    if "$@" >"$logs/$name.log" 2>&1; then
        printf 'PASS\n'
    else
        printf 'FAIL (see %s/%s.log)\n' "$logs" "$name"
        failed=$((failed + 1))
    fi
}
# Sequential: database suites deliberately reset shared disposable tables.
for suite in ReaderTests TriggerMonitorTests IngestionTests ProcessingTests BatchJobTests GeneratorTests; do
    run "$suite" jbang "corebank/tests/$suite.java"
done
run KafkaGeneratorTests jbang camel/tests/KafkaGeneratorTests.java
run KafkaGeneratorCliTests jbang camel/tests/KafkaGeneratorCliTests.java
run CamelTests jbang camel/tests/CamelTests.java
run JfrSocketReportTests "$python" -m unittest discover -s camel/performance/tools -p 'test_*.py'
printf 'Finished 10 suites; %s failed. Logs: %s\n' "$failed" "$logs"
[ "$failed" -eq 0 ]
