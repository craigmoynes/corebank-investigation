#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
# Dedicated project, ports and disposable DB. Never substitute the normal compose file.
cleanup() { docker compose -f docker/compose.camel-test.yaml down -v; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
# Fresh broker offsets/data are required for deterministic replay assertions.
cleanup
docker compose -f docker/compose.camel-test.yaml up -d --wait
export COREBANK_TEST_DB_URL=jdbc:postgresql://localhost:55432/corebank_test
export COREBANK_TEST_DB_PASSWORD=test_only
export CAMEL_TEST_BROKERS=localhost:39092
jbang camel/tests/CamelTests.java
