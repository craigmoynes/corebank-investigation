# JFR socket-read report

`jfr_socket_report.py` uses JPype1 to call the JDK's binary `RecordingFile` API
in-process. It avoids the large `jfr print` JSON expansion and does not attach to
the running Camel JVM. A compatible JDK is still required. JAVA_HOME selects the
JDK; on macOS the installed JDK is detected automatically. `--jvm` can specify the
JVM shared library explicitly.

Set up the project-local virtual environment from the repository root:

```sh
python3 -m venv .env
.env/bin/python -m pip install -r camel/performance/tools/requirements.txt
```

The `.env` directory is ignored by Git. Use `.env/bin/python` below, or activate
it with `source .env/bin/activate`. Progress is written to stderr every two seconds;
CSV/JSON stdout stays clean. Ctrl-C stops parsing without a traceback.

From the repository root:

```sh
.env/bin/python camel/performance/tools/jfr_socket_report.py /tmp/camel-io.jfr --port 5432
```

By default, only thread names containing `input.transaction` are included.
Use `--thread ''` to include all threads; background extraction/publication
operations are classified separately. Restrict a test interval with timezone-aware
timestamps, and optionally export CSV or JSON:

```sh
.env/bin/python camel/performance/tools/jfr_socket_report.py /tmp/camel-io.jfr \
  --port 5432 \
  --start '2026-10-18T12:12:23.738-04:00' \
  --end '2026-10-18T12:12:47.178-04:00' \
  --format csv > /tmp/camel-io-report.csv
```

Events are selected by start time in the inclusive-start, exclusive-end interval;
selected events retain their entire duration. Python timestamp filtering has
microsecond precision. Receipt timestamps are not exact workload boundaries:
the first receipt follows earlier queries and the last precedes the final commit.
Use a slightly wider interval to capture the full test if no other work overlaps.

The parser reports count, summed duration, mean, nearest-rank p95, maximum
(CSV/JSON), and percentage of selected read time. It streams exported events,
retaining only durations for percentile calculation. Binary recordings are decoded
directly; no intermediate JSON file is generated. Existing JSON from
`jfr print --json --events jdk.SocketRead --stack-depth 128` remains supported,
but is substantially slower to process.

Interpretation:

- These are socket-read durations, not complete database-call timings or CPU time.
- Recording thresholds omit shorter reads; zero-threshold recordings provide
  more detail but add overhead. Missing events cannot be reconstructed.
- Multiple threads' durations can overlap; the sum is not wall-clock utilization.
- Classification uses method names, not source line numbers. Receipt and conflict
  lookups remain combined. Missing/truncated stacks may yield unknown categories.
- Classification reflects the current Camel Store/FilesBridge implementation;
  review it when changing those call paths. Read count is not generally SQL count.
- Kafka socket reads do not measure all Kafka acknowledgement waiting.

Run focused parser tests:

```sh
.env/bin/python -m unittest discover -s camel/performance/tools -p 'test_*.py'
```
