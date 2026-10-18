# Profiling Camel with JFR and JMC

JDK Flight Recorder (JFR) records JVM events without per-message console logging.
Use the JDK's `jcmd` to control recording and JDK Mission Control (JMC, installed
separately) to inspect the saved file. Commands below assume a local Camel JVM
running under the same user and JDK tools available on PATH.

## 1. Find the Camel JVM

```sh
jcmd -l
```

The command is `jcmd -l`, not `jcmd ls`. Find the Camel application rather than
the generator or JMC process. Replace `12345` below with its PID.
If JDK tools are not on PATH, use `$JAVA_HOME/bin/jcmd`; on macOS you can locate
the installed JDK with `/usr/libexec/java_home`.

## 2. Start a recording

For an initial CPU, allocation, GC, and thread investigation:

```sh
jcmd 12345 JFR.start name=camel-perf settings=profile
```

For a separate, detailed database I/O investigation, capture even short socket reads:

```sh
jcmd 12345 JFR.start name=camel-io settings=profile \
  'jdk.SocketRead#enabled=true' \
  'jdk.SocketRead#threshold=0ms'
```

Choose one recording for the test. Run the workload after recording starts and
note its start/end times and receipt count. Zero-threshold recording captures
many more events and can slow the workload; keep it short and separate from
unprofiled throughput benchmarks. These settings apply across the JVM, not just
the transaction consumer.

## 3. Stop and save

Once ingestion finishes, stop the recording using its name:

```sh
jcmd 12345 JFR.stop name=camel-perf filename=/tmp/camel-pooling.jfr
```

Or, for the detailed I/O recording:

```sh
jcmd 12345 JFR.stop name=camel-io filename=/tmp/camel-io.jfr
```

Use `jcmd 12345 JFR.check` to see active recordings. Saving writes to the Camel
process's filesystem; stopping a recording does not stop Camel. Use distinct
filenames to preserve earlier runs.

## 4. Explore in JMC

Open the `.jfr` file using **File → Open File**. Page labels vary by JMC version.

1. **Threads** (or **Legacy Threads**): select the thread whose name contains
   `KafkaConsumer[input.transaction]`. Select the ingestion interval in the
   timeline and apply the time/thread selection to the view being investigated.
   Check the displayed Focus/Aspect and time range rather than assuming every
   page inherited your filters. Allow a small margin around receipt timestamps:
   the first receipt follows queries and the last precedes the final commit.
2. **Socket I/O**: inspect remote ports, read counts, and **Total I/O Time**.
   In the default local setup, `5432` is PostgreSQL and `19092` is Kafka.
   Both may have address `127.0.0.1`, so address alone is insufficient.
3. **Event Browser**: search for `Socket Read`. Select one event to inspect its
   duration and stack, or select multiple events to aggregate their stacks in
   **Stack Trace** / **Flame Graph**. Expand caller branches to distinguish
   `PgConnection.commit`, queries, receipt inserts, and staging upserts.
4. **Method Profiling**: inspect CPU samples separately from allocation and I/O
   events. CPU hotspots do not measure time spent waiting for database responses.
5. **Garbage Collections**, **Memory / Allocations**, and **Locks / Thread Parking**:
   look for pauses, allocation pressure, or Java-side waits coinciding with slow
   intervals. PostgreSQL row-lock waits appear as database response waits, not
   Java monitor contention.

Interpret the views carefully:

- **Samples / Percentage** in a count-based stack tree means event counts, not
  elapsed-time percentages. Avoid mixing allocation samples and I/O events.
- A dense coloured timeline is not proof of continuous CPU use or one long wait.
- Standard profiles omit short socket reads. Missing events cannot be recovered
  by changing display filters after recording.
- Copied stack-tree text may include only expanded/visible branches. Use the
  original recording for a complete duration report.
- Socket durations measure reads, not entire SQL calls or Kafka acknowledgements.
  Durations summed across threads can overlap.

## 5. Generate an operation-level report

Install the dependency once (see [setup](tools/README.md)), then run from the repository root:

```sh
.env/bin/python camel/performance/tools/jfr_socket_report.py /tmp/camel-io.jfr --port 5432
```

The parser defaults to the transaction consumer and groups recorded read durations
into commits, clock queries, receipt/conflict lookups, receipt inserts, and staging
upserts. It reports counts, total time, mean, and p95; CSV/JSON also include maximum.

```sh
.env/bin/python camel/performance/tools/jfr_socket_report.py /tmp/camel-io.jfr \
  --port 5432 --format csv > /tmp/camel-io-report.csv
```

See [parser options and limitations](tools/README.md) for time filters and tests.
