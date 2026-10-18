#!/usr/bin/env python3
"""Summarize JFR SocketRead events using JPype and the JDK binary recording reader."""
import argparse
import csv
from datetime import datetime
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time


def events(stream):
    """Stream the events array from `jfr print --json`, retaining one event at a time."""
    decoder = json.JSONDecoder()
    buf = ''
    while True:
        chunk = stream.read(65536)
        if not chunk:
            raise ValueError('Missing recording.events array')
        buf += chunk
        match = re.search(r'"events"\s*:\s*\[', buf)
        if match:
            buf = buf[match.end():]
            break
    while True:
        buf = buf.lstrip()
        if buf.startswith(','):
            buf = buf[1:].lstrip()
        if buf.startswith(']'):
            return
        try:
            event, end = decoder.raw_decode(buf)
        except json.JSONDecodeError:
            chunk = stream.read(65536)
            if not chunk:
                raise ValueError('Truncated or invalid JFR JSON')
            buf += chunk
            continue
        yield event
        buf = buf[end:]


def timestamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Timestamps must include a timezone, e.g. -04:00')
    return result


def milliseconds(value):
    match = re.fullmatch(r'PT(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?', value)
    if not match or not any(match.groups()):
        raise ValueError(f'Unsupported JFR duration: {value}')
    hours, minutes, seconds = (float(x or 0) for x in match.groups())
    return (hours * 3600 + minutes * 60 + seconds) * 1000


def classify(values):
    frames = (values.get('stackTrace') or {}).get('frames', [])
    methods = {(f['method']['type']['name'].replace('/', '.'), f['method']['name']) for f in frames}
    def has(cls, name):
        return (cls, name) in methods
    if any(cls.startswith('org.postgresql.') for cls, _ in methods):
        if has('org.postgresql.jdbc.PgConnection', 'isValid'):
            return 'DB connection validation'
        # Classify timer routes separately even when their stacks include commit/lock.
        if any(cls == 'banking.FilesBridge' and 'publish' in name for cls, name in methods):
            return 'DB balance publication/check'
        if any(cls == 'banking.FilesBridge' and 'extract' in name for cls, name in methods):
            return 'DB batch extraction/check'
        if has('org.postgresql.jdbc.PgConnection', 'commit'):
            return 'DB commit'
        if has('banking.Store', 'receipt'):
            return 'DB receipt insert'
        ingestion = any(cls == 'banking.Store' and name.startswith('lambda$accept$') for cls, name in methods)
        if ingestion:
            if has('org.postgresql.jdbc.PgPreparedStatement', 'executeUpdate'):
                return 'DB staging upsert'
            if has('org.postgresql.jdbc.PgPreparedStatement', 'executeQuery'):
                return 'DB receipt/conflict lookups'
            if has('org.postgresql.jdbc.PgStatement', 'executeQuery'):
                return 'DB clock lookup/lock'
        return 'DB other/unknown'
    if any(cls.startswith('org.apache.kafka.') for cls, _ in methods):
        return 'Kafka socket read'
    return 'Other/unknown socket read'


def summarize(source, thread='', start=None, end=None, port=None):
    groups = {}
    for event in source:
        if event.get('type') != 'jdk.SocketRead':
            continue
        v = event['values']
        name = (v.get('eventThread') or {}).get('javaName', '')
        if thread not in name or (port is not None and v.get('port') != port):
            continue
        at = timestamp(v['startTime'])
        # Include the full duration of events whose start falls in [start, end).
        if (start and at < start) or (end and at >= end):
            continue
        groups.setdefault(classify(v), []).append(milliseconds(v['duration']))
    rows = []
    total = sum(sum(v) for v in groups.values())
    for operation, values in groups.items():
        values.sort()
        elapsed = sum(values)
        rows.append(dict(operation=operation, count=len(values), total_ms=round(elapsed, 3),
                         mean_ms=round(elapsed / len(values), 6),
                         p95_ms=round(values[math.ceil(.95 * len(values)) - 1], 6),
                         max_ms=round(values[-1], 6),
                         read_time_percent=round(100 * elapsed / total, 2) if total else 0))
    return sorted(rows, key=lambda row: row['total_ms'], reverse=True)


def binary_events(path, jvm=None, thread_filter='', port_filter=None):
    """Read binary JFR via JPype and the JDK API; never expand stack metadata to JSON."""
    try:
        import jpype
    except ImportError:
        raise ValueError('JPype1 is required: use .env/bin/python or install tools/requirements.txt') from None
    if not jpype.isJVMStarted():
        if not os.environ.get('JAVA_HOME') and sys.platform == 'darwin':
            result = subprocess.run(['/usr/libexec/java_home'], capture_output=True, text=True)
            if result.returncode == 0:
                os.environ['JAVA_HOME'] = result.stdout.strip()
        jpype.startJVM(jvmpath=jvm, convertStrings=True)
    RecordingFile = jpype.JClass('jdk.jfr.consumer.RecordingFile')
    Paths = jpype.JClass('java.nio.file.Paths')
    reader = RecordingFile(Paths.get(str(path.resolve())))
    count = 0
    started = last = time.monotonic()
    print('Reading binary JFR...', file=sys.stderr)
    try:
        while reader.hasMoreEvents():
            event = reader.readEvent()
            count += 1
            now = time.monotonic()
            if now - last >= 2:
                print(f'Scanned {count:,} events ({now - started:.1f}s)...', file=sys.stderr)
                last = now
            if event.getEventType().getName() != 'jdk.SocketRead':
                continue
            thread = event.getThread()
            thread_name = thread.getJavaName() if thread else ''
            port = event.getInt('port')
            if thread_filter not in thread_name or (port_filter is not None and port != port_filter):
                continue
            stack = event.getStackTrace()
            frames = []
            if stack is not None:
                for frame in stack.getFrames():
                    method = frame.getMethod()
                    cls, name = method.getType().getName(), method.getName()
                    frames.append({'method': {'type': {'name': cls}, 'name': name}})
                    # Caller framework frames below these boundaries cannot change classification.
                    if (cls == 'banking.Store' and name == 'accept') or cls == 'banking.FilesBridge':
                        break
            yield {'type': 'jdk.SocketRead', 'values': {
                'startTime': str(event.getStartTime()),
                'duration': str(event.getDuration()),
                'port': port,
                'eventThread': {'javaName': thread_name},
                'stackTrace': {'frames': frames}}}
    finally:
        reader.close()
    print(f'Scanned {count:,} events in {time.monotonic() - started:.2f}s.', file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('recording', type=Path, help='.jfr recording or JSON from jfr print --json')
    parser.add_argument('--jvm', help='Path to JVM shared library; otherwise discovered via JAVA_HOME')
    parser.add_argument('--thread', default='input.transaction', help='Thread-name substring (default: input.transaction); empty string selects all')
    parser.add_argument('--start', type=timestamp, help='Inclusive event-start time, ISO 8601 with timezone')
    parser.add_argument('--end', type=timestamp, help='Exclusive event-start time, ISO 8601 with timezone')
    parser.add_argument('--port', type=int, help='Remote port, e.g. 5432')
    parser.add_argument('--format', choices=['table', 'csv', 'json'], default='table')
    args = parser.parse_args()
    try:
        if args.start and args.end and args.start >= args.end:
            raise ValueError('--start must precede --end')
        def report(stream):
            return summarize(events(stream), args.thread, args.start, args.end, args.port)
        if args.recording.suffix.lower() == '.json':
            with args.recording.open() as stream:
                rows = report(stream)
        else:
            source = binary_events(args.recording, args.jvm, args.thread, args.port)
            try:
                rows = summarize(source, args.thread, args.start, args.end, args.port)
            finally:
                source.close()
        if not rows:
            raise ValueError('No socket-read events matched the filters')
        if args.format == 'json':
            print(json.dumps(rows, indent=2))
        elif args.format == 'csv':
            writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        else:
            print(f"{'Operation':35} {'Count':>8} {'Total ms':>12} {'Mean ms':>10} {'p95 ms':>10} {'Read %':>8}")
            for r in rows:
                print(f"{r['operation']:35} {r['count']:8} {r['total_ms']:12.3f} {r['mean_ms']:10.6f} {r['p95_ms']:10.6f} {r['read_time_percent']:8.2f}")
            print('\nDurations describe recorded socket reads, not full SQL/commit calls or CPU time.')
            print('Percentages use summed selected read durations; concurrent threads may overlap.')
    except KeyboardInterrupt:
        parser.exit(130, 'Interrupted.\n')
    except (OSError, ValueError) as exc:
        parser.exit(1, f'Error: {exc}\n')


if __name__ == '__main__':
    main()
