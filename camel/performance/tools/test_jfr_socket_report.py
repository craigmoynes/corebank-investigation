import io
import json
import unittest
from jfr_socket_report import events, milliseconds, timestamp, classify, summarize


def event(methods, duration='PT0.002S', thread='input.transaction', at='2026-10-18T12:12:24-04:00'):
    return {'type': 'jdk.SocketRead', 'values': {
        'startTime': at, 'duration': duration, 'port': 5432,
        'eventThread': {'javaName': thread},
        'stackTrace': {'frames': [{'method': {'type': {'name': c}, 'name': m}} for c, m in methods]}}}


class ReportTest(unittest.TestCase):
    def test_streams_across_small_chunks(self):
        class Small(io.StringIO):
            def read(self, n=-1):
                return super().read(min(n, 7))
        expected = [event([]), event([])]
        self.assertEqual(expected, list(events(Small(json.dumps({'recording': {'events': expected}})))))
        with self.assertRaises(ValueError):
            list(events(io.StringIO('{"recording":{"events":[{')))

    def test_duration_and_timezone(self):
        self.assertAlmostEqual(3723004, milliseconds('PT1H2M3.004S'))
        self.assertEqual(timestamp('2026-10-18T16:12:24Z'), timestamp('2026-10-18T12:12:24-04:00'))
        with self.assertRaises(ValueError):
            timestamp('2026-10-18T12:12:24')

    def test_classifies_commit_and_timer_separately(self):
        stack = [('org/postgresql/jdbc/PgConnection', 'commit')]
        self.assertEqual('DB commit', classify(event(stack)['values']))
        stack.append(('banking/FilesBridge', 'publishBalanceFileIfUnpublished'))
        self.assertEqual('DB balance publication/check', classify(event(stack)['values']))
        self.assertEqual('Other/unknown socket read', classify(event([])['values']))

    def test_receipt_and_upsert(self):
        stack = [('org/postgresql/jdbc/PgPreparedStatement', 'executeUpdate'), ('banking/Store', 'lambda$accept$0')]
        self.assertEqual('DB staging upsert', classify(event(stack)['values']))
        stack.append(('banking/Store', 'receipt'))
        self.assertEqual('DB receipt insert', classify(event(stack)['values']))

    def test_filters_and_duration_statistics(self):
        stack = [('org/postgresql/jdbc/PgConnection', 'commit')]
        source = [event(stack, f'PT0.{n:03}S') for n in range(1, 21)]
        source += [event(stack, thread='timer'), event(stack, at='2026-10-18T12:12:25-04:00')]
        rows = summarize(source, 'input.transaction', timestamp('2026-10-18T12:12:24-04:00'), timestamp('2026-10-18T12:12:25-04:00'), 5432)
        self.assertEqual(20, rows[0]['count'])
        self.assertEqual(210, rows[0]['total_ms'])
        self.assertEqual(19, rows[0]['p95_ms'])
        self.assertEqual(100, rows[0]['read_time_percent'])


if __name__ == '__main__':
    unittest.main()
