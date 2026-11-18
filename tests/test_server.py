import importlib.util
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('garden_supervisor', ROOT / 'server.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class SupervisorTests(unittest.TestCase):
    def test_restart_marker_change_is_detected_even_if_same_size(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'restart.request'
            self.assertIsNone(runner.marker_signature(path))
            path.write_text('first')
            first = runner.marker_signature(path)
            path.write_text('other')
            self.assertNotEqual(first, runner.marker_signature(path))

    def test_graceful_stop_and_forced_timeout(self):
        child = Mock()
        child.poll.return_value = None
        child.wait.return_value = 0
        self.assertEqual(runner.stop_child(child), 0)
        child.send_signal.assert_called_once_with(signal.SIGTERM)
        child.kill.assert_not_called()
        child.reset_mock()
        child.wait.side_effect = [subprocess.TimeoutExpired('test', 20), -9]
        self.assertEqual(runner.stop_child(child), -9)
        child.kill.assert_called_once()

    def test_marker_restarts_only_after_old_child_is_stopped(self):
        with tempfile.TemporaryDirectory() as folder:
            events = []
            old = Mock()
            old.poll.return_value = None
            old.wait.side_effect = lambda timeout: events.append('old-stopped') or 0
            new = Mock()
            new.poll.return_value = 0

            def launch(*args, **kwargs):
                events.append('spawn')
                return old if events.count('spawn') == 1 else new

            with patch.object(runner.subprocess, 'Popen', side_effect=launch), \
                 patch.object(runner, 'marker_signature', side_effect=[None, 'changed', 'changed']), \
                 patch.object(runner.time, 'sleep'):
                self.assertEqual(runner.supervise(['synthetic'], folder), 0)
            self.assertEqual(events, ['spawn', 'old-stopped', 'spawn'])


if __name__ == '__main__':
    unittest.main()
