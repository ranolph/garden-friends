"""Exercise first-run configuration through the real supervised HTTP service."""
import http.cookiejar
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request


def test_first_run_settings_are_applied_by_managed_restart(tmp_path):
    root = Path(__file__).resolve().parents[1]
    data = tmp_path / 'private-data'
    art = tmp_path / 'art'
    art.mkdir()
    with socket.socket() as selected:
        selected.bind(('127.0.0.1', 0))
        port = selected.getsockname()[1]
    base = 'http://127.0.0.1:' + str(port)
    env = dict(os.environ, GARDEN_FRIENDS_DATA_DIR=str(data), ART_DIR=str(art),
               WEB_PORT=str(port), WEB_BIND='127.0.0.1')
    cookies = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPCookieProcessor(cookies))

    def get(path):
        with opener.open(base + path, timeout=2) as response:
            body = response.read()
            return json.loads(body) if 'application/json' in response.headers.get('Content-Type', '') else body

    def post(path, value, session):
        request = urllib.request.Request(base + path, data=json.dumps(value).encode(),
            headers={'Content-Type': 'application/json', 'Origin': base, 'X-CSRF-Token': session['csrf_token']})
        with opener.open(request, timeout=5) as response:
            return json.load(response)

    def eventually(check):
        until = time.monotonic() + 25
        while time.monotonic() < until:
            try:
                result = check()
                if result:
                    return result
            except (OSError, urllib.error.URLError, ValueError):
                pass
            time.sleep(0.1)
        raise AssertionError('The supervised service did not reach the expected state.')

    with (tmp_path / 'private-server.log').open('wb') as log:
        process = subprocess.Popen([sys.executable, '-u', str(root / 'server.py')], cwd=root, env=env,
                                   stdout=log, stderr=subprocess.STDOUT)
        try:
            health = eventually(lambda: get('/api/health'))
            assert health['read_only'] is True and health['day_boundary'] == '00:00'
            page = get('/')
            assert b'<title>Garden Friends</title>' in page and b'href="/settings"' in page
            assert b'https://github.com/ranolph' in page
            assert b'Garden Friends' in get('/settings')
            session = get('/api/settings/session')
            assert session['setup_required'] and not session['authenticated']
            setup = (data / 'setup-code.txt').read_text().strip()
            session = post('/api/settings/setup', {'setup_code': setup, 'password': 'a-local-test-password-only'}, session)
            assert session['authenticated'] and not session['setup_required']
            assert not (data / 'setup-code.txt').exists()
            initial = get('/api/settings')
            saved = post('/api/settings/save', {'values': {'TIMEZONE': 'America/New_York',
                         'INFLUX_URL': 'http://127.0.0.1:9', 'INFLUX_TOKEN': 'LOCAL_TEST_TOKEN_NOT_A_REAL_CREDENTIAL'},
                         'clear_secrets': [], 'revision': initial['revision']}, session)
            assert saved['restarting']

            def applied():
                latest = get('/api/health')
                return latest if latest['settings_revision'] == saved['revision'] else None

            health = eventually(applied)
            assert health['timezone'] == 'America/New_York' and health['influx_configured']
            assert get('/api/settings/session')['authenticated']
            configured = get('/api/settings')
            assert configured['values']['TIMEZONE'] == 'America/New_York'
            assert configured['secrets']['INFLUX_TOKEN'] is True
            assert 'LOCAL_TEST_TOKEN_NOT_A_REAL_CREDENTIAL' not in json.dumps(configured)
            assert (data / 'settings.json').stat().st_mode & 0o777 == 0o600
            assert get('/') == page
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=25)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
