"""Settings/security contract checks; no external service or network required."""
import email
from http.server import BaseHTTPRequestHandler
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest import mock
import urllib.error

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("tested_garden_settings", ROOT / "garden_friends_settings.py")
settings = importlib.util.module_from_spec(spec)
spec.loader.exec_module(settings)


class MemoryConnection:
    def __init__(self, request):
        self.incoming = io.BytesIO(request)
        self.outgoing = io.BytesIO()

    def makefile(self, *_args, **_kwargs):
        return self.incoming

    def sendall(self, data):
        self.outgoing.write(data)

    def settimeout(self, _value):
        pass


class Handler(settings.GardenSettingsMixin, BaseHTTPRequestHandler):
    def do_GET(self):
        if not self.handle_garden_request():
            self._gf_send({"public": True})

    def log_message(self, *_args):
        pass


class SettingsTests(unittest.TestCase):
    password = "a long private garden password"

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="garden-settings-")
        self.addCleanup(self.temporary.cleanup)
        self.data = Path(self.temporary.name) / "data"
        self.environment = mock.patch.dict(os.environ, {"GARDEN_FRIENDS_DATA_DIR": str(self.data)})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.rate = mock.patch.object(settings, "_RATE", settings._RateLimiter())
        self.rate.start()
        self.addCleanup(self.rate.stop)
        self.revision = mock.patch.object(settings, "_ACTIVE_REVISION", "initial")
        self.revision.start()
        self.addCleanup(self.revision.stop)
        self.cookie = ""
        self.csrf = ""
        with mock.patch.object(settings.LOG, "warning"):
            settings.initialize_admin()

    def request(self, method, path, value=None, *, origin="http://garden.test", csrf=None, cookie=None, headers=None):
        body = json.dumps(value).encode() if value is not None else b""
        request_headers = {"Host": "garden.test", "Connection": "close"}
        if cookie is None:
            cookie = self.cookie
        if cookie:
            request_headers["Cookie"] = cookie
        if method == "POST":
            request_headers.update({"Content-Type": "application/json", "Content-Length": str(len(body))})
            if origin is not None:
                request_headers["Origin"] = origin
            if csrf is None:
                csrf = self.csrf
            if csrf:
                request_headers["X-CSRF-Token"] = csrf
        request_headers.update(headers or {})
        raw = (method + " " + path + " HTTP/1.1\r\n" + "".join(k + ": " + v + "\r\n" for k, v in request_headers.items()) + "\r\n").encode() + body
        connection = MemoryConnection(raw)
        Handler(connection, ("127.0.0.1", 42000), types.SimpleNamespace())
        response = connection.outgoing.getvalue()
        head, content = response.split(b"\r\n\r\n", 1)
        lines = head.split(b"\r\n", 1)
        code = int(lines[0].split()[1])
        response_headers = email.message_from_bytes(lines[1])
        parsed = json.loads(content)
        if response_headers.get("Set-Cookie"):
            self.cookie = response_headers["Set-Cookie"].split(";", 1)[0]
        if "csrf_token" in parsed:
            self.csrf = parsed["csrf_token"]
        return code, parsed, response_headers

    def setup_admin(self):
        status, session, _ = self.request("GET", "/api/settings/session")
        self.assertEqual(status, 200)
        self.assertTrue(session["setup_required"])
        code = (self.data / "setup-code.txt").read_text().strip()
        status, result, _ = self.request("POST", "/api/settings/setup", {"setup_code": code, "password": self.password})
        self.assertEqual(status, 200)
        self.assertTrue(result["authenticated"])
        return self.cookie

    def test_default_settings_and_environment_need_no_files(self):
        elsewhere = Path(self.temporary.name) / "never-created"
        with mock.patch.dict(os.environ, {"GARDEN_FRIENDS_DATA_DIR": str(elsewhere)}):
            self.assertEqual(settings.apply_environment(), "initial")
            self.assertFalse(elsewhere.exists())
            self.assertEqual(os.environ["READ_ONLY"], "1")
            self.assertEqual(os.environ["MQTT_CLIENT_ID"], "garden-friends")

    def test_public_view_and_authentication_boundary(self):
        self.assertEqual(self.request("GET", "/")[1], {"public": True})
        self.assertEqual(self.request("GET", "/api/settings")[0], 401)
        status, result, headers = self.request("GET", "/api/settings/session")
        self.assertEqual(status, 200)
        self.assertFalse(result["authenticated"])
        self.assertIn("HttpOnly", headers["Set-Cookie"])
        self.assertIn("SameSite=Strict", headers["Set-Cookie"])
        self.assertIsNone(headers.get("Access-Control-Allow-Origin"))
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertNotIn("signing_key", json.dumps(result))

    def test_setup_requires_local_code_origin_csrf_and_password(self):
        self.request("GET", "/api/settings/session")
        code = (self.data / "setup-code.txt").read_text().strip()
        payload = {"setup_code": code, "password": self.password}
        self.assertEqual(self.request("POST", "/api/settings/setup", payload, origin="https://evil.test")[0], 403)
        self.assertEqual(self.request("POST", "/api/settings/setup", payload, csrf="incorrect")[0], 403)
        self.assertEqual(self.request("POST", "/api/settings/setup", {**payload, "setup_code": "incorrect"})[0], 403)
        self.assertEqual(self.request("POST", "/api/settings/setup", {**payload, "password": "short"})[0], 400)
        self.assertEqual(self.request("POST", "/api/settings/setup", payload)[0], 200)
        self.assertFalse((self.data / "setup-code.txt").exists())
        text = (self.data / "admin.json").read_text()
        self.assertNotIn(self.password, text)
        self.assertEqual(stat.S_IMODE((self.data / "admin.json").stat().st_mode), 0o600)
        self.assertEqual(self.request("POST", "/api/settings/setup", payload)[0], 409)

    def test_login_cookie_is_secure_on_https_and_survives_initialization(self):
        self.setup_admin()
        self.request("POST", "/api/settings/logout", {})
        self.request("GET", "/api/settings/session")
        self.assertEqual(self.request("POST", "/api/settings/login", {"password": "wrong"})[0], 401)
        status, result, headers = self.request("POST", "/api/settings/login", {"password": self.password}, origin="https://garden.test")
        self.assertEqual(status, 200)
        self.assertTrue(result["authenticated"])
        self.assertIn("; Secure", headers["Set-Cookie"])
        settings.initialize_admin()
        self.assertEqual(self.request("GET", "/api/settings")[0], 200)

    def test_secret_redaction_preservation_and_explicit_clear(self):
        self.setup_admin()
        payload = {"revision": "initial", "values": {"INFLUX_TOKEN": "private-influx-token", "MQTT_PASSWORD": "private-mqtt-password"}}
        status, saved, _ = self.request("POST", "/api/settings/save", payload)
        self.assertEqual(status, 200)
        self.assertTrue(saved["restarting"])
        self.assertEqual((self.data / "restart.request").read_text().strip(), saved["revision"])
        self.assertEqual(settings.active_revision(), "initial")
        status, response, _ = self.request("GET", "/api/settings")
        self.assertEqual(status, 200)
        self.assertNotIn("private-", json.dumps(response))
        self.assertNotIn("INFLUX_TOKEN", response["values"])
        self.assertTrue(response["secrets"]["INFLUX_TOKEN"])
        self.assertTrue(response["secrets"]["MQTT_PASSWORD"])
        second = {"revision": saved["revision"], "values": {"INFLUX_TOKEN": "", "MQTT_PASSWORD": ""}, "clear_secrets": ["MQTT_PASSWORD"]}
        self.assertEqual(self.request("POST", "/api/settings/save", second)[0], 200)
        actual, revision = settings.load_settings()
        self.assertEqual(actual["INFLUX_TOKEN"], "private-influx-token")
        self.assertEqual(actual["MQTT_PASSWORD"], "")
        self.assertEqual(stat.S_IMODE((self.data / "settings.json").stat().st_mode), 0o600)
        self.assertEqual(settings.apply_environment(), revision)
        self.assertEqual(settings.active_revision(), revision)

    def test_stale_revision_and_invalid_values_do_not_overwrite_config(self):
        self.setup_admin()
        self.assertEqual(self.request("POST", "/api/settings/save", {"revision": "initial", "values": {"TIMEZONE": "UTC"}})[0], 200)
        before = (self.data / "settings.json").read_bytes()
        self.assertEqual(self.request("POST", "/api/settings/save", {"revision": "initial", "values": {"TIMEZONE": "Europe/London"}})[0], 400)
        current = settings.load_settings()[1]
        self.assertEqual(self.request("POST", "/api/settings/save", {"revision": current, "values": {"TIMEZONE": "Bad/Zone"}})[0], 400)
        self.assertEqual((self.data / "settings.json").read_bytes(), before)
        self.assertEqual(list(self.data.glob(".garden-friends-*")), [])

    def test_origin_and_csrf_checks_precede_settings_write(self):
        self.setup_admin()
        payload = {"revision": "initial", "values": {"INFLUX_TOKEN": "private"}}
        for options in ({"origin": None}, {"origin": "https://evil.test"}, {"csrf": "bad"}):
            self.assertEqual(self.request("POST", "/api/settings/save", payload, **options)[0], 403)
        self.assertFalse((self.data / "settings.json").exists())

    def test_password_change_revokes_previous_sessions(self):
        old_cookie = self.setup_admin()
        next_password = "a different long garden password"
        self.assertEqual(self.request("POST", "/api/settings/password", {"current_password": "wrong", "password": next_password})[0], 401)
        self.assertEqual(self.request("POST", "/api/settings/password", {"current_password": self.password, "password": next_password})[0], 200)
        self.assertEqual(self.request("GET", "/api/settings", cookie=old_cookie)[0], 401)
        self.assertEqual(self.request("GET", "/api/settings")[0], 200)
        self.assertNotIn(next_password, (self.data / "admin.json").read_text())

    def test_authentication_attempts_are_rate_limited(self):
        self.setup_admin()
        settings._RATE = settings._RateLimiter()
        self.request("POST", "/api/settings/logout", {})
        self.request("GET", "/api/settings/session")
        for _ in range(8):
            self.assertEqual(self.request("POST", "/api/settings/login", {"password": "wrong"})[0], 401)
        self.assertEqual(self.request("POST", "/api/settings/login", {"password": "wrong"})[0], 429)

    def test_validation_rejects_credentials_urls_invalid_topics_and_nonfinite_values(self):
        for change in ({"INFLUX_URL": "http://user:secret@host:8086"}, {"INFLUX_URL": "file:///private"},
                       {"INFLUX_URL": "http://host/api?secret=1"}, {"BIRDNET_TOPICS": "birds/#/wrong"},
                       {"FRIGATE_MIN_SCORE": float("nan")}, {"MQTT_PORT": True},
                       {"MQTT_BROKER": "mqtt://broker"}, {"MQTT_CA_FILE": "relative.pem"},
                       {"BIRDNET_MISSING_CONFIDENCE": "nan"}, {"UNKNOWN": "x"}):
            with self.subTest(change=change):
                with self.assertRaises(settings.SettingsError):
                    settings.validate_values(change)
        normalized = settings.validate_values({"BIRDNET_TOPICS": "birds/#\nbats/+", "FRIGATE_URLS": "http://frigate:5000/\nhttps://camera.example/"})
        self.assertEqual(normalized["BIRDNET_TOPICS"], "birds/#,bats/+")
        self.assertEqual(normalized["FRIGATE_URLS"], "http://frigate:5000,https://camera.example")

    def test_influx_probe_validates_bucket_access_and_never_returns_credentials(self):
        values = settings.validate_values({"INFLUX_TOKEN": "private-token", "INFLUX_ORG": "my organisation", "INFLUX_BUCKET": 'birds"bucket'})
        with mock.patch.object(settings, "_request", return_value=(200, "text/csv", b"")) as request:
            result = settings.test_connection("influx", values)
        self.assertTrue(result["ok"])
        req = request.call_args.args[0]
        self.assertEqual(req.get_header("Authorization"), "Token private-token")
        self.assertIn("org=my+organisation", req.full_url)
        query = json.loads(req.data)["query"]
        self.assertIn(json.dumps('birds"bucket'), query)
        self.assertNotIn("private-token", json.dumps(result))
        with mock.patch.object(settings, "_request", side_effect=urllib.error.HTTPError("http://private", 403, "private-token", {}, None)):
            result = settings.test_connection("influx", values)
        self.assertFalse(result["ok"])
        self.assertNotIn("private-token", json.dumps(result))

    def test_frigate_probe_uses_bearer_token_and_rejects_html_login_page(self):
        values = settings.validate_values({"FRIGATE_URLS": "https://frigate.example", "FRIGATE_AUTH_TOKEN": "camera-token"})
        with mock.patch.object(settings, "_request", return_value=(200, "text/plain", b"0.16.0")) as request:
            self.assertTrue(settings.test_connection("frigate", values)["ok"])
        self.assertEqual(request.call_args.args[0].get_header("Authorization"), "Bearer camera-token")
        with mock.patch.object(settings, "_request", return_value=(200, "text/html", b"<html>login</html>")):
            self.assertFalse(settings.test_connection("frigate", values)["ok"])
        self.assertIsNone(settings._NoRedirect().redirect_request(None, None, 302, "Found", {}, "https://elsewhere"))

    def test_frigate_total_probe_is_bounded_even_if_a_request_stalls(self):
        values = settings.validate_values({"FRIGATE_URLS": "http://first,http://second,http://third"})
        release = threading.Event()
        completed = threading.Event()
        calls = []
        def stalled(request, **kwargs):
            calls.append(request.full_url)
            try:
                release.wait(2)
                raise TimeoutError("A slow resolver or response")
            finally:
                completed.set()
        with mock.patch.object(settings, "_request", side_effect=stalled):
            started = time.monotonic()
            try:
                with self.assertRaisesRegex(settings.SettingsError, "timed out"):
                    settings._test_frigate(values, budget=0.05)
                self.assertLess(time.monotonic() - started, 0.5)
            finally:
                release.set()
                self.assertTrue(completed.wait(1))
        self.assertEqual(calls, ["http://first/api/version"])

    def test_mqtt_probe_uses_unique_client_and_does_not_publish_or_subscribe(self):
        clients = []
        class Client:
            def __init__(self, api, client_id):
                self.client_id = client_id
                self.calls = []
                clients.append(self)
            def username_pw_set(self, username, password): self.calls.append(("credentials", username, password))
            def tls_set_context(self, context): self.calls.append(("tls", context))
            def connect_async(self, host, port, keepalive): self.calls.append(("connect", host, port, keepalive))
            def loop_start(self): self.on_connect(self, None, {}, 0, None)
            def disconnect(self): self.calls.append(("disconnect",))
            def loop_stop(self): self.calls.append(("stop",))
            def publish(self, *_a, **_k): raise AssertionError("Must not publish")
            def subscribe(self, *_a, **_k): raise AssertionError("Must not subscribe")
        paho = types.ModuleType("paho"); paho.__path__ = []
        mqtt_package = types.ModuleType("paho.mqtt"); mqtt_package.__path__ = []
        mqtt = types.ModuleType("paho.mqtt.client")
        mqtt.Client = Client; mqtt.CallbackAPIVersion = types.SimpleNamespace(VERSION2=2)
        paho.mqtt = mqtt_package; mqtt_package.client = mqtt
        with mock.patch.dict(sys.modules, {"paho": paho, "paho.mqtt": mqtt_package, "paho.mqtt.client": mqtt}):
            result = settings.test_connection("mqtt", settings.validate_values({"MQTT_USERNAME": "bird", "MQTT_PASSWORD": "secret"}))
        self.assertTrue(result["ok"])
        self.assertNotEqual(clients[0].client_id, settings.DEFAULTS["MQTT_CLIENT_ID"])
        self.assertTrue(clients[0].client_id.startswith("garden-friends-probe-"))
        self.assertIn(("credentials", "bird", "secret"), clients[0].calls)
        self.assertNotIn("secret", json.dumps(result))


if __name__ == "__main__":
    unittest.main()
