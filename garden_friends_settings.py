"""Private, browser-managed Garden Friends connections and administrator access.

The public wildlife pages remain readable without signing in. Settings, credentials
and connection probes require a signed administrator session and same-origin CSRF.
Only this module writes settings; importing it creates no files or connections.
"""
from __future__ import annotations

import base64
from collections import OrderedDict
import hashlib
import hmac
from http.cookies import SimpleCookie
import json
import logging
import math
import os
from pathlib import Path
import secrets
import ssl
import stat
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zoneinfo

LOG = logging.getLogger("garden_friends.settings")
APP_DIR = Path(__file__).resolve().parent
COOKIE = "garden_friends_session"
SESSION_SECONDS = 12 * 60 * 60
PASSWORD_ITERATIONS = 600_000
SECRET_FIELDS = frozenset({"INFLUX_TOKEN", "MQTT_PASSWORD", "FRIGATE_AUTH_TOKEN"})
_LOCK = threading.RLock()
_ACTIVE_REVISION = "initial"


def field(key, label, group, type="text", default="", **extra):
    return dict(key=key, label=label, group=group, type=type, default=default, **extra)


SCHEMA = [
    field("INFLUX_URL", "InfluxDB address", "influx", "url", "http://influxdb:8086", help="InfluxDB 2.x base URL, reachable from this container."),
    field("INFLUX_TOKEN", "API token", "influx", "password", help="Read access to the selected bucket; add write permission when ingestion is enabled."),
    field("INFLUX_ORG", "Organisation", "influx", default="wildlife"),
    field("INFLUX_BUCKET", "Bucket", "influx", default="catalog"),
    field("INFLUX_TIMEOUT_MS", "Request timeout (milliseconds)", "influx", "number", 15000, min=1000, max=120000, step=1000),
    field("MQTT_BROKER", "Broker hostname or IP", "mqtt", default="mqtt", help="Use a hostname or IP address, without mqtt:// or a port."),
    field("MQTT_PORT", "Broker port", "mqtt", "number", 1883, min=1, max=65535, step=1),
    field("MQTT_USERNAME", "Username", "mqtt"),
    field("MQTT_PASSWORD", "Password", "mqtt", "password"),
    field("MQTT_CLIENT_ID", "Client ID", "mqtt", default="garden-friends", help="Give each running Garden Friends instance its own client ID."),
    field("MQTT_TLS", "Use TLS", "mqtt", "boolean", False, help="Server certificates are verified. TLS brokers commonly use port 8883."),
    field("MQTT_CA_FILE", "Custom CA certificate path", "mqtt", help="Optional absolute path inside the container. Leave blank to use trusted system certificates."),
    field("BIRDNET_TOPICS", "BirdNET subscription topics", "mqtt", "textarea", "birdnet/#,birdnet-go/#,birdnet,birdnet-go", help="One MQTT topic filter per line, or comma-separated. + and # wildcards are supported."),
    field("BIRDNET_MISSING_CONFIDENCE", "Confidence when a message omits it", "mqtt", default="0.85", help="A value from 0 to 1. Leave blank to reject messages with no confidence."),
    field("FRIGATE_URLS", "Frigate addresses", "frigate", "textarea", help="One http:// or https:// base URL per line. Leave blank if you do not use cameras."),
    field("FRIGATE_TOPIC_PREFIX", "MQTT topic prefix", "frigate", default="frigate"),
    field("FRIGATE_AUTH_TOKEN", "Bearer token", "frigate", "password", help="Optional: use only when your Frigate endpoint requires a bearer token."),
    field("FRIGATE_MIN_SCORE", "Minimum bird confidence", "frigate", "number", 0.75, min=0, max=1, step=0.01),
    field("FRIGATE_MAMMAL_MIN_SCORE", "Minimum mammal confidence", "frigate", "number", 0.75, min=0, max=1, step=0.01),
    field("HA_ENABLED", "Publish Home Assistant discovery", "homeassistant", "boolean", True, help="Home Assistant uses the MQTT broker configured above. No Home Assistant API token is needed."),
    field("HA_DISCOVERY_PREFIX", "Discovery prefix", "homeassistant", default="homeassistant", help="Match the discovery prefix in Home Assistant's MQTT integration."),
    field("PUBLIC_BASE_URL", "Garden Friends public address", "display", "url", help="Optional browser-accessible base URL used for Home Assistant artwork and snapshot links."),
    field("TIMEZONE", "Time zone", "display", default="Europe/London", help="An IANA time zone, for example Europe/London. Today resets at local midnight, including daylight-saving changes."),
    field("READ_ONLY", "Read-only mode", "advanced", "boolean", True, help="Read existing detections only. Turn off to consume MQTT events, write detections and publish Home Assistant state."),
    field("SUMMARY_INTERVAL", "Summary interval (seconds)", "advanced", "number", 30, min=5, max=3600, step=1),
    field("QUERY_CACHE_SECONDS", "Query cache (seconds)", "advanced", "number", 10, min=0, max=300, step=1),
]
FIELDS = {item["key"]: item for item in SCHEMA}
DEFAULTS = {item["key"]: item["default"] for item in SCHEMA}
SETTINGS_ENV_KEYS = frozenset(DEFAULTS)


class SettingsError(ValueError):
    """A message deliberately suitable for the browser."""


def data_dir():
    return Path(os.getenv("GARDEN_FRIENDS_DATA_DIR", str(APP_DIR / "data"))).expanduser()


def _read_json(path):
    if path.is_symlink():
        raise SettingsError("Settings storage must use regular files.")
    if not path.exists():
        return None
    if not path.is_file() or path.stat().st_size > 256 * 1024:
        raise SettingsError("Settings storage has an unexpected format.")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError, UnicodeError):
        raise SettingsError("Saved settings could not be read. Check the private data folder.") from None
    if not isinstance(value, dict):
        raise SettingsError("Settings storage has an unexpected format.")
    return value


def _atomic(path, data):
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    if path.is_symlink():
        raise SettingsError("Settings storage must use regular files.")
    fd, name = tempfile.mkstemp(prefix=".garden-friends-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fchmod(stream.fileno(), 0o600)
            os.fsync(stream.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def _write_json(path, value):
    _atomic(path, (json.dumps(value, ensure_ascii=False, allow_nan=False, indent=2) + "\n").encode())


def _url(value, name, optional=False):
    if optional and not value:
        return ""
    try:
        parsed = urllib.parse.urlsplit(value)
        valid_port = parsed.port
    except ValueError:
        raise SettingsError(name + " must be a valid http:// or https:// address.") from None
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname or
            parsed.username is not None or parsed.password is not None or
            parsed.query or parsed.fragment or any(c.isspace() for c in value)):
        raise SettingsError(name + " must be an http:// or https:// base address without credentials, query or fragment.")
    return value.rstrip("/")


def _topic(value, name, wildcards=False):
    if not value or "\x00" in value or len(value.encode("utf-8")) > 65535:
        raise SettingsError(name + " is not a valid MQTT topic.")
    for i, segment in enumerate(value.split("/")):
        if ("+" in segment and (not wildcards or segment != "+")) or (
                "#" in segment and (not wildcards or segment != "#" or i != len(value.split("/")) - 1)):
            raise SettingsError(name + " has an invalid MQTT wildcard.")
    return value


def validate_values(values):
    if not isinstance(values, dict) or set(values) - SETTINGS_ENV_KEYS:
        raise SettingsError("Unknown settings fields were supplied.")
    result = dict(DEFAULTS)
    result.update(values)
    for key, value in list(result.items()):
        spec = FIELDS[key]
        if spec["type"] == "boolean":
            if type(value) is not bool:
                raise SettingsError(spec["label"] + " must be enabled or disabled.")
        elif spec["type"] == "number":
            if type(value) not in (int, float) or not math.isfinite(value) or not spec["min"] <= value <= spec["max"]:
                raise SettingsError(spec["label"] + " must be between " + str(spec["min"]) + " and " + str(spec["max"]) + ".")
            if spec.get("step") == 1 or key == "INFLUX_TIMEOUT_MS":
                if int(value) != value:
                    raise SettingsError(spec["label"] + " must be a whole number.")
                result[key] = int(value)
        else:
            if not isinstance(value, str) or len(value.encode("utf-8")) > 8192 or "\x00" in value:
                raise SettingsError(spec["label"] + " has an invalid value.")
            if key not in SECRET_FIELDS:
                result[key] = value.strip()
            if key in SECRET_FIELDS and any(c in value for c in "\r\n"):
                raise SettingsError(spec["label"] + " must be a single line.")
    for key in ("INFLUX_ORG", "INFLUX_BUCKET", "MQTT_BROKER", "MQTT_CLIENT_ID", "TIMEZONE"):
        if not result[key] or any(c in result[key] for c in "\r\n"):
            raise SettingsError(FIELDS[key]["label"] + " is required and must be a single line.")
    broker = result["MQTT_BROKER"]
    if any(c.isspace() for c in broker) or any(c in broker for c in "/?#@"):
        raise SettingsError("Broker hostname must not include a scheme, path or credentials.")
    # Raw IPv6 is accepted; IPv4/hostname values must not include :port.
    if ":" in broker:
        import ipaddress
        try:
            ipaddress.IPv6Address(broker.strip("[]"))
        except ValueError:
            raise SettingsError("Enter the MQTT port in its separate port field.") from None
        result["MQTT_BROKER"] = broker.strip("[]")
    if len(result["MQTT_CLIENT_ID"].encode()) > 128:
        raise SettingsError("Client ID must be at most 128 bytes.")
    ca = result["MQTT_CA_FILE"]
    if ca and (not Path(ca).is_absolute() or "\n" in ca or "\r" in ca):
        raise SettingsError("Custom CA certificate path must be absolute inside the container.")
    result["INFLUX_URL"] = _url(result["INFLUX_URL"], "InfluxDB address")
    result["PUBLIC_BASE_URL"] = _url(result["PUBLIC_BASE_URL"], "Public address", optional=True)
    urls = [part.strip() for part in result["FRIGATE_URLS"].replace("\n", ",").split(",") if part.strip()]
    if len(urls) > 8:
        raise SettingsError("Use no more than eight Frigate addresses.")
    result["FRIGATE_URLS"] = ",".join(dict.fromkeys(_url(url, "Frigate address") for url in urls))
    topics = [part.strip() for part in result["BIRDNET_TOPICS"].replace("\n", ",").split(",") if part.strip()]
    if not topics or len(topics) > 32:
        raise SettingsError("Supply between one and 32 BirdNET topic filters.")
    result["BIRDNET_TOPICS"] = ",".join(dict.fromkeys(_topic(t, "BirdNET topic", True) for t in topics))
    for key in ("FRIGATE_TOPIC_PREFIX", "HA_DISCOVERY_PREFIX"):
        result[key] = _topic(result[key].strip("/"), FIELDS[key]["label"])
    missing = result["BIRDNET_MISSING_CONFIDENCE"]
    if missing:
        try:
            confidence = float(missing)
            if not math.isfinite(confidence) or not 0 <= confidence <= 1:
                raise ValueError
        except ValueError:
            raise SettingsError("Missing confidence must be blank or a number between 0 and 1.") from None
    try:
        zoneinfo.ZoneInfo(result["TIMEZONE"])
    except (ValueError, zoneinfo.ZoneInfoNotFoundError):
        raise SettingsError("Time zone must be an installed IANA name, such as Europe/London.") from None
    return result


def load_settings():
    with _LOCK:
        saved = _read_json(data_dir() / "settings.json")
        if saved is None:
            return dict(DEFAULTS), "initial"
        if saved.get("version") != 1 or not isinstance(saved.get("revision"), str) or len(saved["revision"]) > 128:
            raise SettingsError("Saved settings have an unsupported format.")
        return validate_values(saved.get("values")), saved["revision"]


def apply_environment():
    """Read managed connection settings before the legacy module creates globals."""
    global _ACTIVE_REVISION
    values, _ACTIVE_REVISION = load_settings()
    for key, value in values.items():
        os.environ[key] = ("1" if value else "0") if type(value) is bool else str(value)
    return _ACTIVE_REVISION


def active_revision():
    """The revision actually loaded by this process, not merely saved on disk."""
    return _ACTIVE_REVISION


def merged_values(payload):
    supplied = payload.get("values", {})
    cleared = payload.get("clear_secrets", [])
    if not isinstance(supplied, dict) or set(supplied) - SETTINGS_ENV_KEYS:
        raise SettingsError("Unknown settings fields were supplied.")
    if not isinstance(cleared, list) or not all(isinstance(k, str) and k in SECRET_FIELDS for k in cleared):
        raise SettingsError("Only saved credentials may be explicitly cleared.")
    values, revision = load_settings()
    for key, value in supplied.items():
        if key in SECRET_FIELDS and value == "":
            continue
        values[key] = value
    for key in cleared:
        values[key] = ""
    return validate_values(values), revision


def save_settings(payload):
    with _LOCK:
        values, revision = merged_values(payload)
        if payload.get("revision") != revision:
            raise SettingsError("Settings changed in another window. Reload before saving.")
        next_revision = secrets.token_hex(16)
        _write_json(data_dir() / "settings.json", {"version": 1, "revision": next_revision, "values": values})
        return next_revision


def mark_restart(revision):
    _atomic(data_dir() / "restart.request", (revision + "\n").encode())


def _b64(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _unb64(value):
    return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)


def _admin():
    value = _read_json(data_dir() / "admin.json")
    if value is None or value.get("version") != 1:
        raise SettingsError("Administrator storage is unavailable.")
    try:
        if len(_unb64(value["signing_key"])) != 32 or not isinstance(value["epoch"], str):
            raise ValueError
    except (ValueError, KeyError, TypeError):
        raise SettingsError("Administrator storage has an unexpected format.") from None
    return value


def initialize_admin():
    """Create private setup credentials on startup; never from an HTTP request."""
    with _LOCK:
        root = data_dir()
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        value = _read_json(root / "admin.json")
        if value is None:
            value = {"version": 1, "signing_key": _b64(secrets.token_bytes(32)),
                     "epoch": secrets.token_hex(16), "password": None}
            _write_json(root / "admin.json", value)
        else:
            value = _admin()
        if not value.get("password"):
            path = root / "setup-code.txt"
            if path.is_symlink():
                raise SettingsError("Setup code must be a regular file.")
            if not path.exists():
                _atomic(path, (secrets.token_urlsafe(24) + "\n").encode())
            code = path.read_text().strip()
            if len(code) < 24 or len(code) > 128:
                raise SettingsError("Setup code has an unexpected format.")
            LOG.warning("Garden Friends first-run setup: open /settings and use setup code %s", code)
        elif (root / "setup-code.txt").exists():
            (root / "setup-code.txt").unlink()


def _password_hash(password):
    if not isinstance(password, str) or not 12 <= len(password) <= 1024:
        raise SettingsError("Use an administrator password of 12 to 1024 characters.")
    salt = secrets.token_bytes(24)
    hashed = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, PASSWORD_ITERATIONS)
    return {"algorithm": "pbkdf2-sha256", "iterations": PASSWORD_ITERATIONS,
            "salt": _b64(salt), "hash": _b64(hashed)}


def _password_matches(password, stored):
    if not isinstance(password, str) or not 1 <= len(password) <= 1024 or not isinstance(stored, dict):
        return False
    try:
        if stored["algorithm"] != "pbkdf2-sha256" or stored["iterations"] != PASSWORD_ITERATIONS:
            return False
        digest = hashlib.pbkdf2_hmac("sha256", password.encode(), _unb64(stored["salt"]), stored["iterations"])
        return hmac.compare_digest(digest, _unb64(stored["hash"]))
    except (KeyError, ValueError, TypeError):
        return False


def _new_token(admin, role):
    payload = {"role": role, "exp": int(time.time()) + SESSION_SECONDS,
               "nonce": secrets.token_hex(16), "epoch": admin["epoch"]}
    body = _b64(json.dumps(payload, separators=(",", ":")).encode())
    signature = _b64(hmac.new(_unb64(admin["signing_key"]), body.encode(), hashlib.sha256).digest())
    return body + "." + signature


def _valid_token(admin, token):
    if not isinstance(token, str) or len(token) > 2048:
        return None
    try:
        body, signature = token.split(".")
        expected = hmac.new(_unb64(admin["signing_key"]), body.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(_unb64(signature), expected):
            return None
        payload = json.loads(_unb64(body))
        if not isinstance(payload, dict):
            return None
        if (type(payload.get("exp")) is not int or not time.time() < payload["exp"] <= time.time() + SESSION_SECONDS + 5 or
                payload.get("epoch") != admin["epoch"] or payload.get("role") not in {"admin", "anonymous"}):
            return None
        if payload["role"] == "admin" and not admin.get("password"):
            return None
        return payload
    except (ValueError, TypeError, KeyError, UnicodeError):
        return None


def _csrf(admin, token):
    return hmac.new(_unb64(admin["signing_key"]), ("csrf\0" + token).encode(), hashlib.sha256).hexdigest()


class _RateLimiter:
    def __init__(self):
        self.entries = OrderedDict()
        self.lock = threading.Lock()

    def allow(self, key, limit=8, seconds=60):
        now = time.monotonic()
        with self.lock:
            used = [t for t in self.entries.get(key, []) if now - t < seconds]
            if len(used) >= limit:
                return False
            self.entries[key] = used + [now]
            self.entries.move_to_end(key)
            while len(self.entries) > 4096:
                self.entries.popitem(last=False)
            return True


_RATE = _RateLimiter()


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _request(request, timeout=8, limit=65536):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
    with opener.open(request, timeout=timeout) as response:
        data = response.read(limit + 1)
        if len(data) > limit:
            raise SettingsError("The service response was larger than expected.")
        return response.status, response.headers.get("Content-Type", ""), data


def _test_frigate(values, budget=20):
    """Bound the whole fallback list, including slow DNS/HTTP responses."""
    urls = [url for url in values["FRIGATE_URLS"].split(",") if url]
    if not urls:
        raise SettingsError("Enter at least one Frigate address first.")
    finished = threading.Event()
    cancelled = threading.Event()
    result = []
    deadline = time.monotonic() + budget

    def probe():
        try:
            for url in urls:
                remaining = deadline - time.monotonic()
                if cancelled.is_set() or remaining <= 0:
                    break
                headers = {"Accept": "application/json,text/plain"}
                if values["FRIGATE_AUTH_TOKEN"]:
                    headers["Authorization"] = "Bearer " + values["FRIGATE_AUTH_TOKEN"]
                try:
                    status, content_type, body = _request(
                        urllib.request.Request(url + "/api/version", headers=headers),
                        timeout=min(8, remaining), limit=4096)
                    if status == 200 and body.strip() and "html" not in content_type.lower() and not body.lstrip().startswith(b"<"):
                        result.append({"ok": True, "message": "Frigate's version endpoint responded successfully."})
                        return
                except (urllib.error.URLError, TimeoutError, OSError, SettingsError):
                    continue
            result.append({"ok": False, "message": "None of the Frigate addresses returned its version endpoint. Check the address and authentication."})
        except Exception:
            result.append({"ok": False, "message": "The Frigate connection could not be completed. Check the address, network and authentication."})
        finally:
            finished.set()

    # A daemon prevents a stalled resolver from holding the request or shutdown
    # open. Cancellation stops further fallback requests after the time budget.
    threading.Thread(target=probe, name="garden-frigate-probe", daemon=True).start()
    if not finished.wait(budget):
        cancelled.set()
        raise SettingsError("The Frigate connection checks timed out. Check the addresses and network.")
    return result[0]


def test_connection(connection, values):
    """Probe configured services without changing databases, subscriptions or state."""
    try:
        if connection == "influx":
            if not values["INFLUX_TOKEN"]:
                raise SettingsError("Enter an InfluxDB API token first.")
            query = ('from(bucket: ' + json.dumps(values["INFLUX_BUCKET"]) + ')\n'
                     '  |> range(start: -1m)\n  |> limit(n: 1)\n  |> keep(columns: ["_time"])\n  |> group()\n  |> limit(n: 1)')
            payload = json.dumps({"query": query, "type": "flux"}).encode()
            request = urllib.request.Request(values["INFLUX_URL"] + "/api/v2/query?" +
                                             urllib.parse.urlencode({"org": values["INFLUX_ORG"]}), data=payload,
                                             headers={"Authorization": "Token " + values["INFLUX_TOKEN"],
                                                      "Content-Type": "application/json", "Accept": "application/csv"})
            status, content_type, _ = _request(request, timeout=min(10, values["INFLUX_TIMEOUT_MS"] / 1000))
            if status != 200 or "html" in content_type.lower():
                raise SettingsError("InfluxDB did not accept the bucket query.")
            return {"ok": True, "message": "InfluxDB accepted a read query for this organisation and bucket."}
        if connection == "frigate":
            return _test_frigate(values)
        if connection == "mqtt":
            import paho.mqtt.client as mqtt
            done = threading.Event()
            accepted = []
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="garden-friends-probe-" + secrets.token_hex(6))
            client.connect_timeout = 5
            if values["MQTT_USERNAME"]:
                client.username_pw_set(values["MQTT_USERNAME"], values["MQTT_PASSWORD"])
            if values["MQTT_TLS"]:
                client.tls_set_context(ssl.create_default_context(cafile=values["MQTT_CA_FILE"] or None))
            def connected(_client, _userdata, _flags, reason_code, _properties=None):
                accepted.append(reason_code == 0)
                done.set()
            client.on_connect = connected
            client.on_connect_fail = lambda *_args: done.set()
            try:
                client.connect_async(values["MQTT_BROKER"], values["MQTT_PORT"], keepalive=15)
                client.loop_start()
                done.wait(8)
                if not accepted or not accepted[0]:
                    raise SettingsError("MQTT did not accept the connection. Check the address, port, credentials and TLS settings.")
            finally:
                client.disconnect()
                client.loop_stop()
            return {"ok": True, "message": "The MQTT broker accepted the connection. No messages were published or subscribed to."}
        raise SettingsError("Choose InfluxDB, MQTT or Frigate to test.")
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            return {"ok": False, "message": "The service rejected the credentials or their permissions."}
        return {"ok": False, "message": "The service returned HTTP " + str(exc.code) + ". Check its address and configuration."}
    except SettingsError as exc:
        return {"ok": False, "message": str(exc)}
    except (urllib.error.URLError, TimeoutError, OSError, ValueError, ImportError):
        return {"ok": False, "message": "The connection could not be completed. Check the service address, network, TLS and credentials."}


class GardenSettingsMixin:
    """Add to AvianWebHandler; call handle_garden_request at start of handle_get."""

    def _gf_send(self, value, status=200, content_type="application/json; charset=utf-8", cookie=None):
        content = value if isinstance(value, bytes) else json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
        self.send_response(status)
        for key, val in (("Content-Type", content_type), ("Content-Length", str(len(content))),
                         ("Cache-Control", "no-store"), ("Pragma", "no-cache"),
                         ("X-Content-Type-Options", "nosniff"), ("Referrer-Policy", "same-origin"),
                         ("X-Frame-Options", "DENY"),
                         ("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")):
            self.send_header(key, val)
        if cookie:
            self.send_header("Set-Cookie", cookie)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(content)

    def _gf_cookie(self, token, clear=False):
        secure = self.headers.get("Origin", "").startswith("https://")
        return (COOKIE + "=" + token + "; Path=/; HttpOnly; SameSite=Strict; Max-Age=" +
                ("0" if clear else str(SESSION_SECONDS)) + ("; Secure" if secure else ""))

    def _gf_session(self):
        admin = _admin()
        cookie = SimpleCookie()
        try:
            cookie.load(self.headers.get("Cookie", ""))
        except Exception:
            pass
        token = cookie[COOKIE].value if COOKIE in cookie else ""
        payload = _valid_token(admin, token)
        return admin, token, payload

    def _gf_session_body(self, admin, token, payload):
        return {"authenticated": bool(payload and payload.get("role") == "admin"),
                "setup_required": not bool(admin.get("password")), "csrf_token": _csrf(admin, token)}

    def _gf_origin(self):
        origin = self.headers.get("Origin", "")
        try:
            parsed = urllib.parse.urlsplit(origin)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.path or parsed.query or parsed.fragment or parsed.username:
                return False
            allowed = {self.headers.get("Host", "").lower()}
            current, _ = load_settings()
            if current["PUBLIC_BASE_URL"]:
                public = urllib.parse.urlsplit(current["PUBLIC_BASE_URL"])
                if parsed.scheme == public.scheme:
                    allowed.add(public.netloc.lower())
            return parsed.netloc.lower() in allowed
        except (ValueError, SettingsError):
            return False

    def _gf_require(self, admin_only=True):
        admin, token, payload = self._gf_session()
        if not payload:
            self._gf_send({"error": "Your session has expired. Reload Settings and sign in again."}, 401)
            return None
        if admin_only and payload.get("role") != "admin":
            self._gf_send({"error": "Sign in to edit connection settings."}, 401)
            return None
        if self.command not in {"GET", "HEAD"}:
            supplied = self.headers.get("X-CSRF-Token", "")
            if not self._gf_origin() or not hmac.compare_digest(supplied.encode("utf-8"), _csrf(admin, token).encode("ascii")):
                self._gf_send({"error": "The request could not be verified. Reload Settings and try again."}, 403)
                return None
        return admin, token, payload

    def handle_garden_request(self):
        path = urllib.parse.urlsplit(self.path).path
        static = {"/settings": ("settings.html", "text/html; charset=utf-8"),
                  "/settings/": ("settings.html", "text/html; charset=utf-8"),
                  "/static/garden-settings.js": ("garden-settings.js", "text/javascript; charset=utf-8"),
                  "/static/garden-settings.css": ("garden-settings.css", "text/css; charset=utf-8")}
        if path not in static and not path.startswith("/api/settings"):
            return False
        try:
            if path in static:
                name, content_type = static[path]
                self._gf_send((APP_DIR / "static" / name).read_bytes(), content_type=content_type)
            elif path == "/api/settings/session":
                admin, token, payload = self._gf_session()
                created = not payload
                if created:
                    token = _new_token(admin, "anonymous")
                    payload = _valid_token(admin, token)
                self._gf_send(self._gf_session_body(admin, token, payload), cookie=self._gf_cookie(token) if created else None)
            elif path == "/api/settings":
                if self._gf_require():
                    values, revision = load_settings()
                    self._gf_send({"values": {k: v for k, v in values.items() if k not in SECRET_FIELDS},
                                   "secrets": {k: bool(values[k]) for k in SECRET_FIELDS},
                                   "schema": SCHEMA, "revision": revision})
            else:
                self._gf_send({"error": "Not found"}, 404)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except (SettingsError, OSError):
            self._gf_send({"error": "Settings are unavailable. Check the private data folder and server logs."}, 503)
        return True

    def _gf_body(self):
        if self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() != "application/json":
            raise SettingsError("Send a JSON request.")
        if self.headers.get("Transfer-Encoding"):
            raise SettingsError("This request encoding is not supported.")
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 65536:
                raise ValueError
            self.connection.settimeout(10)
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ValueError
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError
            return value
        except (ValueError, UnicodeError):
            raise SettingsError("The request must be a JSON object smaller than 64 KiB.") from None

    def do_POST(self):
        path = urllib.parse.urlsplit(self.path).path
        routes = {"/api/settings/setup", "/api/settings/login", "/api/settings/logout",
                  "/api/settings/save", "/api/settings/test", "/api/settings/password"}
        if path not in routes:
            self._gf_send({"error": "Not found"}, 404)
            return
        try:
            authenticated = self._gf_require(admin_only=path not in {"/api/settings/setup", "/api/settings/login"})
            if not authenticated:
                return
            admin, token, session = authenticated
            if path in {"/api/settings/setup", "/api/settings/login", "/api/settings/password", "/api/settings/test"}:
                # Do not trust X-Forwarded-For to bypass the login limit.
                if not _RATE.allow((self.client_address[0], "test" if path.endswith("test") else "authentication")):
                    self._gf_send({"error": "Too many attempts. Wait a minute before trying again."}, 429)
                    return
            payload = self._gf_body()
            if path == "/api/settings/setup":
                with _LOCK:
                    admin = _admin()
                    if admin.get("password"):
                        self._gf_send({"error": "Administrator setup is already complete."}, 409)
                        return
                    supplied = payload.get("setup_code", "")
                    expected = (data_dir() / "setup-code.txt").read_text().strip()
                    if not isinstance(supplied, str) or not hmac.compare_digest(supplied.encode("utf-8"), expected.encode("utf-8")):
                        self._gf_send({"error": "The setup code is incorrect."}, 403)
                        return
                    admin["password"] = _password_hash(payload.get("password"))
                    admin["epoch"] = secrets.token_hex(16)
                    _write_json(data_dir() / "admin.json", admin)
                    (data_dir() / "setup-code.txt").unlink(missing_ok=True)
                self._gf_authenticated(admin)
            elif path == "/api/settings/login":
                if not _password_matches(payload.get("password"), admin.get("password")):
                    self._gf_send({"error": "The administrator password is incorrect."}, 401)
                    return
                self._gf_authenticated(admin)
            elif path == "/api/settings/logout":
                self._gf_send({"ok": True}, cookie=self._gf_cookie("", clear=True))
            elif path == "/api/settings/password":
                with _LOCK:
                    admin = _admin()
                    if not _password_matches(payload.get("current_password"), admin.get("password")):
                        self._gf_send({"error": "The current password is incorrect."}, 401)
                        return
                    admin["password"] = _password_hash(payload.get("password"))
                    admin["epoch"] = secrets.token_hex(16)
                    _write_json(data_dir() / "admin.json", admin)
                self._gf_authenticated(admin)
            elif path == "/api/settings/save":
                revision = save_settings(payload)
                try:
                    self._gf_send({"ok": True, "restarting": True, "revision": revision})
                    self.wfile.flush()
                finally:
                    # A browser disconnect after a successful save must not leave
                    # the process silently running its old connection settings.
                    try:
                        mark_restart(revision)
                    except (SettingsError, OSError) as exc:
                        LOG.error("Settings saved, but the restart request failed (%s); restart Garden Friends to activate the saved revision.", type(exc).__name__)
            elif path == "/api/settings/test":
                values, _ = merged_values(payload)
                self._gf_send(test_connection(payload.get("connection"), values))
        except SettingsError as exc:
            self._gf_send({"error": str(exc)}, 400)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:
            LOG.error("Settings request failed (%s)", type(exc).__name__)
            self._gf_send({"error": "The settings operation could not be completed. Check the server logs."}, 503)

    def _gf_authenticated(self, admin):
        token = _new_token(admin, "admin")
        self._gf_send(self._gf_session_body(admin, token, _valid_token(admin, token)), cookie=self._gf_cookie(token))
