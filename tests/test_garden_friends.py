"""Public release contracts: calendar days, connection hooks and page entry points."""
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pytest
import main as app


@pytest.mark.parametrize('zone,when,start,stop', [
    ('Europe/London', '2026-11-18T00:00:00+00:00', '2026-11-18T00:00:00Z', '2026-11-19T00:00:00Z'),
    ('Europe/London', '2026-11-18T23:59:59+00:00', '2026-11-18T00:00:00Z', '2026-11-19T00:00:00Z'),
    ('Europe/London', '2026-03-29T12:00:00+01:00', '2026-03-29T00:00:00Z', '2026-03-29T23:00:00Z'),
    ('Europe/London', '2026-10-25T12:00:00+00:00', '2026-10-24T23:00:00Z', '2026-10-26T00:00:00Z'),
    ('America/Los_Angeles', '2026-11-18T12:00:00-08:00', '2026-11-18T08:00:00Z', '2026-11-19T08:00:00Z'),
    ('UTC', '2026-11-18T22:30:00+00:00', '2026-11-18T00:00:00Z', '2026-11-19T00:00:00Z'),
])
def test_calendar_day_is_exact_and_timezone_aware(monkeypatch, zone, when, start, stop):
    monkeypatch.setattr(app, 'TZ', ZoneInfo(zone))
    monkeypatch.setattr(app, 'TIMEZONE', zone)
    result = app.get_observation_window('today', 0, dt.datetime.fromisoformat(when))
    assert result[:2] == (start, stop)
    assert result[4].hour == result[5].hour == 0
    assert 'Midnight to midnight' in result[3] and zone in result[3]


def test_previous_day_does_not_overlap_current_day():
    now = dt.datetime(2026, 11, 18, 0, 0, 1, tzinfo=app.TZ)
    yesterday = app.get_observation_window('yesterday', 0, now)
    today = app.get_observation_window('today', 0, now)
    assert yesterday[1] == today[0]
    assert yesterday[4].date() == dt.date(2026, 11, 17)
    assert today[4].date() == dt.date(2026, 11, 18)


def test_today_query_has_exclusive_next_midnight_stop(monkeypatch):
    window = app.get_observation_window('today', 0, dt.datetime(2026, 11, 18, 12, tzinfo=app.TZ))
    monkeypatch.setattr(app, 'get_observation_window', lambda *_: window)
    query = Mock(return_value=[])
    monkeypatch.setattr(app, 'query_records', query)
    monkeypatch.setattr(app, 'artwork_index', lambda: ({}, {}))
    app.clear_query_cache()
    app.get_active_species('today', 0)
    flux = query.call_args.args[0]
    assert 'range(start: 2026-11-18T00:00:00Z, stop: 2026-11-19T00:00:00Z)' in flux


def test_home_assistant_uses_configured_discovery_prefix(monkeypatch):
    client = Mock()
    monkeypatch.setattr(app, 'mqtt_client', client)
    monkeypatch.setattr(app, 'READ_ONLY', False)
    monkeypatch.setattr(app, 'HA_ENABLED', True)
    monkeypatch.setattr(app, 'HA_DISCOVERY_PREFIX', 'my-home/discovery')
    app.publish_ha_discovery()
    assert client.publish.call_count == 6
    assert all(c.args[0].startswith('my-home/discovery/') for c in client.publish.call_args_list)
    assert all('wildlife/' in json.loads(c.args[1]).get('state_topic', json.loads(c.args[1]).get('topic', ''))
               for c in client.publish.call_args_list)
    client.reset_mock()
    monkeypatch.setattr(app, 'HA_ENABLED', False)
    app.publish_ha_discovery()
    app.mqtt_publish('wildlife/recent_detection', '{}')
    client.publish.assert_not_called()


def test_settings_and_profile_entry_points_belong_to_public_page():
    assert '<title>Garden Friends</title>' in app.HTML_PAGE
    assert 'href="/settings"' in app.HTML_PAGE
    assert app.HTML_PAGE.count('id="avian-github-link"') == 1
    assert 'href="https://github.com/ranolph"' in app.HTML_PAGE
    assert "get('view') === 'statistics'" in app.HTML_PAGE
    assert '22:30' not in app.HTML_PAGE
    assert 'AVIAN_GEMINI_AREA_RATIO_V1' in app.HTML_PAGE
    assert 'AVIAN_DESKTOP_COLLAGE_FILL_V2' in app.HTML_PAGE


def test_webp_is_preferred_without_removing_original_masks(monkeypatch, tmp_path):
    monkeypatch.setattr(app, 'ART_DIR', str(tmp_path))
    png = tmp_path / 'robin.png'
    webp = tmp_path / 'robin.webp'
    png.write_bytes(b'original placeholder bytes')
    webp.write_bytes(b'webp placeholder bytes')
    mask = {'w': 1, 'h': 1, 'aspect': 1, 'bitmap': ['1']}
    index = ({'robin': (png, png.stat().st_mtime_ns, png.stat().st_size)}, {'robin': mask})
    result = app.resolve_art('Robin', 'Erithacus rubecula', index)
    assert result['image_url'].startswith('/art/robin.webp?v=')
    assert result['image_mask_key'] == 'robin'
    assert png.exists()


def test_release_webp_freshness_survives_checkout_timestamps(monkeypatch, tmp_path):
    art = tmp_path / 'species_art'
    art.mkdir()
    monkeypatch.setattr(app, 'ART_DIR', str(art))
    png, webp = art / 'robin.png', art / 'robin.webp'
    png.write_bytes(b'original PNG contents')
    webp.write_bytes(b'current WebP contents')
    records = {p.name: {'bytes': p.stat().st_size, 'sha256': hashlib.sha256(p.read_bytes()).hexdigest(), 'mtime_ns': 100}
               for p in (png, webp)}
    (tmp_path / 'ARTWORK_MANIFEST.json').write_text(json.dumps({'files': records}))
    os.utime(webp, ns=(200, 200))
    os.utime(png, ns=(300, 300))
    assert app.webp_is_current(png, webp)
    png.write_bytes(b'changed PNG contents!')
    assert not app.webp_is_current(png, webp)
