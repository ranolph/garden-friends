"""Core wildlife regression tests. No real MQTT, Influx or Frigate writes.

Run: python -m pytest -q tests
Requires pytest and Pillow in addition to the application requirements.
All images and detections in these tests are synthetic fixtures, not recovered data.
"""
import datetime as dt
import importlib.util
import io
import json
import random
import threading
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from PIL import Image

SPEC = importlib.util.spec_from_file_location("wildlife_under_test", Path(__file__).resolve().parents[1] / "main.py")
app = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(app)


class FakePoint:
    def __init__(self, measurement):
        self.measurement, self.tags, self.fields = measurement, {}, {}
    def tag(self, name, value):
        self.tags[name] = value
        return self
    def field(self, name, value):
        self.fields[name] = value
        return self
    def time(self, when, precision):
        self.when, self.precision = when, precision
        return self


class FakeRecord:
    def __init__(self, name, scientific="", source="audio_birdnet", timestamp=None, confidence=.9):
        self.values = dict(common_name=name, scientific_name=scientific, source=source,
                           device="fixture", model="fixture")
        self.timestamp = timestamp or dt.datetime.now(app.UTC)
        self.confidence = confidence
    def get_time(self):
        return self.timestamp
    def get_value(self):
        return self.confidence


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    monkeypatch.setattr(app, "ART_DIR", str(tmp_path / "species_art"))
    monkeypatch.setattr(app, "MASKS_FILE", str(tmp_path / "species_art" / "masks.json"))
    monkeypatch.setattr(app, "SNAPSHOT_DIR", tmp_path / "snapshots")
    monkeypatch.setattr(app, "PUBLIC_BASE_URL", "http://127.0.0.1:8090")
    monkeypatch.setattr(app, "READ_ONLY", False)
    monkeypatch.setattr(app, "INFLUX_TOKEN", "fixture-not-a-real-token")
    monkeypatch.setattr(app, "Point", FakePoint)
    monkeypatch.setattr(app, "WritePrecision", SimpleNamespace(NS="ns"))
    monkeypatch.setattr(app, "write_api", Mock())
    monkeypatch.setattr(app, "influx_query_api", Mock())
    monkeypatch.setattr(app, "mqtt_client", Mock())
    monkeypatch.setattr(app, "fetch_and_publish_snapshot_binary", Mock())
    monkeypatch.setattr(app, "fetch_frigate_score_and_process", Mock())
    app.stop_event.clear()
    app.summary_needed.clear()
    app.logged_events.clear()
    app.logged_event_times.clear()
    app.inflight_events.clear()
    app.pending_frigate_scores.clear()
    app.fetching_frigate.clear()
    app.clear_query_cache()
    app.art_cache.update(stamp=None, files={}, masks={})
    app.latest_visual_payload.clear()
    monkeypatch.setattr(app, "latest_visual_event", "")
    monkeypatch.setattr(app, "latest_snapshot_event", "")
    yield
    app.stop_event.set()


@pytest.mark.parametrize("common,scientific,expected", [
    ("Hooded Crow", "Corvus cornix", "Carrion Crow"),
    ("Corvus cornix", "", "Carrion Crow"),
    ("hooded crow", "", "Carrion Crow"),
    ("Robin", "", "European Robin"),
    ("European Robin", "Erithacus rubecula", "European Robin"),
    ("Blackcap", "Sylvia atricapilla", "Blackcap"),
    ("Treecreeper", "", "Eurasian Treecreeper"),
    ("Leisler's Bat", "", "Lesser Noctule / Leisler's Bat"),
    ("Nyctalus leisleri", "", "Lesser Noctule / Leisler's Bat"),
    ("Unknown species name", "Nyctalus leisleri", "Lesser Noctule / Leisler's Bat"),
    ("Common Crossbill", "Loxia curvirostra", "Common Crossbill"),
    ("Kingfisher", "Alcedo atthis", "Kingfisher"),
])
def test_species_aliases(common, scientific, expected):
    assert app.normalize_species(common, scientific)[0] == expected


@pytest.mark.parametrize("scientific", list(app.BAT_SYNONYMS))
def test_every_documented_bat(scientific):
    common, sci = app.normalize_species(scientific)
    assert common == app.BAT_SYNONYMS[scientific][0]
    assert app.get_target_type(common, sci) == "bat"


@pytest.mark.parametrize("name", ["None", "No animal", "no bird", "domestic cat", "dog", "person", "Bird", "Bat", "Animal", "Background only", "", "<script>alert(1)</script>"])
def test_reject_negative_and_generic(name):
    assert app.normalize_species(name)[0] is None


def test_empty_scientific_is_not_a_negative():
    assert app.normalize_species("Chaffinch", "")[0] == "Chaffinch"
    assert not app.is_negative_response("Saxicola rubicola")


@pytest.mark.parametrize("raw,expected", [(0,0),(.75,.75),(1,1),(".9",.9),(None,None),(-1,None),(101,None),(float("nan"),None),(float("inf"),None),("wrong",None)])
def test_confidence(raw,expected):
    assert app.valid_confidence(raw) == expected


def test_species_description():
    assert app.parse_species_description("European Robin (Erithacus rubecula)") == ("European Robin", "Erithacus rubecula")
    assert app.parse_species_description("**Red Fox (Vulpes vulpes)**")[0] == "Red Fox"
    assert app.parse_species_description("No animal (plants)") == (None,None)


def test_point_schema_and_timestamp():
    assert app.write_to_influx("audio_birdnet", "bird", "Hooded Crow", "Corvus cornix", .9, "BirdNET-Go", "mic")
    call = app.write_api.write.call_args.kwargs
    assert call["bucket"] == "catalog" and call["org"] == "wildlife"
    point = call["record"]
    assert point.measurement == "wildlife_detection"
    assert point.tags == dict(source="audio_birdnet", target_type="bird", common_name="Carrion Crow", scientific_name="Corvus corone", model="BirdNET-Go", device="mic")
    assert point.fields == {"confidence": .9}
    assert point.when.tzinfo == app.UTC


def test_visual_threshold_and_duplicates():
    desc = "European Robin (Erithacus rubecula)"
    assert not app.process_frigate_visual("e1",desc,.749,"garden_camera")
    assert "e1" not in app.logged_events
    assert app.process_frigate_visual("e1",desc,.75,"garden_camera")
    assert not app.process_frigate_visual("e1",desc,.99,"garden_camera")
    assert app.write_api.write.call_count == 1
    assert app.fetch_and_publish_snapshot_binary.call_count == 1


def test_failed_write_not_deduplicated_forever():
    app.write_api.write.side_effect = [RuntimeError("fixture offline"), None]
    args=("e1","European Robin (Erithacus rubecula)",.8,"garden_camera")
    assert not app.process_frigate_visual(*args)
    assert "e1" not in app.logged_events and "e1" not in app.inflight_events
    assert app.process_frigate_visual(*args)
    assert "e1" in app.logged_events


def test_ha_failure_does_not_duplicate_saved_point(monkeypatch):
    monkeypatch.setattr(app,"publish_ha_detection",Mock(side_effect=RuntimeError("fixture MQTT")))
    assert app.process_frigate_visual("e1","Robin",.8,"garden")
    assert not app.process_frigate_visual("e1","Robin",.8,"garden")
    assert app.write_api.write.call_count == 1


def test_mammals_default_75_optional_55(monkeypatch):
    assert not app.process_frigate_visual("m1","European Hedgehog (Erinaceus europaeus)",.62,"garden")
    monkeypatch.setattr(app,"FRIGATE_MAMMAL_MIN_SCORE",.55)
    assert app.process_frigate_visual("m1","European Hedgehog (Erinaceus europaeus)",.62,"garden")
    assert not app.process_frigate_visual("b1","Robin",.62,"garden")


def test_sublabel_score_cannot_bypass_detector_gate():
    event={"score":.51,"data":{"sub_label_score":.99},"snapshot":{"score":.65}}
    assert app.detector_score(event) == .65
    assert app.detector_score({"sub_label_score":.99}) is None


def test_two_stage_frigate_score_first():
    app.handle_message("frigate/events",{"after":{"id":"e1","camera":"garden_camera","top_score":.82}})
    app.write_api.write.assert_not_called()
    app.handle_message("frigate/tracked_object_update",{"type":"description","id":"e1","description":"Robin (Erithacus rubecula)"})
    assert app.write_api.write.call_count == 1
    assert app.write_api.write.call_args.kwargs["record"].fields["confidence"] == .82


def test_two_stage_frigate_description_first():
    app.handle_message("frigate/tracked_object_update",{"type":"description","id":"e1","description":"Robin (Erithacus rubecula)"})
    app.write_api.write.assert_not_called()
    app.handle_message("frigate/events",{"after":{"id":"e1","camera":"garden_camera","top_score":.82}})
    assert app.write_api.write.call_count == 1


def test_later_high_score_not_lost():
    for score in (.65,.81):
        app.handle_message("frigate/events",{"after":{"id":"e1","top_score":score,"description":"Robin"}})
    assert app.write_api.write.call_count == 1


def test_false_positive_ignored():
    app.handle_message("frigate/events",{"after":{"id":"e1","top_score":.9,"description":"Robin","false_positive":True}})
    app.write_api.write.assert_not_called()


def test_reviews_fetch_actual_detection_ids():
    app.handle_message("frigate/reviews",{"after":{"id":"review_not_event","camera":"garden","data":{"detections":["event1","event2"]}}})
    calls=app.fetch_frigate_score_and_process.call_args_list
    assert [c.args[0] for c in calls] == ["event1","event2"]


@pytest.mark.parametrize("topic",["birdnet","birdnet-go","birdnet/detections","birdnet-go/observations"])
def test_audio_topics(topic):
    app.handle_message(topic,json.dumps({"CommonName":"Robin","ScientificName":"Erithacus rubecula","Confidence":.9}).encode())
    assert app.write_api.write.call_count == 1


def test_audio_confidence_zero_preserved():
    app.process_birdnet_payload({"common_name":"Robin","confidence":0})
    assert app.write_api.write.call_args.kwargs["record"].fields["confidence"] == 0


def test_missing_audio_confidence_legacy_default_and_optout(monkeypatch):
    app.process_birdnet_payload({"common_name":"Robin"})
    assert app.write_api.write.call_args.kwargs["record"].fields["confidence"] == .85
    app.write_api.reset_mock()
    monkeypatch.setattr(app,"BIRDNET_MISSING_CONFIDENCE","")
    assert not app.process_birdnet_payload({"common_name":"Robin"})
    app.write_api.write.assert_not_called()


@pytest.mark.parametrize("payload,source",[
    ({"species":"Robin","model_id":"perch_v2","confidence":.8},"audio_perch"),
    ({"species":"Nyctalus leisleri","model_id":"bat","confidence":.8},"audio_bat_classifier"),
    ({"species":"Robin","model_id":"birdnet","confidence":.8},"audio_birdnet"),
])
def test_audio_source(payload,source):
    app.process_birdnet_payload(payload)
    assert app.write_api.write.call_args.kwargs["record"].tags["source"] == source


@pytest.mark.parametrize("topic,payload",[("birdnet/stats",b'{}'),("birdnet/soundlevel",b'{}'),("birdnet",b'not json'),("birdnet",b'[]'),("birdnet",b'null'),("wildlife/detections",b'{"species":"Robin"}')])
def test_ignored_messages(topic,payload):
    app.handle_message(topic,payload)
    app.write_api.write.assert_not_called()


def test_read_only_never_writes_or_publishes(monkeypatch):
    monkeypatch.setattr(app,"READ_ONLY",True)
    assert not app.write_to_influx("audio_birdnet","bird","Robin","",.8,"model","mic")
    app.handle_message("birdnet",{"species":"Robin","confidence":.9})
    app.publish_ha_discovery()
    app.write_api.write.assert_not_called()
    app.mqtt_client.publish.assert_not_called()


def test_ha_discovery_legacy_ids_and_binary():
    app.publish_ha_discovery()
    assert app.mqtt_client.publish.call_count == 6
    configs={c.args[0]:json.loads(c.args[1]) for c in app.mqtt_client.publish.call_args_list}
    cam=configs["homeassistant/camera/garden_wildlife_latest_camera_snapshot/config"]
    assert cam["encoding"] == "" and cam["topic"] == "wildlife/latest_visual/image"
    assert "homeassistant/sensor/garden_wildlife_latest_camera_detection/config" in configs


@pytest.mark.parametrize("hour,minute,expected_day",[(22,29,22),(22,30,22),(0,0,22),(23,59,22)])
def test_observation_day_boundary(hour,minute,expected_day):
    now=dt.datetime(2026,10,22,hour,minute,tzinfo=app.TZ)
    out=app.get_observation_window("today",0,now)
    assert out[4].day == expected_day and out[4].hour == 0 and out[4].minute == 0
    assert out[1] == app.utc_string(out[5])


@pytest.mark.parametrize("date,expected",[(dt.datetime(2026,3,29,12,tzinfo=app.TZ),23),(dt.datetime(2026,10,25,12,tzinfo=app.TZ),25)])
def test_dst_day_hour_buckets(date,expected):
    out=app.get_observation_window("today",0,date)
    timeline=app.generate_multiseries_timeline("today",[],out[4],out[5],app.TZ)
    assert len(timeline) == expected


def test_all_time_includes_newest_and_single_point():
    now=dt.datetime.now(app.TZ)
    t=now.timestamp()
    for raw in ([(t,"Robin","birdnet")],[(t-3600,"Robin","birdnet"),(t,"Robin","birdnet")]):
        timeline=app.generate_multiseries_timeline("all",raw,now-dt.timedelta(days=1),now,app.TZ)
        assert sum(x["count"] for x in timeline) == len(raw)


def test_pair_count_equivalent_to_original():
    rng=random.Random(19)
    raw=sorted((rng.randrange(1000),rng.choice(["Robin","Blackbird","Wren"]),"birdnet") for _ in range(160))
    expected={}
    for i,(a,name,_) in enumerate(raw):
        for b,other,_ in raw[i+1:]:
            if b-a>90:break
            if other != name:
                pair=tuple(sorted((name,other))); expected[pair]=expected.get(pair,0)+1
    _,flocks,_=app.compute_dwell_and_flocks(raw)
    assert {(f['species_a'],f['species_b']):f['co_occurrences'] for f in flocks} == {k:v for k,v in expected.items() if v>=2}


def test_dwell_gap():
    dwell,_,_=app.compute_dwell_and_flocks([(0,"Robin","birdnet"),(180,"Robin","birdnet"),(361,"Robin","birdnet")])
    assert dwell["Robin"]["sessions_count"] == 2
    assert dwell["Robin"]["total_dwell_min"] == 3.5


def test_query_aggregates_all_rows_and_sources(monkeypatch):
    now=dt.datetime.now(app.UTC)
    records=[FakeRecord("Robin","Erithacus rubecula","audio_birdnet",now-dt.timedelta(minutes=2)),
             FakeRecord("Robin","Erithacus rubecula","visual_frigate",now-dt.timedelta(minutes=1)),
             FakeRecord("Nyctalus leisleri","","audio_birdnet",now)]
    monkeypatch.setattr(app,"query_records",Mock(return_value=records))
    species,_,_,total,timeline,_=app.get_active_species("all")
    assert total == 3 and len(species) == 2
    robin=next(x for x in species if x["common_name"]=="European Robin")
    bat=next(x for x in species if x["target_type"]=="bat")
    assert robin["count"]==2 and robin["birdnet"] and robin["frigate"]
    assert bat["bat_classifier"] and not bat["birdnet"]
    assert all(s["seen_today"] for s in species)
    assert sum(x["count"] for x in timeline)==3
    assert all(s["art_missing"] for s in species)
    app.get_active_species("all")
    assert app.query_records.call_count == 1


def png(name,size=(93,62)):
    folder=Path(app.ART_DIR); folder.mkdir(parents=True,exist_ok=True)
    path=folder/name
    Image.new("RGBA",size,(80,120,150,255)).save(path)
    return path


def test_masks_and_pose_aliases():
    png("robin.png")
    png("european_robin_flight.png",(120,40))
    art=app.resolve_art("European Robin","Erithacus rubecula")
    assert urllib.parse.urlsplit(art["image_url"]).path=="/art/robin.png"
    assert urllib.parse.urlsplit(art["flight_image_url"]).path=="/art/european_robin_flight.png"
    assert art["image_mask_key"]=="robin"
    assert art["flight_mask_key"]=="european_robin_flight"
    masks=app.artwork_index()[1]
    assert masks["robin"]["w"]==93 and masks["european_robin_flight"]["aspect"]==3
    assert not Path(app.MASKS_FILE).exists()  # normal web reads do not overwrite masks


def test_missing_art_explicit():
    art=app.resolve_art("European Robin","Erithacus rubecula")
    assert art["art_missing"] and art["image_mask_key"]=="__missing__"
    assert art["image_url"].startswith('/api/missing-art.svg?name=')


def test_bat_filename_aliases():
    png("lesser_noctule.png")
    art=app.resolve_art("Lesser Noctule / Leisler's Bat","Nyctalus leisleri")
    assert urllib.parse.urlsplit(art["image_url"]).path=="/art/lesser_noctule.png"


def test_force_rebuild_masks():
    png("robin.png")
    assert app.rebuild_masks()==1
    data=json.loads(Path(app.MASKS_FILE).read_text())
    assert app.valid_mask(data["robin"])
    assert "__missing__" not in data


@pytest.fixture
def http_server():
    server=app.AvianHTTPServer(("127.0.0.1",0),app.AvianWebHandler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    yield "http://127.0.0.1:"+str(server.server_port)
    server.shutdown();server.server_close();thread.join(timeout=2)


def get(url):
    try:
        r=urllib.request.urlopen(url,timeout=3)
    except urllib.error.HTTPError as e:
        r=e
    return r.status,r.read(),r.headers


def test_http_page_and_masks(http_server):
    status,body,_=get(http_server+"/")
    assert status==200 and b"Garden Friends" in body
    status,body,_=get(http_server+"/api/masks")
    assert status==200 and "__missing__" in json.loads(body)


@pytest.mark.parametrize("path",["/.env","/main.py","/art/../main.py","/art/%2E%2E%2Fmain.py","/art/%5C..%5Cmain.py","/art/latest_visual.json","/not-present"])
def test_http_no_source_or_traversal(http_server,path):
    assert get(http_server+path)[0] == 404


def test_http_bad_period(http_server):
    assert get(http_server+"/api/species?timeframe=invalid")[0]==400
    assert get(http_server+"/api/species?offset=-1")[0]==400


def test_http_database_error_not_empty_success(http_server,monkeypatch):
    monkeypatch.setattr(app,"influx_query_api",None)
    status,body,_=get(http_server+"/api/species")
    assert status==503 and "error" in json.loads(body)
    assert "species" not in json.loads(body)
    assert "INFLUX_TOKEN" in json.loads(body)["error"]


def test_http_snapshot_event_guard(http_server,monkeypatch):
    folder=app.SNAPSHOT_DIR;folder.mkdir(parents=True)
    (folder/"latest_visual.jpg").write_bytes(b"fixture")
    monkeypatch.setattr(app,"latest_snapshot_event","new")
    assert get(http_server+"/art/latest_visual.jpg?event=old")[0]==404
    assert get(http_server+"/art/latest_visual.jpg?event=new")[0]==200


def test_no_network_on_import_or_check(monkeypatch):
    with pytest.MonkeyPatch.context() as m:
        m.setattr(urllib.request,"urlopen",Mock(side_effect=AssertionError("unexpected network")))
        data=app.check_configuration()
        assert data["version"]==app.APP_VERSION
        assert "fixture-not-a-real-token" not in json.dumps(data)


def test_source_ui_features():
    for token in ('id="chk-bat"','id="chk-total"','id="chk-taxon-all"','id="chk-taxon-birds"',
                  'id="chk-taxon-mammals"','function packCollage','function drawTimelineChart',
                  'function refreshSpeciesListDisplay','function getDisplayName'):
        assert token in app.HTML_PAGE
    assert "document.body.className = data.solar.state" not in app.HTML_PAGE
