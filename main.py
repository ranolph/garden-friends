#!/usr/bin/env python3
"""Garden Friends: an illustrated, self-hosted wildlife observation dashboard.

Receives classified BirdNET/Frigate MQTT detections, stores them in InfluxDB 2.x,
and displays a silhouette-aware collage, activity charts and wildlife statistics.
Connections are configured through the authenticated Settings page.
"""
from __future__ import annotations

import argparse
import bisect
import collections
import copy
import datetime
import hashlib
import html
import importlib.metadata
import io
import json
import logging
import math
import mimetypes
import os
from pathlib import Path
import queue
import re
import signal
import socket
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zoneinfo
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import garden_friends_settings as gf_settings

APP_VERSION = "1.0.0"
APP_DIR = Path(__file__).resolve().parent
LOG = logging.getLogger("garden_friends")


def load_env_file(path: Path) -> None:
    """Small, literal .env reader. Environment variables take precedence.

    Values are never evaluated, expanded as shell commands or printed.
    Use one NAME=value per line; enclosing single/double quotes are optional.
    """
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        if text.startswith("export "):
            text = text[7:]
        name, sep, value = text.partition("=")
        name, value = name.strip(), value.strip()
        if not sep or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError("Invalid .env entry: expected NAME=value")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ.setdefault(name, value)


load_env_file(Path(os.getenv("WILDLIFE_ENV_FILE", str(APP_DIR / ".env"))))
# Managed web settings take precedence over legacy environment connection values.
gf_settings.apply_environment()


def env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


WEB_PORT = int(os.getenv("WEB_PORT", "8090"))
WEB_BIND = os.getenv("WEB_BIND", "0.0.0.0")
MQTT_BROKER = os.getenv("MQTT_BROKER", os.getenv("MQTT_HOST", "mqtt"))
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "garden-friends")
MQTT_TLS = env_bool("MQTT_TLS")
MQTT_CA_FILE = os.getenv("MQTT_CA_FILE", "")
HA_ENABLED = env_bool("HA_ENABLED", True)
HA_DISCOVERY_PREFIX = os.getenv("HA_DISCOVERY_PREFIX", "homeassistant").strip("/")
FRIGATE_TOPIC_PREFIX = os.getenv("FRIGATE_TOPIC_PREFIX", "frigate").strip("/")
BIRDNET_TOPICS = tuple(x.strip() for x in os.getenv(
    "BIRDNET_TOPICS", "birdnet/#,birdnet-go/#,birdnet,birdnet-go"
).split(",") if x.strip())
FRIGATE_MIN_SCORE = float(os.getenv("FRIGATE_MIN_SCORE", "0.75"))
# Camera score thresholds are configurable independently for birds and mammals.
FRIGATE_MAMMAL_MIN_SCORE = float(os.getenv(
    "FRIGATE_MAMMAL_MIN_SCORE", str(FRIGATE_MIN_SCORE)
))
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086").rstrip("/")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")
INFLUX_ORG = os.getenv("INFLUX_ORG", "wildlife")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "catalog")
INFLUX_TIMEOUT_MS = int(os.getenv("INFLUX_TIMEOUT_MS", "15000"))
TIMEZONE = os.getenv("TIMEZONE", "Europe/London")
TZ = zoneinfo.ZoneInfo(TIMEZONE)
UTC = datetime.timezone.utc
ART_DIR = str(Path(os.getenv("ART_DIR", str(APP_DIR / "species_art"))).resolve())
MASKS_FILE = str(Path(ART_DIR) / "masks.json")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
READ_ONLY = env_bool("READ_ONLY", True)
SUMMARY_INTERVAL = max(5.0, float(os.getenv("SUMMARY_INTERVAL", "30")))
QUERY_CACHE_SECONDS = max(0.0, float(os.getenv("QUERY_CACHE_SECONDS", "10")))
# Optional fallback for upstream messages that omit a confidence value.
# Set to an empty value to reject messages that omit a confidence value.
BIRDNET_MISSING_CONFIDENCE = os.getenv("BIRDNET_MISSING_CONFIDENCE", "0.85")
FRIGATE_AUTH_TOKEN = os.getenv("FRIGATE_AUTH_TOKEN", "")
FRIGATE_URLS = [u.strip().rstrip("/") for u in os.getenv("FRIGATE_URLS", "").split(",") if u.strip()]
SNAPSHOT_DIR = Path(os.getenv("GARDEN_FRIENDS_DATA_DIR", str(APP_DIR / "data"))) / "snapshots"

# No sockets, client instances or application files are created on import.
influx_client = None
influx_query_api = None
write_api = None
mqtt_client = None
Point = None
WritePrecision = None
mqtt = None
stop_event = threading.Event()
summary_needed = threading.Event()
state_lock = threading.RLock()
query_lock = threading.RLock()
art_lock = threading.RLock()
logged_events: set[str] = set()
logged_event_times: dict[str, float] = {}
inflight_events: set[str] = set()
pending_frigate_scores: dict[str, dict] = {}
fetching_frigate: set[str] = set()
message_queue: queue.Queue = queue.Queue(maxsize=4096)
query_cache: dict[tuple, tuple[float, object]] = {}
query_generation = 0
art_cache: dict = {"stamp": None, "masks": {}, "files": {}}
latest_visual_event = ""
latest_visual_payload: dict = {}
latest_snapshot_event = ""
last_query_error = ""


def get_host_ip() -> str:
    """Legacy fallback only: set PUBLIC_BASE_URL to avoid Docker-internal URLs."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        sock.close()


def public_url(path: str) -> str:
    base = PUBLIC_BASE_URL or f"http://{os.getenv('HOST_IP') or get_host_ip()}:{WEB_PORT}"
    return base + path


# --- Canonical species names and common aliases. ---
CANONICAL_SPECIES = {
    # UK Nocturnal & Diurnal Mammals
    "erinaceus europaeus": ("European Hedgehog", "Erinaceus europaeus"),
    "european hedgehog": ("European Hedgehog", "Erinaceus europaeus"),
    "hedgehog": ("European Hedgehog", "Erinaceus europaeus"),
    "vulpes vulpes": ("Red Fox", "Vulpes vulpes"),
    "red fox": ("Red Fox", "Vulpes vulpes"),
    "fox": ("Red Fox", "Vulpes vulpes"),
    "meles meles": ("European Badger", "Meles meles"),
    "european badger": ("European Badger", "Meles meles"),
    "badger": ("European Badger", "Meles meles"),
    "sciurus carolinensis": ("Grey Squirrel", "Sciurus carolinensis"),
    "grey squirrel": ("Grey Squirrel", "Sciurus carolinensis"),
    "apodemus sylvaticus": ("Wood Mouse", "Apodemus sylvaticus"),
    "wood mouse": ("Wood Mouse", "Apodemus sylvaticus"),
    "rattus norvegicus": ("Brown Rat", "Rattus norvegicus"),
    "brown rat": ("Brown Rat", "Rattus norvegicus"),
    "muntiacus reevesi": ("Muntjac Deer", "Muntiacus reevesi"),
    "muntjac deer": ("Muntjac Deer", "Muntiacus reevesi"),
    "muntjac": ("Muntjac Deer", "Muntiacus reevesi"),
    "capreolus capreolus": ("Roe Deer", "Capreolus capreolus"),
    "roe deer": ("Roe Deer", "Capreolus capreolus"),

    # Treecreepers
    "certhia brachydactyla": ("Short-toed Treecreeper", "Certhia brachydactyla"),
    "short-toed treecreeper": ("Short-toed Treecreeper", "Certhia brachydactyla"),
    "short toed treecreeper": ("Short-toed Treecreeper", "Certhia brachydactyla"),
    "certhia familiaris": ("Eurasian Treecreeper", "Certhia familiaris"),
    "eurasian treecreeper": ("Eurasian Treecreeper", "Certhia familiaris"),
    "treecreeper": ("Eurasian Treecreeper", "Certhia familiaris"),

    # Robins
    "erithacus rubecula": ("European Robin", "Erithacus rubecula"),
    "european robin": ("European Robin", "Erithacus rubecula"),
    "robin": ("European Robin", "Erithacus rubecula"),

    # Doves & Pigeons
    "columba livia": ("Feral Pigeon", "Columba livia"),
    "columba livia domestica": ("Feral Pigeon", "Columba livia domestica"),
    "feral pigeon": ("Feral Pigeon", "Columba livia domestica"),
    "rock dove": ("Feral Pigeon", "Columba livia"),
    "rock pigeon": ("Feral Pigeon", "Columba livia"),
    "columba palumbus": ("Wood Pigeon", "Columba palumbus"),
    "wood pigeon": ("Wood Pigeon", "Columba palumbus"),
    "columba oenas": ("Stock Dove", "Columba oenas"),
    "stock dove": ("Stock Dove", "Columba oenas"),
    "streptopelia decaocto": ("Collared Dove", "Streptopelia decaocto"),
    "collared dove": ("Collared Dove", "Streptopelia decaocto"),

    # Common Garden Birds
    "delichon urbicum": ("House Martin", "Delichon urbicum"),
    "house martin": ("House Martin", "Delichon urbicum"),
    "hirundo rustica": ("Barn Swallow", "Hirundo rustica"),
    "barn swallow": ("Barn Swallow", "Hirundo rustica"),
    "apus apus": ("Common Swift", "Apus apus"),
    "common swift": ("Common Swift", "Apus apus"),
    "swift": ("Common Swift", "Apus apus"),
    "phylloscopus collybita": ("Common Chiffchaff", "Phylloscopus collybita"),
    "chiffchaff": ("Common Chiffchaff", "Phylloscopus collybita"),
    "regulus regulus": ("Goldcrest", "Regulus regulus"),
    "sitta europaea": ("Eurasian Nuthatch", "Sitta europaea"),
    "nuthatch": ("Eurasian Nuthatch", "Sitta europaea"),
    "spinus spinus": ("Eurasian Siskin", "Spinus spinus"),
    "cyanistes caeruleus": ("Blue Tit", "Cyanistes caeruleus"),
    "blue tit": ("Blue Tit", "Cyanistes caeruleus"),
    "parus major": ("Great Tit", "Parus major"),
    "great tit": ("Great Tit", "Parus major"),
    "periparus ater": ("Coal Tit", "Periparus ater"),
    "coal tit": ("Coal Tit", "Periparus ater"),
    "aegithalos caudatus": ("Long-tailed Tit", "Aegithalos caudatus"),
    "long-tailed tit": ("Long-tailed Tit", "Aegithalos caudatus"),
    "turdus merula": ("Blackbird", "Turdus merula"),
    "blackbird": ("Blackbird", "Turdus merula"),
    "turdus philomelos": ("Song Thrush", "Turdus philomelos"),
    "song thrush": ("Song Thrush", "Turdus philomelos"),
    "turdus viscivorus": ("Mistle Thrush", "Turdus viscivorus"),
    "mistle thrush": ("Mistle Thrush", "Turdus viscivorus"),
    "pica pica": ("Magpie", "Pica pica"),
    "magpie": ("Magpie", "Pica pica"),
    "corvus monedula": ("Jackdaw", "Corvus monedula"),
    "jackdaw": ("Jackdaw", "Corvus monedula"),
    "corvus corone": ("Carrion Crow", "Corvus corone"),
    "carrion crow": ("Carrion Crow", "Corvus corone"),
    "corvus cornix": ("Carrion Crow", "Corvus corone"),
    "hooded crow": ("Carrion Crow", "Corvus corone"),
    "corvus frugilegus": ("Rook", "Corvus frugilegus"),
    "rook": ("Rook", "Corvus frugilegus"),
    "corvus corax": ("Raven", "Corvus corax"),
    "raven": ("Raven", "Corvus corax"),
    "garrulus glandarius": ("Jay", "Garrulus glandarius"),
    "jay": ("Jay", "Garrulus glandarius"),
    "prunella modularis": ("Dunnock", "Prunella modularis"),
    "dunnock": ("Dunnock", "Prunella modularis"),
    "troglodytes troglodytes": ("Wren", "Troglodytes troglodytes"),
    "wren": ("Wren", "Troglodytes troglodytes"),
    "passer domesticus": ("House Sparrow", "Passer domesticus"),
    "house sparrow": ("House Sparrow", "Passer domesticus"),
    "chloris chloris": ("Greenfinch", "Chloris chloris"),
    "greenfinch": ("Greenfinch", "Chloris chloris"),
    "fringilla coelebs": ("Chaffinch", "Fringilla coelebs"),
    "chaffinch": ("Chaffinch", "Fringilla coelebs"),
    "carduelis carduelis": ("Goldfinch", "Carduelis carduelis"),
    "goldfinch": ("Goldfinch", "Carduelis carduelis"),
    "pyrrhula pyrrhula": ("Bullfinch", "Pyrrhula pyrrhula"),
    "bullfinch": ("Bullfinch", "Pyrrhula pyrrhula"),
    "sturnus vulgaris": ("Starling", "Sturnus vulgaris"),
    "starling": ("Starling", "Sturnus vulgaris"),
    "motacilla alba yarrellii": ("Pied Wagtail", "Motacilla alba yarrellii"),
    "motacilla alba": ("Pied Wagtail", "Motacilla alba"),
    "pied wagtail": ("Pied Wagtail", "Motacilla alba yarrellii"),
    "motacilla cinerea": ("Grey Wagtail", "Motacilla cinerea"),
    "grey wagtail": ("Grey Wagtail", "Motacilla cinerea"),
    "sylvia atricapilla": ("Blackcap", "Sylvia atricapilla"),
    "blackcap": ("Blackcap", "Sylvia atricapilla"),
    "picus viridis": ("Green Woodpecker", "Picus viridis"),
    "green woodpecker": ("Green Woodpecker", "Picus viridis"),
    "dendrocopos major": ("Great Spotted Woodpecker", "Dendrocopos major"),
    "accipiter nisus": ("Sparrowhawk", "Accipiter nisus"),
    "milvus milvus": ("Red Kite", "Milvus milvus"),
    "buteo buteo": ("Common Buzzard", "Buteo buteo"),
    "falco tinnunculus": ("Kestrel", "Falco tinnunculus"),
    "kestrel": ("Kestrel", "Falco tinnunculus"),
    "falco peregrinus": ("Peregrine Falcon", "Falco peregrinus"),
    "strix aluco": ("Tawny Owl", "Strix aluco"),
    "tawny owl": ("Tawny Owl", "Strix aluco"),

    # Gulls
    "larus argentatus": ("Herring Gull", "Larus argentatus"),
    "herring gull": ("Herring Gull", "Larus argentatus"),
    "seagull": ("Herring Gull", "Larus argentatus"),
    "larus fuscus": ("Lesser Black-backed Gull", "Larus fuscus"),
    "lesser black-backed gull": ("Lesser Black-backed Gull", "Larus fuscus"),
    "chroicocephalus ridibundus": ("Black-headed Gull", "Chroicocephalus ridibundus"),
    "black-headed gull": ("Black-headed Gull", "Chroicocephalus ridibundus"),

    # UK Bats
    "pipistrellus pipistrellus": ("Common Pipistrelle", "Pipistrellus pipistrellus"),
    "pipistrellus pygmaeus": ("Soprano Pipistrelle", "Pipistrellus pygmaeus"),
    "pipistrellus pygmaeus/pipistrellus": ("Common Pipistrelle", "Pipistrellus pipistrellus"),
    "pipistrellus nathusii": ("Nathusius' Pipistrelle", "Pipistrellus nathusii"),
    "nyctalus noctula": ("Noctule", "Nyctalus noctula"),
    "plecotus auritus": ("Brown Long-eared Bat", "Plecotus auritus"),
    "myotis daubentonii": ("Daubenton's Bat", "Myotis daubentonii"),
    "myotis nattereri": ("Natterer's Bat", "Myotis nattereri"),
    "eptesicus serotinus": ("Serotine", "Eptesicus serotinus")
}

# Regional override explicitly requested in PDF pp.278–280, 421–426.
SPECIES_ALIASES = {
    "hooded crow": ("Carrion Crow", "Corvus corone"),
    "corvus cornix": ("Carrion Crow", "Corvus corone"),
    "corvus corone cornix": ("Carrion Crow", "Corvus corone"),
}

# Later bat-name patch, pp.1853–1862 and 1874–1878. Names deliberately follow
# the transcript (including the displayed Lesser Noctule / Leisler's Bat alias).
BAT_SYNONYMS = {
    "nyctalus leisleri": ("Lesser Noctule / Leisler's Bat", "Nyctalus leisleri"),
    "nyctalus noctula": ("Noctule Bat", "Nyctalus noctula"),
    "pipistrellus pipistrellus": ("Common Pipistrelle", "Pipistrellus pipistrellus"),
    "pipistrellus pygmaeus": ("Soprano Pipistrelle", "Pipistrellus pygmaeus"),
    "pipistrellus nathusii": ("Nathusius's Pipistrelle", "Pipistrellus nathusii"),
    "myotis nattereri": ("Natterer's Bat", "Myotis nattereri"),
    "myotis daubentonii": ("Daubenton's Bat", "Myotis daubentonii"),
    "myotis mystacinus": ("Whiskered Bat", "Myotis mystacinus"),
    "myotis brandtii": ("Brandt's Bat", "Myotis brandtii"),
    "myotis alcathoe": ("Alcathoe Bat", "Myotis alcathoe"),
    "myotis bechsteinii": ("Bechstein's Bat", "Myotis bechsteinii"),
    "myotis myotis": ("Greater Mouse-eared Bat", "Myotis myotis"),
    "plecotus auritus": ("Brown Long-eared Bat", "Plecotus auritus"),
    "plecotus austriacus": ("Grey Long-eared Bat", "Plecotus austriacus"),
    "eptesicus serotinus": ("Serotine Bat", "Eptesicus serotinus"),
    "rhinolophus ferrumequinum": ("Greater Horseshoe Bat", "Rhinolophus ferrumequinum"),
    "rhinolophus hipposideros": ("Lesser Horseshoe Bat", "Rhinolophus hipposideros"),
    "barbastella barbastellus": ("Barbastelle Bat", "Barbastella barbastellus"),
    "miniopterus schreibersii": ("Schreibers's Bent-winged Bat", "Miniopterus schreibersii"),
    "hypsugo savii": ("Savi's Pipistrelle", "Hypsugo savii"),
}
BAT_GENERA = tuple(sorted({name.split()[0] for name in BAT_SYNONYMS}))
# These names/aliases also appear in the later image-integration scripts.
EXTRA_SPECIES = {
    "turdus iliacus": ("Redwing", "Turdus iliacus"),
    "gallinula chloropus": ("Moorhen", "Gallinula chloropus"),
    "alcedo atthis": ("Kingfisher", "Alcedo atthis"),
    "phoenicurus ochruros": ("Black Redstart", "Phoenicurus ochruros"),
    "emberiza citrinella": ("Yellowhammer", "Emberiza citrinella"),
    "charadrius dubius": ("Little Ringed Plover", "Charadrius dubius"),
    "turdus pilaris": ("Fieldfare", "Turdus pilaris"),
    "loxia curvirostra": ("Common Crossbill", "Loxia curvirostra"),
    "calidris alpina": ("Dunlin", "Calidris alpina"),
}
CANONICAL_SPECIES.update(EXTRA_SPECIES)
CANONICAL_SPECIES.update(BAT_SYNONYMS)
for _common, _scientific in list(CANONICAL_SPECIES.values()):
    CANONICAL_SPECIES.setdefault(_common.casefold(), (_common, _scientific))
CANONICAL_SPECIES.update({
    "leisler's bat": BAT_SYNONYMS["nyctalus leisleri"],
    "leislers bat": BAT_SYNONYMS["nyctalus leisleri"],
    "lesser noctule": BAT_SYNONYMS["nyctalus leisleri"],
    "crossbill": EXTRA_SPECIES["loxia curvirostra"],
    "common kingfisher": EXTRA_SPECIES["alcedo atthis"],
    "common moorhen": EXTRA_SPECIES["gallinula chloropus"],
})

NEGATIVE_PHRASES = (
    "no bird", "not contain", "does not contain", "no animal", "no wildlife",
    "not a bird", "cannot identify", "cannot see", "unable to", "does not appear",
    "nothing visible", "empty", "background only", "false positive", "there is no",
)
GENERIC_NAMES = {"bird", "bat", "animal", "unknown", "none", "null", "wildlife", ""}
PET_PATTERN = re.compile(r"\b(?:domestic\s+)?(?:cat|dog)\b|\b(?:human|person)\b", re.I)
SPECIES_PATTERN = re.compile(r"([A-Za-zÀ-ž\s'’\-/]+?)\s*\(([A-Za-z][A-Za-z\s'’\-/]+)\)")
MAMMAL_WORDS = (
    "hedgehog", "fox", "badger", "squirrel", "mouse", "rat", "deer", "muntjac",
    "stoat", "weasel", "rabbit", "hare", "otter", "mole", "shrew", "vole",
)


def clean_name(value) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"\s+", " ", value.replace("’", "'").strip(" \t\r\n*`\""))


def is_negative_response(text) -> bool:
    value = clean_name(text).casefold()
    if not value:
        return True
    # Word boundaries repair the old accidental rejection of names containing
    # the letters 'cat' or 'dog' inside a valid word or scientific name.
    return any(p in value for p in NEGATIVE_PHRASES) or bool(PET_PATTERN.search(value))


def apply_species_alias(common_name, scientific_name="") -> tuple[str, str]:
    common, scientific = clean_name(common_name), clean_name(scientific_name)
    for key in (common.casefold(), scientific.casefold()):
        if key in SPECIES_ALIASES:
            return SPECIES_ALIASES[key]
    return common, scientific


def get_target_type(name, scientific="") -> str:
    common, sci = clean_name(name).casefold(), clean_name(scientific).casefold()
    genera = {x.split(" ", 1)[0] for x in (common, sci) if x}
    if genera.intersection(BAT_GENERA) or re.search(
        r"\bbat\b|pipistrelle|noctule|serotine|barbastelle", common
    ):
        return "bat"
    if any(re.search(r"\b" + re.escape(word) + r"\b", common) for word in MAMMAL_WORDS):
        return "mammal"
    return "bird"


def normalize_species(common, scientific="") -> tuple[str | None, str | None]:
    common, scientific = apply_species_alias(common, scientific)
    if not common and scientific:
        common = scientific
    if not common or is_negative_response(common):
        return None, None
    # An absent scientific name is valid; the old check treated it as a negative.
    if scientific and is_negative_response(scientific):
        return None, None
    if common.casefold() in GENERIC_NAMES:
        return None, None
    for key in (scientific.casefold(), common.casefold()):
        if key in SPECIES_ALIASES:
            return SPECIES_ALIASES[key]
        if key in CANONICAL_SPECIES:
            resolved = CANONICAL_SPECIES[key]
            return BAT_SYNONYMS.get(resolved[1].casefold(), resolved)
    norm = re.sub(r"^(?:Common|Eurasian|European|Western|Northern)\s+", "", common, flags=re.I)
    for key, (c_name, s_name) in CANONICAL_SPECIES.items():
        short = re.sub(r"^(?:Common|Eurasian|European|Western|Northern)\s+", "", c_name, flags=re.I)
        if norm.casefold() in (key, c_name.casefold(), short.casefold()):
            return BAT_SYNONYMS.get(s_name.casefold(), (c_name, s_name))
    if len(norm) > 64 or re.search(r"[.!?<>\n]", norm):
        return None, None
    return norm.title(), scientific


def parse_species_description(description) -> tuple[str | None, str | None]:
    desc = clean_name(description)
    if not desc or is_negative_response(desc):
        return None, None
    match = SPECIES_PATTERN.search(desc)
    if match:
        return normalize_species(match.group(1), match.group(2))
    return normalize_species(desc)


def valid_confidence(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and 0.0 <= number <= 1.0 else None


def first_present(mapping: dict, *keys):
    for key in keys:
        value = mapping.get(key)
        if value is not None and value != "":
            return value
    return None


# --- Artwork / alpha masks: original filenames, plus documented alias variants. ---
ART_ALIASES = {
    "nyctalus leisleri": ("lesser_noctule", "lesser_noctule_bat", "leislers_bat", "leisler_bat", "nyctalus_leisleri"),
    "nyctalus noctula": ("noctule", "noctule_bat", "nyctalus_noctula"),
    "pipistrellus nathusii": ("nathusius_pipistrelle", "nathusiuss_pipistrelle"),
    "eptesicus serotinus": ("serotine", "serotine_bat"),
    "barbastella barbastellus": ("barbastelle", "barbastelle_bat"),
    "erithacus rubecula": ("european_robin", "robin"),
    "certhia brachydactyla": ("short-toed_treecreeper", "short_toed_treecreeper"),
    "gallinula chloropus": ("moorhen", "common_moorhen", "gallinula_chloropus"),
    "alcedo atthis": ("kingfisher", "common_kingfisher", "alcedo_atthis"),
    "loxia curvirostra": ("crossbill", "common_crossbill", "loxia_curvirostra"),
}

# Per-species artwork scale nudges. These are intentionally minimal and only
# compensate for specific imported illustrations whose apparent size is
# visually out of proportion inside the collage, without changing detection
# counts or ranking.
ART_SCALE_OVERRIDES = {
    "pipistrellus pipistrellus": 0.68,  # Common Pipistrelle was presenting far too large
}


def slug_variants(text: str) -> list[str]:
    stem = clean_name(text).lower().replace("'", "").replace(" ", "_")
    stem = re.sub(r"[^a-z0-9_-]", "", stem)
    return list(dict.fromkeys((stem, stem.replace("-", "_"), stem.replace("_", "-"))))


def mask_from_png(path: Path) -> dict:
    from PIL import Image
    with Image.open(path) as original:
        image = original.convert("RGBA")
        width, height = image.size
        mask_w = 93
        mask_h = max(1, min(1024, int(height * mask_w / width)))
        alpha = image.resize((mask_w, mask_h), Image.Resampling.LANCZOS).getchannel("A")
        pixels = list(alpha.getdata())
        bitmap = ["".join("1" if a > 25 else "0" for a in pixels[y*mask_w:(y+1)*mask_w])
                  for y in range(mask_h)]
        return {"w": mask_w, "h": mask_h, "aspect": round(width / height, 6), "bitmap": bitmap}


def valid_mask(mask) -> bool:
    if not isinstance(mask, dict):
        return False
    w, h, bits = mask.get("w"), mask.get("h"), mask.get("bitmap")
    return (isinstance(w, int) and isinstance(h, int) and 0 < w <= 1024 and 0 < h <= 1024
            and isinstance(bits, list) and len(bits) == h
            and all(isinstance(row, str) and len(row) == w and set(row) <= {"0", "1"} for row in bits)
            and isinstance(mask.get("aspect"), (int, float))
            and math.isfinite(mask["aspect"]) and mask["aspect"] > 0)


def artwork_index(force: bool = False) -> tuple[dict, dict]:
    """Read surviving assets. Rebuild missing/stale masks in memory, not on disk."""
    folder = Path(ART_DIR)
    files = {}
    if folder.is_dir():
        for path in sorted(folder.glob("*.png")):
            if path.name == "favicon.png" or path.name.startswith("mask_") or path.is_symlink():
                continue
            try:
                stat = path.stat()
                files[path.stem] = (path, stat.st_mtime_ns, stat.st_size)
            except OSError:
                continue
    maskpath = Path(MASKS_FILE)
    try:
        maskstamp = maskpath.stat().st_mtime_ns
    except OSError:
        maskstamp = 0
    stamp = (str(folder), maskstamp, tuple((k, v[1], v[2]) for k, v in files.items()))
    with art_lock:
        if not force and stamp == art_cache.get("stamp"):
            return art_cache["files"], art_cache["masks"]
        masks = {}
        if maskstamp:
            try:
                data = json.loads(maskpath.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    masks = {k: v for k, v in data.items() if k in files and valid_mask(v)}
            except (OSError, ValueError):
                LOG.warning("masks.json is unreadable; rebuilding available PNG masks in memory")
        for stem, (path, modified, _) in files.items():
            if force or stem not in masks or modified > maskstamp:
                try:
                    masks[stem] = mask_from_png(path)
                except Exception as exc:
                    LOG.warning("Cannot build artwork mask for %s (%s)", path.name, type(exc).__name__)
        # A rectangular mask for explicitly labelled missing-artwork cards.
        masks["__missing__"] = {"w": 93, "h": 58, "aspect": 1.6, "bitmap": ["1"*93]*58}
        art_cache.update(stamp=stamp, files=files, masks=masks)
        return files, masks


_webp_freshness_cache = {}


def webp_is_current(png: Path, webp: Path) -> bool:
    """Keep valid WebP choices after Git checkout resets filesystem timestamps.

The release records original timestamps and hashes. Verify the pair once per
file change before using that provenance; manually replaced assets safely fall
back to PNG until a fresh release manifest is generated.
"""
    if not webp.is_file() or webp.is_symlink():
        return False
    original, derivative = png.stat(), webp.stat()
    fallback = derivative.st_mtime_ns >= original.st_mtime_ns
    manifest = Path(ART_DIR).parent / "ARTWORK_MANIFEST.json"
    if not manifest.is_file() or manifest.is_symlink():
        return fallback
    stamp = manifest.stat()
    key = (str(png), original.st_size, original.st_mtime_ns,
           str(webp), derivative.st_size, derivative.st_mtime_ns,
           stamp.st_size, stamp.st_mtime_ns)
    with art_lock:
        if key in _webp_freshness_cache:
            return _webp_freshness_cache[key]
    try:
        if stamp.st_size > 8 * 1024 * 1024:
            return False
        records = json.loads(manifest.read_text(encoding="utf-8"))["files"]
        a = records[png.relative_to(Path(ART_DIR)).as_posix()]
        b = records[webp.relative_to(Path(ART_DIR)).as_posix()]
        if not all(type(record.get("mtime_ns")) is int for record in (a, b)):
            return fallback
        fresh = b["mtime_ns"] >= a["mtime_ns"]
        for path, record in ((png, a), (webp, b)):
            if path.stat().st_size != record["bytes"]:
                fresh = False
                break
            checksum = hashlib.sha256()
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    checksum.update(chunk)
            if checksum.hexdigest() != record["sha256"]:
                fresh = False
                break
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        fresh = False
    with art_lock:
        if len(_webp_freshness_cache) > 4096:
            _webp_freshness_cache.clear()
        _webp_freshness_cache[key] = fresh
    return fresh


def resolve_art(common: str, scientific: str, index: tuple[dict, dict] | None = None) -> dict:
    files, masks = index or artwork_index()
    candidates = list(ART_ALIASES.get(scientific.casefold(), ()))
    for name in (common, scientific):
        candidates.extend(slug_variants(name))
        shortened = re.sub(r"^(?:Common|Eurasian|European|Western|Northern)\s+", "", name, flags=re.I)
        candidates.extend(slug_variants(shortened))
    candidates = list(dict.fromkeys(x for x in candidates if x))
    perched = next((s for s in candidates if s in files), None)
    flight = next((s + "_flight" for s in candidates if s + "_flight" in files), None)
    # A surviving flight pose is better than a broken perched URL, without
    # overwriting files or pretending that an illustration has been recovered.
    chosen = perched or flight
    # Prefer maintained WebP copies while retaining PNG silhouettes and aliases.
    def image_link(path):
        webp = path.with_suffix(".webp")
        if webp_is_current(path, webp):
            return "/art/" + urllib.parse.quote(webp.name) + "?v=" + str(webp.stat().st_mtime_ns)
        return "/art/" + urllib.parse.quote(path.name) + "?v=" + str(path.stat().st_mtime_ns)
    image_url = image_link(files[chosen][0]) if chosen else (
        "/api/missing-art.svg?name=" + urllib.parse.quote(common)
    )
    return {
        "slug": perched or (flight[:-7] if flight else (candidates[0] if candidates else "unknown")),
        "has_flight": bool(flight),
        "image_url": image_url,
        "flight_image_url": image_link(files[flight][0]) if flight else image_url,
        "image_mask_key": chosen if chosen in masks else "__missing__",
        "flight_mask_key": flight if flight in masks else (chosen if chosen in masks else "__missing__"),
        "art_missing": not bool(chosen),
        "art_scale": float(ART_SCALE_OVERRIDES.get(scientific.casefold(), 1.0)),
    }


def atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix="." + path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as file:
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def rebuild_masks() -> int:
    files, masks = artwork_index(force=True)
    content = {k: v for k, v in masks.items() if k in files}
    atomic_write(Path(MASKS_FILE), json.dumps(content, ensure_ascii=False, indent=2).encode("utf-8"))
    return len(content)


# --- Home Assistant discovery and retained state: original topics/unique IDs. ---
def mqtt_publish(topic: str, payload, retain: bool = False) -> None:
    if READ_ONLY or not HA_ENABLED or mqtt_client is None:
        return
    try:
        mqtt_client.publish(topic, payload, qos=1, retain=retain)
    except Exception as exc:
        LOG.warning("MQTT publish failed for %s (%s)", topic, type(exc).__name__)


def publish_ha_discovery() -> None:
    if not HA_ENABLED:
        return
    device = {
        "identifiers": ["garden_wildlife_monitor"], "name": "Garden Wildlife Monitor",
        "manufacturer": "Bioacoustic & Frigate Bridge", "model": "UK Kachō-e Monitor",
    }
    definitions = (
        ("garden_wildlife_last_visitor", "Garden Wildlife Last Visitor", "wildlife/recent_detection", "common_name", "mdi:paw", None),
        ("garden_wildlife_latest_camera_detection", "Garden Wildlife Latest Camera Detection", "wildlife/latest_visual", "common_name", "mdi:camera", None),
        ("wildlife_24h_summary", "Wildlife 24h Summary", "wildlife/summary_24h", "species_count", "mdi:book-open-page-variant", "species"),
        ("garden_wildlife_today_species_count", "Garden Wildlife Today Species Count", "wildlife/summary_today", "species_count", "mdi:counter", "species"),
        ("garden_wildlife_today_total_detections", "Garden Wildlife Today Total Detections", "wildlife/summary_today", "total_detections", "mdi:chart-bell-curve-cumulative", "visits"),
    )
    for unique_id, name, topic, field, icon, unit in definitions:
        config = {
            "name": name, "unique_id": unique_id, "state_topic": topic,
            "value_template": "{{ value_json." + field + " }}",
            "json_attributes_topic": topic, "icon": icon, "device": device,
        }
        if unit:
            config["unit_of_measurement"] = unit
        mqtt_publish(f"{HA_DISCOVERY_PREFIX}/sensor/{unique_id}/config", json.dumps(config), True)
    camera = {
        "name": "Garden Wildlife Latest Camera Snapshot",
        "unique_id": "garden_wildlife_latest_camera_snapshot",
        "topic": "wildlife/latest_visual/image",
        "json_attributes_topic": "wildlife/latest_visual", "device": device,
        # JPEG bytes are sent directly, not Base64. Explicitly disable decoding.
        "encoding": "",
    }
    mqtt_publish(f"{HA_DISCOVERY_PREFIX}/camera/garden_wildlife_latest_camera_snapshot/config", json.dumps(camera), True)


def frigate_request(base_url: str, path: str) -> urllib.request.Request:
    headers = {"User-Agent": "GardenFriends/" + APP_VERSION}
    if FRIGATE_AUTH_TOKEN:
        headers["Authorization"] = "Bearer " + FRIGATE_AUTH_TOKEN
    return urllib.request.Request(base_url + path, headers=headers)


def fetch_and_publish_snapshot_binary(event_id: str) -> None:
    """Original snapshot/thumbnail fallback, with atomic and ordered writes."""
    if READ_ONLY:
        return

    def worker():
        global latest_snapshot_event
        from PIL import Image
        encoded = urllib.parse.quote(str(event_id), safe="")
        for delay in (0.5, 2.0, 4.0, 6.0):
            if stop_event.wait(delay):
                return
            with state_lock:
                if latest_visual_event != event_id:
                    return
            for base in FRIGATE_URLS:
                for ending in ("snapshot.jpg", "thumbnail.jpg"):
                    try:
                        with urllib.request.urlopen(frigate_request(base, f"/api/events/{encoded}/{ending}"), timeout=4) as response:
                            data = response.read(20 * 1024 * 1024 + 1)
                        if not 100 < len(data) <= 20 * 1024 * 1024:
                            continue
                        with Image.open(io.BytesIO(data)) as image:
                            image.verify()
                        with state_lock:
                            if latest_visual_event != event_id:
                                return
                            atomic_write(SNAPSHOT_DIR / "latest_visual.jpg", data)
                            latest_snapshot_event = event_id
                            updated = dict(latest_visual_payload, snapshot_available=True)
                            latest_visual_payload.update(updated)
                            atomic_write(SNAPSHOT_DIR / "latest_visual.json",
                                         json.dumps(updated, ensure_ascii=False).encode("utf-8"))
                            mqtt_publish("wildlife/latest_visual/image", data, True)
                            mqtt_publish("wildlife/latest_visual", json.dumps(updated), True)
                        LOG.info("[HA SNAPSHOT SUCCESS] %s (%d bytes, %s)", event_id, len(data), ending)
                        return
                    except Exception:
                        continue
        LOG.warning("[HA SNAPSHOT] No readable snapshot for event %s; check FRIGATE_URLS/network/auth", event_id)

    threading.Thread(target=worker, name="wildlife-snapshot", daemon=True).start()


def publish_ha_detection(source, target_type, norm_c, norm_s, confidence, model, device,
                         timestamp_str=None, event_id=None) -> dict:
    global latest_visual_event
    art = resolve_art(norm_c, norm_s)
    payload = {
        "common_name": norm_c, "scientific_name": norm_s, "type": target_type,
        "confidence": round(float(confidence), 4), "source": source, "model": model,
        "device": device, "camera": device,
        "image_url": public_url(art["image_url"]),
        "timestamp": timestamp_str or datetime.datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
    }
    if source == "visual_frigate" and event_id:
        snap = public_url("/art/latest_visual.jpg?event=" + urllib.parse.quote(str(event_id), safe=""))
        visual = dict(payload, event_id=str(event_id), snapshot_url=snap, image_url=snap,
                      snapshot_available=False)
        payload.update(event_id=str(event_id), snapshot_url=snap)
        with state_lock:
            latest_visual_event = str(event_id)
            latest_visual_payload.clear()
            latest_visual_payload.update(visual)
        mqtt_publish("wildlife/latest_visual", json.dumps(visual), True)
        fetch_and_publish_snapshot_binary(str(event_id))
    mqtt_publish("wildlife/recent_detection", json.dumps(payload), True)
    mqtt_publish("wildlife/detections", json.dumps(payload), False)
    return payload


def publish_ha_summary() -> None:
    if READ_ONLY:
        return
    try:
        species, date_str, _, total, _, _ = get_active_species("today", 0)
        payload = {
            "species_count": len(species), "total_detections": total,
            "total_count": total, "window_count": total, "date": date_str,
            "species": species, "top_species": [s["common_name"] for s in species[:5]],
            "species_details": {s["common_name"]: s["count"] for s in species},
        }
        encoded = json.dumps(payload, ensure_ascii=False)
        mqtt_publish("wildlife/summary_today", encoded, True)
        # Compatibility topic: this is the local calendar day, not a rolling 24-hour window.
        mqtt_publish("wildlife/summary_24h", encoded, True)
    except Exception as exc:
        LOG.warning("[HA SUMMARY] %s", safe_error(exc))


def sync_last_detection_from_influx() -> None:
    """Restore recent and camera metadata, without writing history back to Influx."""
    for visual in (False, True):
        filt = '\n  |> filter(fn: (r) => r.source == "visual_frigate")' if visual else ""
        query = (
            f'from(bucket: {json.dumps(INFLUX_BUCKET)})\n'
            '  |> range(start: -7d)\n'
            '  |> filter(fn: (r) => r._measurement == "wildlife_detection" and r._field == "confidence")'
            + filt + '\n  |> group()\n  |> sort(columns: ["_time"], desc: true)\n  |> limit(n: 20)'
        )
        try:
            records = query_records(query)
            for record in records:
                common, scientific = normalize_species(record.values.get("common_name"), record.values.get("scientific_name", ""))
                if not common:
                    continue
                when = ensure_aware(record.get_time())
                art = resolve_art(common, scientific)
                payload = {
                    "common_name": common, "scientific_name": scientific,
                    "type": get_target_type(common, scientific),
                    "confidence": valid_confidence(record.get_value()),
                    "source": record.values.get("source", "audio_birdnet"),
                    "model": record.values.get("model", "BirdNET"),
                    "device": record.values.get("device", "clippy_mic"),
                    "camera": record.values.get("device", "garden_camera"),
                    "timestamp": when.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S"),
                    "image_url": public_url(art["image_url"]),
                }
                if visual:
                    # Do not label an arbitrary old JPEG as the latest detection.
                    # New captures carry a local metadata sidecar for this check.
                    payload.update(image_url="", snapshot_url="", snapshot_available=False)
                    sidecar = SNAPSHOT_DIR / "latest_visual.json"
                    try:
                        saved = json.loads(sidecar.read_text(encoding="utf-8"))
                        if (saved.get("common_name") == common and saved.get("timestamp") == payload["timestamp"]
                                and (SNAPSHOT_DIR / "latest_visual.jpg").is_file()):
                            payload.update(saved)
                            global latest_snapshot_event, latest_visual_event
                            latest_snapshot_event = str(saved.get("event_id", ""))
                            latest_visual_event = latest_snapshot_event
                            latest_visual_payload.update(saved)
                            mqtt_publish("wildlife/latest_visual/image", (SNAPSHOT_DIR / "latest_visual.jpg").read_bytes(), True)
                    except (OSError, ValueError, TypeError):
                        pass
                mqtt_publish("wildlife/latest_visual" if visual else "wildlife/recent_detection", json.dumps(payload), True)
                break
        except Exception as exc:
            LOG.warning("[HA SYNC] %s", safe_error(exc))


def update_ha_summary_loop() -> None:
    # Called independently of the MQTT callback so queries cannot block networking.
    if not stop_event.wait(2):
        sync_last_detection_from_influx()
    while not stop_event.is_set():
        publish_ha_summary()
        summary_needed.wait(SUMMARY_INTERVAL)
        summary_needed.clear()
        if stop_event.wait(1):
            return


# --- Observation windows, InfluxDB retrieval, original dwell/flock statistics. ---
VALID_TIMEFRAMES = {"1h", "today", "1d", "yesterday", "7d", "1w", "1m", "30d", "1y", "365d", "all"}


def ensure_aware(value: datetime.datetime) -> datetime.datetime:
    if not isinstance(value, datetime.datetime):
        raise ValueError("Detection does not contain a valid timestamp")
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


def utc_string(value: datetime.datetime) -> str:
    return ensure_aware(value).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_observation_window(timeframe="today", offset=0, now=None):
    if timeframe not in VALID_TIMEFRAMES:
        raise ValueError("Unsupported timeframe")
    offset = int(offset)
    if not 0 <= offset <= 10000:
        raise ValueError("Offset must be between 0 and 10000")
    now_local = ensure_aware(now or datetime.datetime.now(TZ)).astimezone(TZ)
    if timeframe == "yesterday":
        return get_observation_window("1d", offset + 1, now_local)
    if timeframe == "all":
        start = datetime.datetime(1970, 1, 1, tzinfo=UTC)
        return utc_string(start), None, "All-Time Catalogue", "All-Time Catalogue", start, now_local
    if timeframe == "1h":
        # Use elapsed time for the hourly view, including DST transitions.
        end = (now_local.astimezone(UTC) - datetime.timedelta(hours=offset)).astimezone(TZ)
        start = (end.astimezone(UTC) - datetime.timedelta(hours=1)).astimezone(TZ)
        label = start.strftime("%A, %d %b • %H:%M") + " - " + end.strftime("%H:%M")
        stats = label
    elif timeframe in {"today", "1d"}:
        # Local calendar midnights, including 23/25-hour DST days.
        date = now_local.date() - datetime.timedelta(days=offset)
        start = datetime.datetime.combine(date, datetime.time.min, tzinfo=TZ)
        end = datetime.datetime.combine(date + datetime.timedelta(days=1), datetime.time.min, tzinfo=TZ)
        display_day = start
        day = "Today" if offset == 0 else "Yesterday" if offset == 1 else display_day.strftime("%a %d %b")
        label = f"{day} ({display_day.strftime('%d %b %Y')})"
        stats = label + " • Midnight to midnight · " + TIMEZONE
    else:
        days = 7 if timeframe in {"7d", "1w"} else 30 if timeframe in {"1m", "30d"} else 365
        end = now_local - datetime.timedelta(days=offset*days)
        start = end - datetime.timedelta(days=days)
        prefix = "Week" if days == 7 else "Month" if days == 30 else "Year"
        label = f"{prefix}: {start.strftime('%d %b')} – {end.strftime('%d %b %Y')}"
        stats = label
    return utc_string(start), utc_string(end) if offset or timeframe in {"today", "1d"} else None, label, stats, start, end


def safe_error(exc: Exception) -> str:
    """Readable diagnostics, without dumping HTTP headers/tokens into the UI."""
    status = getattr(exc, "status", getattr(exc, "code", None))
    suffix = f", HTTP {status}" if status else ""
    if isinstance(exc, RuntimeError) and str(exc).startswith(("InfluxDB client is not configured", "InfluxDB query failed", "Install requirements.txt")):
        return str(exc)
    return f"{type(exc).__name__}{suffix}"


def query_records(query: str) -> list:
    global last_query_error
    if influx_query_api is None:
        raise RuntimeError("InfluxDB client is not configured; set INFLUX_TOKEN and INFLUX_URL")
    try:
        with query_lock:
            tables = influx_query_api.query(org=INFLUX_ORG, query=query)
        last_query_error = ""
        return [record for table in tables for record in table.records]
    except Exception as exc:
        last_query_error = "InfluxDB query failed (" + safe_error(exc) + "). Check URL, organisation, bucket and token."
        raise RuntimeError(last_query_error) from None


def compute_dwell_and_flocks(raw_detections):
    by_species = collections.defaultdict(list)
    for timestamp, name, _ in raw_detections:
        by_species[name].append(timestamp)
    dwell = {}
    for name, times in by_species.items():
        times.sort()
        sessions, start, end, calls = [], times[0], times[0], 1
        for timestamp in times[1:]:
            if timestamp - end <= 180:
                end, calls = timestamp, calls + 1
            else:
                sessions.append((max(0.5, (end - start) / 60.0), calls))
                start, end, calls = timestamp, timestamp, 1
        sessions.append((max(0.5, (end - start) / 60.0), calls))
        total = round(sum(s[0] for s in sessions), 1)
        avg = round(total / len(sessions), 1)
        behaviour = "Transitory / Flyover" if avg < 1 else "Active Forager" if avg <= 5 else "Territory Dwell"
        dwell[name] = {"sessions_count": len(sessions), "total_dwell_min": total,
                       "avg_dwell_min": avg, "behavior": behaviour}
    # Same pair-count semantics as the source's nested loop, implemented with a
    # sliding window so dense all-time histories do not compare every record pair.
    pairs = collections.Counter()
    active = collections.Counter()
    window = collections.deque()
    for timestamp, name, _ in sorted(raw_detections, key=lambda row: row[0]):
        while window and timestamp - window[0][0] > 90:
            _, old = window.popleft()
            active[old] -= 1
            if not active[old]:
                del active[old]
        for other, count in active.items():
            if other != name:
                pairs[tuple(sorted((name, other)))] += count
        window.append((timestamp, name))
        active[name] += 1
    cooccur = collections.defaultdict(dict)
    for (a, b), count in pairs.items():
        cooccur[a][b] = count
        cooccur[b][a] = count
    flocks = [{"species_a": a, "species_b": b, "co_occurrences": count}
              for (a, b), count in sorted(pairs.items(), key=lambda item: (-item[1], item[0])) if count >= 2]
    companions = {}
    for name, others in cooccur.items():
        other, count = max(others.items(), key=lambda item: (item[1], item[0]))
        companions[name] = {"companion": other, "count": count}
    return dwell, flocks, companions


def display_source(source: str, common: str, scientific: str = "") -> str:
    # Faithful to the later bat timeline patch: recognised bats are grouped in
    # the Bat Classifier display series. Existing raw Influx source tags remain
    # unchanged; this is a UI grouping rule, not proof of the upstream model.
    if get_target_type(common, scientific) == "bat":
        return "bat_classifier"
    source = source.lower()
    if "perch" in source:
        return "perch"
    if "frigate" in source or "visual" in source:
        return "frigate"
    if "bat" in source:
        return "bat_classifier"
    return "birdnet"


def get_active_species(timeframe="today", offset=0):
    start_iso, stop_iso, label, stats_label, start_dt, end_dt = get_observation_window(timeframe, offset)
    key = (timeframe, int(offset), start_iso if timeframe in {"today", "1d", "yesterday"} else "rolling")
    now = time.monotonic()
    with query_lock:
        cached = query_cache.get(key)
        generation = query_generation
        if cached and now - cached[0] < QUERY_CACHE_SECONDS:
            return copy.deepcopy(cached[1])
    range_clause = f"range(start: {start_iso}" + (f", stop: {stop_iso}" if stop_iso else "") + ")"
    query = (
        f'from(bucket: {json.dumps(INFLUX_BUCKET)})\n  |> {range_clause}\n'
        '  |> filter(fn: (r) => r._measurement == "wildlife_detection" and r._field == "confidence")'
    )
    records = query_records(query)
    index = artwork_index()
    species = {}
    raw = []
    for record in records:
        try:
            common, scientific = normalize_species(record.values.get("common_name", ""), record.values.get("scientific_name", ""))
            if not common:
                continue
            when = ensure_aware(record.get_time()).astimezone(TZ)
            source = str(record.values.get("source", "audio_birdnet"))
            category = display_source(source, common, scientific)
            raw.append((when.timestamp(), common, category))
            if common not in species:
                species[common] = {
                    "common_name": common, "scientific_name": scientific,
                    "target_type": get_target_type(common, scientific),
                    "count": 0, "first_seen_dt": when, "last_seen_dt": when,
                    "birdnet": False, "perch": False, "frigate": False, "bat_classifier": False,
                    "hourly_activity": [0]*24, **resolve_art(common, scientific, index),
                }
            item = species[common]
            item["count"] += 1
            item["hourly_activity"][when.hour] += 1
            item[category] = True
            item["first_seen_dt"] = min(item["first_seen_dt"], when)
            item["last_seen_dt"] = max(item["last_seen_dt"], when)
        except (TypeError, ValueError, AttributeError):
            LOG.warning("Skipping a malformed historic detection row")
    dwell, flocks, companions = compute_dwell_and_flocks(raw)
    format_time = "%d %b %H:%M" if timeframe in {"7d", "1w", "1m", "30d", "1y", "365d", "all"} else "%H:%M"
    output = []
    for item in sorted(species.values(), key=lambda x: (-x["count"], x["common_name"])):
        entry = dict(item)
        entry.update(window_count=item["count"], total_count=item["count"], windowCount=item["count"],
                     first_seen=item["first_seen_dt"].strftime(format_time),
                     last_seen=item["last_seen_dt"].strftime(format_time),
                     last_detected=item["last_seen_dt"].strftime(format_time), seen_today=True,
                     dwell=dwell[item["common_name"]], companion=companions.get(item["common_name"]))
        del entry["first_seen_dt"], entry["last_seen_dt"]
        output.append(entry)
    timeline = generate_multiseries_timeline(timeframe, raw, start_dt, end_dt, TZ)
    result = (output, label, stats_label, len(raw), timeline, flocks)
    with query_lock:
        if generation == query_generation:
            if len(query_cache) > 128:
                query_cache.clear()
            query_cache[key] = (now, copy.deepcopy(result))
    return result


def generate_multiseries_timeline(timeframe, raw_detections, start_dt, end_dt, tz_local):
    start_ts, end_ts = start_dt.timestamp(), end_dt.timestamp()
    buckets = []
    if timeframe == "all":
        if not raw_detections:
            return []
        low = min(row[0] for row in raw_detections)
        high = max(row[0] for row in raw_detections) + 0.000001
        step = max(1.0, high - low) / 12
        for i in range(12):
            a, b = low + i*step, low + (i+1)*step
            buckets.append((datetime.datetime.fromtimestamp(a, tz_local).strftime("%d %b"), a, b))
    elif timeframe in {"1y", "365d"}:
        cursor = start_dt
        while cursor < end_dt:
            following = cursor.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            following = following.replace(year=following.year+1, month=1) if following.month == 12 else following.replace(month=following.month+1)
            stop = min(following, end_dt)
            buckets.append((cursor.strftime("%b %Y"), cursor.timestamp(), stop.timestamp()))
            cursor = stop
    elif timeframe in {"7d", "1w", "1m", "30d"}:
        step_days = 1 if timeframe in {"7d", "1w"} else 2
        cursor = start_dt
        while cursor < end_dt:
            stop = min(cursor + datetime.timedelta(days=step_days), end_dt)
            label = cursor.strftime("%a %d" if step_days == 1 else "%d %b")
            buckets.append((label, cursor.timestamp(), stop.timestamp()))
            cursor = stop
    else:
        step = 300 if timeframe == "1h" else 3600
        cursor = start_ts
        # Elapsed hourly buckets correctly include both occurrences of a DST hour.
        while cursor < end_ts:
            when = datetime.datetime.fromtimestamp(cursor, tz_local)
            label = when.strftime("%H:%M")
            buckets.append((label, cursor, min(cursor+step, end_ts)))
            cursor += step
    output = [{"label": label, "count": 0, "species_sources": {}} for label, _, _ in buckets]
    starts = [a for _, a, _ in buckets]
    for timestamp, name, src in raw_detections:
        index = bisect.bisect_right(starts, timestamp)-1
        if index < 0 or index >= len(buckets) or timestamp >= buckets[index][2]:
            continue
        bucket = output[index]
        bucket["count"] += 1
        counts = bucket["species_sources"].setdefault(name, {
            "birdnet": 0, "perch": 0, "frigate": 0, "bat_classifier": 0, "total": 0,
        })
        effective = "bat_classifier" if get_target_type(name) == "bat" else src
        counts[effective] = counts.get(effective, 0)+1
        counts["total"] += 1
    return output


HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
    <title>Garden Friends</title>
    <link rel="icon" type="image/png" href="/art/favicon.png">
    <link rel="apple-touch-icon" href="/art/favicon.png">
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Cinzel:wght@600;800&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        /* Later PDF patches and reconstruction error presentation. */
        .src-bat_classifier { background:rgba(109,58,130,0.12); color:#6d3a82; border:1px solid rgba(109,58,130,0.25); }
        #connection-error { position:fixed; left:12px; right:12px; bottom:64px; z-index:400; padding:9px 14px; border:1px solid #c23b22; border-radius:10px; background:#fff3ed; color:#782719; font:12px Inter,sans-serif; display:none; }
        .species-item .sp-name { overflow-wrap:anywhere; }
        :root {
            --bg-paper: #f4eedb;
            --paper-dot: #e1d7b6;
            --text-ink: #2c2523;
            --subtext-ink: #7d7065;
            --card-bg: rgba(248, 244, 233, 0.90);
            --border-line: #dfd5ba;
            --accent-red: #c23b22;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }

        html, body {
            background-color: var(--bg-paper) !important;
            color: var(--text-ink);
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            min-height: 100vh;
            min-height: -webkit-fill-available;
            height: 100vh;
            height: 100dvh;
            display: flex;
            flex-direction: column;
            overflow: hidden;
            background-image: radial-gradient(var(--paper-dot) 0.75px, transparent 0.75px);
            background-size: 24px 24px;
            -webkit-tap-highlight-color: transparent;
        }

        .top-bar {
            position: absolute;
            top: max(12px, env(safe-area-inset-top, 12px));
            left: max(12px, env(safe-area-inset-left, 12px));
            right: max(12px, env(safe-area-inset-right, 12px));
            display: flex;
            align-items: flex-start;
            justify-content: space-between;
            gap: 8px;
            z-index: 120;
            pointer-events: auto;
        }

        .controls-left {
            display: flex;
            flex-direction: column;
            gap: 6px;
            align-items: flex-start;
            min-width: 0;
            flex: 1;
        }

        .time-nav-row {
            display: flex;
            align-items: center;
            gap: 5px;
            flex-wrap: wrap;
            max-width: 100%;
        }

        .step-btn-group {
            display: flex;
            background: rgba(230, 222, 201, 0.92);
            border: 1px solid var(--border-line);
            padding: 3px;
            border-radius: 20px;
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px);
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            gap: 2px;
            flex-shrink: 0;
        }
        .step-btn {
            background: transparent;
            border: none;
            outline: none;
            padding: 4px 8px;
            font-size: 11px;
            font-weight: 700;
            color: #4a3e35;
            cursor: pointer;
            border-radius: 14px;
            transition: all 0.15s ease;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .step-btn:hover:not(:disabled) {
            background: #2c2523;
            color: #fcf9f2;
        }
        .step-btn:disabled {
            opacity: 0.3;
            cursor: not-allowed;
        }

        .time-toggle {
            display: flex;
            flex-wrap: wrap;
            background: rgba(230, 222, 201, 0.92);
            border: 1px solid var(--border-line);
            padding: 3px;
            border-radius: 20px;
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px);
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            gap: 1px;
        }
        .time-btn {
            background: transparent;
            border: none;
            outline: none;
            padding: 4px 9px;
            font-size: clamp(10px, 1.8vw, 11px);
            font-weight: 600;
            color: #6d6054;
            cursor: pointer;
            border-radius: 16px;
            transition: all 0.2s ease;
            white-space: nowrap;
        }
        .time-btn.active {
            background: #2c2523;
            color: #fcf9f2;
            box-shadow: 0 2px 4px rgba(44, 37, 35, 0.2);
        }

        .stats-chip {
            background: var(--card-bg);
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px);
            border: 1px solid var(--border-line);
            padding: 3px 10px;
            border-radius: 16px;
            font-size: clamp(10px, 1.8vw, 11px);
            font-weight: 600;
            color: var(--text-ink);
            box-shadow: 0 2px 8px rgba(0,0,0,0.04);
            white-space: nowrap;
        }

        .view-btn {
            background: rgba(230, 222, 201, 0.92);
            border: 1px solid var(--border-line);
            padding: 6px 13px;
            border-radius: 20px;
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px);
            box-shadow: 0 2px 8px rgba(0,0,0,0.06);
            font-size: clamp(11px, 2.0vw, 12px);
            font-weight: 700;
            color: #2c2523;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: all 0.2s ease;
            white-space: nowrap;
            flex-shrink: 0;
        }
        .view-btn:active, .view-btn:hover {
            background: #2c2523;
            color: #fcf9f2;
            box-shadow: 0 4px 12px rgba(44, 37, 35, 0.25);
        }

        .bottom-pose-container {
            position: absolute;
            bottom: max(14px, env(safe-area-inset-bottom, 14px));
            left: 50%;
            transform: translateX(-50%);
            z-index: 120;
            pointer-events: auto;
        }

        .pose-toggle {
            display: flex;
            background: rgba(230, 222, 201, 0.94);
            border: 1px solid var(--border-line);
            padding: 3px;
            border-radius: 22px;
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px);
            box-shadow: 0 4px 16px rgba(44, 37, 35, 0.12);
            gap: 2px;
        }

        .pose-btn {
            background: transparent;
            border: none;
            outline: none;
            padding: 5px 14px;
            font-size: clamp(11px, 2.0vw, 12px);
            font-weight: 600;
            color: #6d6054;
            cursor: pointer;
            border-radius: 18px;
            transition: all 0.2s ease;
            white-space: nowrap;
        }

        .pose-btn.active {
            background: #2c2523;
            color: #fcf9f2;
            box-shadow: 0 2px 4px rgba(44, 37, 35, 0.25);
        }

        header {
            position: absolute;
            left: 0;
            right: 0;
            display: flex;
            flex-direction: column;
            align-items: center;
            text-align: center;
            z-index: 100;
            pointer-events: none;
            padding: 0 16px;
        }
        .brand { 
            pointer-events: auto;
            max-width: 92vw;
        }
        .brand h1 {
            font-family: 'Cinzel', serif;
            font-size: clamp(16px, 3.0vw, 25px);
            font-weight: 800;
            letter-spacing: 1.2px;
            color: var(--text-ink);
            text-transform: uppercase;
            line-height: 1.2;
        }
        .brand .period {
            font-size: clamp(11px, 2.0vw, 13px);
            color: var(--subtext-ink);
            margin-top: 4px;
            font-weight: 500;
            letter-spacing: 0.5px;
        }

        #stage-container {
            flex: 1;
            position: relative;
            width: 100vw;
            height: 100vh;
            height: 100dvh;
            overflow: hidden;
            touch-action: pan-x pan-y;
        }

        .avian-tile {
            position: absolute;
            cursor: pointer;
            transition: transform 0.25s cubic-bezier(0.16, 1, 0.3, 1), filter 0.25s ease;
            transform-origin: center center;
            user-select: none;
            -webkit-user-select: none;
            background: transparent !important;
            border: none !important;
            pointer-events: none;
        }
        .avian-tile.hovered {
            transform: scale(1.08) !important;
            z-index: 95 !important;
            filter: drop-shadow(0 14px 24px rgba(44, 37, 35, 0.42)) !important;
        }

        .avian-tile img {
            width: 100%;
            height: 100%;
            object-fit: fill;
            pointer-events: none;
            background: transparent !important;
            filter: drop-shadow(0 4px 10px rgba(44, 37, 35, 0.15)) !important;
            opacity: 1.0 !important;
        }

        #tooltip {
            position: fixed;
            pointer-events: none;
            background: rgba(30, 25, 23, 0.95);
            color: #fdfbf7;
            padding: 8px 14px;
            border-radius: 8px;
            font-size: 13px;
            backdrop-filter: blur(6px);
            -webkit-backdrop-filter: blur(6px);
            box-shadow: 0 8px 24px rgba(0,0,0,0.25);
            display: none;
            z-index: 200;
            transition: opacity 0.15s ease;
            max-width: 85vw;
            border: 1px solid rgba(255,255,255,0.12);
        }
        #tooltip .t-name { font-weight: 700; font-size: 13px; margin-bottom: 2px; }
        #tooltip .t-sub { font-size: 11px; font-style: italic; color: #d2c4b5; }
        #tooltip .t-count { font-size: 11px; color: #f59e0b; margin-top: 4px; font-weight: 600; }
        #tooltip .t-dwell { font-size: 10px; color: #b8ccbb; margin-top: 3px; }

        .empty-banner {
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            text-align: center;
            color: var(--subtext-ink);
            font-family: 'Cinzel', serif;
            font-size: clamp(14px, 3.5vw, 18px);
            letter-spacing: 1px;
            padding: 0 20px;
            width: 100%;
        }

        #stats-container {
            display: none;
            flex: 1;
            width: 100vw;
            height: 100vh;
            height: 100dvh;
            padding: 16px;
            overflow-y: auto;
            -webkit-overflow-scrolling: touch;
            z-index: 50;
        }

        .stats-wrapper {
            display: flex;
            flex-direction: column;
            gap: 16px;
            max-width: 1100px;
            margin: 0 auto;
            width: 100%;
            padding-bottom: 40px;
        }

        .stats-header-card {
            text-align: center;
            padding: 80px 16px 8px 16px;
            display: flex;
            flex-direction: column;
            align-items: center;
        }
        .stats-header-card h1 {
            font-family: 'Cinzel', serif;
            font-size: clamp(18px, 3.4vw, 26px);
            font-weight: 800;
            letter-spacing: 1.5px;
            color: var(--text-ink);
            text-transform: uppercase;
        }
        .stats-header-card .period {
            font-size: clamp(12px, 2.0vw, 14px);
            color: var(--subtext-ink);
            margin-top: 5px;
            font-weight: 500;
        }
        .panel-card {
            background: var(--card-bg);
            border: 1px solid var(--border-line);
            border-radius: 16px;
            padding: 18px;
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px);
            box-shadow: 0 4px 20px rgba(44, 37, 35, 0.06);
            display: flex;
            flex-direction: column;
            width: 100%;
        }

        .panel-title {
            font-family: 'Cinzel', serif;
            font-size: 15px;
            font-weight: 800;
            color: var(--text-ink);
            letter-spacing: 1px;
            margin-bottom: 12px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--border-line);
            padding-bottom: 10px;
        }

        .species-list {
            list-style: none;
            overflow-y: auto;
            max-height: 520px;
            padding-right: 6px;
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 10px;
        }

        .species-item {
            display: flex;
            align-items: center;
            justify-content: space-between;
            background: rgba(255, 255, 255, 0.45);
            padding: 10px 12px;
            border-radius: 10px;
            border: 1px solid var(--border-line);
            transition: transform 0.15s ease;
        }
        .species-item:hover {
            transform: translateX(4px);
            background: rgba(255, 255, 255, 0.75);
        }

        .sp-info {
            display: flex;
            align-items: center;
            gap: 10px;
            min-width: 0;
            flex: 1;
        }
        .sp-thumb {
            width: 40px;
            height: 40px;
            object-fit: contain;
            filter: drop-shadow(0 2px 4px rgba(44,37,35,0.15));
            flex-shrink: 0;
        }
        .sp-name-row {
            display: flex;
            align-items: center;
            gap: 6px;
            flex-wrap: wrap;
        }
        .sp-name {
            font-weight: 700;
            font-size: 13px;
            color: var(--text-ink);
        }
        .sp-sci {
            font-size: 11px;
            font-style: italic;
            color: var(--subtext-ink);
        }

        .source-badges {
            display: flex;
            gap: 4px;
            margin-top: 3px;
            flex-wrap: wrap;
            align-items: center;
        }
        .src-badge {
            font-size: 9px;
            font-weight: 700;
            padding: 2px 6px;
            border-radius: 6px;
            letter-spacing: 0.3px;
            text-transform: uppercase;
        }
        .src-birdnet {
            background: rgba(43, 74, 111, 0.12);
            color: #2b4a6f;
            border: 1px solid rgba(43, 74, 111, 0.25);
        }
        .src-perch {
            background: rgba(67, 107, 71, 0.12);
            color: #385d3b;
            border: 1px solid rgba(67, 107, 71, 0.25);
        }
        .src-frigate {
            background: rgba(184, 74, 57, 0.12);
            color: #a83d2d;
            border: 1px solid rgba(184, 74, 57, 0.25);
        }

        .dwell-badge {
            background: rgba(76, 120, 88, 0.12);
            color: #2e5936;
            border: 1px solid rgba(76, 120, 88, 0.25);
            font-size: 9px;
            font-weight: 700;
            padding: 2px 6px;
            border-radius: 6px;
            letter-spacing: 0.3px;
        }

        .companion-badge {
            background: rgba(199, 125, 40, 0.12);
            color: #8c5311;
            border: 1px solid rgba(199, 125, 40, 0.25);
            font-size: 9px;
            font-weight: 700;
            padding: 2px 6px;
            border-radius: 6px;
            letter-spacing: 0.3px;
        }

        .sp-badge {
            background: #2c2523;
            color: #fdfbf7;
            font-weight: 700;
            font-size: 12px;
            padding: 4px 9px;
            border-radius: 12px;
            display: flex;
            flex-direction: column;
            align-items: flex-end;
            min-width: 46px;
            text-align: right;
            margin-left: 8px;
            flex-shrink: 0;
        }
        .sp-pct {
            font-size: 9px;
            color: #d2c4b5;
            font-weight: 500;
        }

        .flock-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
            gap: 10px;
        }
        .flock-card {
            background: rgba(255, 255, 255, 0.45);
            border: 1px solid var(--border-line);
            border-radius: 10px;
            padding: 10px 14px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .flock-pair {
            font-weight: 700;
            font-size: 12px;
            color: var(--text-ink);
        }
        .flock-count {
            font-size: 11px;
            font-weight: 700;
            color: var(--accent-red);
            background: rgba(194, 59, 34, 0.10);
            padding: 2px 8px;
            border-radius: 10px;
        }

        .sp-heatmap-row {
            display: grid;
            grid-template-columns: repeat(24, 1fr);
            gap: 1.5px;
            height: 6px;
            margin-top: 5px;
            width: 100%;
        }
        .hm-cell {
            background: rgba(0,0,0,0.06);
            border-radius: 1px;
        }
        .hm-1 { background: #90caf9; }
        .hm-2 { background: #42a5f5; }
        .hm-3 { background: #1976d2; }
        .hm-max { background: var(--accent-red); }

        .chart-filter-bar {
            display: flex;
            align-items: center;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 8px;
            margin-bottom: 12px;
            padding-bottom: 8px;
            border-bottom: 1px dashed var(--border-line);
        }

        .source-checkboxes {
            display: flex;
            align-items: center;
            gap: 12px;
            flex-wrap: wrap;
        }

        .source-checkbox-label {
            display: inline-flex;
            align-items: center;
            gap: 5px;
            font-size: 12px;
            font-weight: 600;
            color: var(--text-ink);
            cursor: pointer;
            user-select: none;
        }

        .source-checkbox-label input[type="checkbox"] {
            accent-color: #2c2523;
            width: 14px;
            height: 14px;
            cursor: pointer;
        }

        .timeline-container {
            width: 100%;
            height: 320px;
            position: relative;
            cursor: crosshair;
        }
        .timeline-legend {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
            margin-bottom: 12px;
            max-height: 80px;
            overflow-y: auto;
            padding: 2px;
        }

        .legend-pill {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            font-size: 11px;
            font-weight: 600;
            color: var(--text-ink);
            background: rgba(255, 255, 255, 0.65);
            border: 1px solid var(--border-line);
            padding: 3px 9px;
            border-radius: 12px;
            cursor: pointer;
            user-select: none;
            transition: all 0.15s ease;
        }
        .legend-pill.muted {
            opacity: 0.35;
            text-decoration: line-through;
        }
        .legend-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
        }
        #timeline-chart-canvas {
            width: 100%;
            height: 100%;
            display: block;
        }

        #timeline-tooltip {
            position: absolute;
            pointer-events: none;
            background: rgba(30, 25, 23, 0.95);
            color: #fdfbf7;
            padding: 10px 14px;
            border-radius: 10px;
            font-size: 12px;
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px);
            box-shadow: 0 8px 24px rgba(0,0,0,0.3);
            display: none;
            z-index: 250;
            min-width: 180px;
            border: 1px solid rgba(255,255,255,0.12);
        }
        #timeline-tooltip .tt-time {
            font-weight: 800;
            color: #f59e0b;
            font-size: 13px;
            margin-bottom: 6px;
            border-bottom: 1px solid rgba(255,255,255,0.15);
            padding-bottom: 4px;
        }
        #timeline-tooltip .tt-total {
            font-weight: 700;
            font-size: 12px;
            margin-bottom: 6px;
            color: #fcf9f2;
        }
        #timeline-tooltip .tt-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 11px;
            gap: 14px;
            margin-top: 3px;
        }
        #timeline-tooltip .tt-sp-name {
            display: flex;
            align-items: center;
            gap: 6px;
            color: #d6ccbe;
        }
        #timeline-tooltip .tt-sp-dot {
            width: 7px;
            height: 7px;
            border-radius: 50%;
        }
        #timeline-tooltip .tt-sp-val {
            font-weight: 700;
            color: #fcf9f2;
        }

        .gf-stats-heading { display:flex; align-items:center; justify-content:space-between; gap:12px; flex-wrap:wrap; }
        .gf-settings-link { text-decoration:none; min-height:36px; }
        @media (max-width: 600px) {
            .species-list {
                grid-template-columns: 1fr;
            }
            .stats-header-card {
                padding-top: 90px;
            }
            .timeline-container {
                height: 260px;
            }
        }
    </style>

    <style id="avian-github-link-style">
        /* AVIAN_GITHUB_LINK_V1 */
        #avian-github-link {
            position: fixed;
            right: max(12px, env(safe-area-inset-right, 0px));
            bottom: max(12px, env(safe-area-inset-bottom, 0px));
            z-index: 120;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 7px;
            min-width: 40px;
            height: 40px;
            padding: 0 12px;
            border: 1px solid var(--border-line, #dfd5ba);
            border-radius: 20px;
            background: var(--card-bg, #f8f4e9);
            color: var(--text-ink, #2c2523);
            font: 600 11px/1 'Inter', sans-serif;
            text-decoration: none;
            pointer-events: auto;
            -webkit-tap-highlight-color: transparent;
        }
        #avian-github-link svg { width: 18px; height: 18px; flex: none; fill: currentColor; }
        #avian-github-link:hover { background: var(--text-ink, #2c2523); color: #fcf9f2; }
        #avian-github-link:focus-visible { outline: 2px solid var(--accent-red, #c23b22); outline-offset: 3px; }
        #stats-container .stats-wrapper { padding-bottom: calc(58px + env(safe-area-inset-bottom, 0px)); }
        @media (max-width: 640px) {
            #avian-github-link { width: 40px; padding: 0; }
            #avian-github-link span { display: none; }
        }
        @media (max-width: 480px) {
            #bottom-pose-container {
                left: max(12px, env(safe-area-inset-left, 0px));
                right: calc(max(12px, env(safe-area-inset-right, 0px)) + 48px);
                transform: none;
            }
            #bottom-pose-container .pose-toggle { justify-content: center; }
            #bottom-pose-container .pose-btn { font-size: 10px; padding: 5px 7px; }
        }
    </style>
</head>
<body>
    <div class="top-bar" id="top-bar">
        <div class="controls-left">
            <div class="time-nav-row">
                <div class="step-btn-group">
                    <button class="step-btn" id="step-prev-btn" title="Step Back in Time">◀</button>
                    <button class="step-btn" id="step-next-btn" title="Step Forward in Time" disabled>▶</button>
                </div>
                <div class="time-toggle" id="time-toggle"></div>
            </div>
            <div class="stats-chip" id="stats-chip">0 Visitors • 0 Detections</div>
        </div>
        <button class="view-btn" id="toggle-view-btn">Statistics</button>
    </div>

    <div class="bottom-pose-container" id="bottom-pose-container">
        <div class="pose-toggle">
            <button class="pose-btn" data-mode="perched">Perched / Ground</button>
            <button class="pose-btn active" data-mode="random">Random</button>
            <button class="pose-btn" data-mode="flight">Action / Flight</button>
        </div>
    </div>

    <header id="main-header">
        <div class="brand">
            <h1>Garden Friends</h1>
            <div class="period" id="period-label">Loading window...</div>
        </div>
    </header>

    <div id="stage-container">
        <div class="empty-banner" id="empty-state">Observing sky, trees & garden undergrowth...</div>
    </div>

    <div id="stats-container">
        <div class="stats-wrapper">
            <div class="stats-header-card">
                <div class="gf-stats-heading">
                    <h1>Garden Friends</h1>
                    <a class="view-btn gf-settings-link" href="/settings" aria-label="Garden Friends settings">Settings</a>
                </div>
                <div class="period" id="stats-period-label">Loading window...</div>
            </div>

            <div class="panel-card">
                <div class="panel-title">
                    <span>Species Activity & Dwell Behavior</span>
                    <span style="font-size:12px; color:var(--subtext-ink); font-family:'Inter';" id="stats-list-subtitle">0 Recorded</span>
                </div>
                <div class="chart-filter-bar" style="margin-top:4px; padding:4px 0;">
                    <div style="font-size:12px; font-weight:700;">Show:</div>
                    <div class="source-checkboxes">
                        <label class="source-checkbox-label"><input type="checkbox" id="chk-taxon-all" checked> All</label>
                        <label class="source-checkbox-label"><input type="checkbox" id="chk-taxon-birds" checked> Birds 🐦</label>
                        <label class="source-checkbox-label"><input type="checkbox" id="chk-taxon-mammals" checked> Mammals &amp; Bats 🦇</label>
                    </div>
                </div>
                <ul class="species-list" id="species-list-ul"></ul>
            </div>

            <div class="panel-card">
                <div class="panel-title">
                    <span>Mixed Foraging Bands & Co-Occurrences</span>
                    <span style="font-size:12px; color:var(--subtext-ink); font-family:'Inter';" id="flock-subtitle">Visits within 90s</span>
                </div>
                <div class="flock-grid" id="flock-grid"></div>
            </div>

            <div class="panel-card">
                <div class="panel-title">
                    <span>Visits Over Time</span>
                    <span style="font-size:12px; color:var(--subtext-ink); font-family:'Inter';" id="timeline-subtitle">Hover over chart for details</span>
                </div>

                <div class="chart-filter-bar">
                    <div style="font-size:12px; font-weight:700; color:var(--text-ink);">Sources:</div>
                    <div class="source-checkboxes">
                        <label class="source-checkbox-label">
                            <input type="checkbox" id="chk-birdnet" checked> BirdNET
                        </label>
                        <label class="source-checkbox-label">
                            <input type="checkbox" id="chk-perch" checked> Perch
                        </label>
                        <label class="source-checkbox-label">
                            <input type="checkbox" id="chk-frigate" checked> Frigate
                        </label>
                        <label class="source-checkbox-label"><input type="checkbox" id="chk-bat" checked> Bat Classifier</label>
                        <label class="source-checkbox-label"><input type="checkbox" id="chk-total" checked> Total Curve</label>
                    </div>
                </div>

                <div class="timeline-legend" id="timeline-legend"></div>
                <div class="timeline-container" id="timeline-wrapper">
                    <canvas id="timeline-chart-canvas"></canvas>
                    <div id="timeline-tooltip"></div>
                </div>
            </div>
        </div>
    </div>

    <div id="connection-error" role="status"></div>
    <div id="tooltip">
        <div class="t-name" id="tt-name"></div>
        <div class="t-sub" id="tt-sub"></div>
        <div class="t-count" id="tt-count"></div>
        <div class="t-dwell" id="tt-dwell"></div>
    </div>

    <script>
        const stage = document.getElementById('stage-container');
        const statsView = document.getElementById('stats-container');
        const tooltip = document.getElementById('tooltip');
        const header = document.getElementById('main-header');
        const topBar = document.getElementById('top-bar');
        const bottomPoseContainer = document.getElementById('bottom-pose-container');
        const timeToggle = document.getElementById('time-toggle');
        const toggleViewBtn = document.getElementById('toggle-view-btn');
        const timelineCanvas = document.getElementById('timeline-chart-canvas');
        const timelineWrapper = document.getElementById('timeline-wrapper');
        const timelineTooltip = document.getElementById('timeline-tooltip');
        const timelineLegend = document.getElementById('timeline-legend');
        const prevBtn = document.getElementById('step-prev-btn');
        const nextBtn = document.getElementById('step-next-btn');

        const chkBirdnet = document.getElementById('chk-birdnet');
        const chkPerch = document.getElementById('chk-perch');
        const chkFrigate = document.getElementById('chk-frigate');
        const chkBat = document.getElementById('chk-bat');
        const chkTotal = document.getElementById('chk-total');
        const chkTaxonAll = document.getElementById('chk-taxon-all');
        const chkTaxonBirds = document.getElementById('chk-taxon-birds');
        const chkTaxonMammals = document.getElementById('chk-taxon-mammals');
        let rawSpeciesList = [];
        let rawTotalCalls = 0;
        let fetchSequence = 0;

        let currentView = 'collage';
        let collageTimeframe = 'today';
        let statsTimeframe = '1d';
        let currentTimeOffset = 0;
        let activePoseMode = 'random';
        let currentSpeciesData = [];
        let currentTimelineData = [];
        let currentFlocksData = [];
        let silhouetteMasks = {};
        let touchTimeout = null;
        let activeHoverIndex = null;
        let mutedSeries = new Set();
        let placedTileRecords = [];

        const EDO_PALETTE = [
            '#2b4a6f', '#b84a39', '#436b47', '#c77d28', '#7a3b5c', '#4a6b6c', '#8c5e39', '#555555'
        ];
        const TOTAL_COLOR = '#1f1a18';

        const COLLAGE_PERIODS = [
            { key: 'today', label: '1 Day' },
            { key: '7d', label: '7 Days' },
            { key: 'all', label: 'All Time' }
        ];

        const STATS_PERIODS = [
            { key: '1h', label: '1 Hour' },
            { key: '1d', label: '1 Day' },
            { key: '1w', label: '1 Week' },
            { key: '1m', label: '1 Month' },
            { key: '1y', label: '1 Year' },
            { key: 'all', label: 'All Time' }
        ];

        function getFifteenMinuteSeed() {
            return Math.floor(Date.now() / (15 * 60 * 1000));
        }

        function shouldShowFlight(sp) {
            if (!sp.has_flight) return false;
            if (activePoseMode === 'flight') return true;
            if (activePoseMode === 'perched') return false;

            const seed = getFifteenMinuteSeed();
            let hash = seed;
            for (let i = 0; i < sp.slug.length; i++) {
                hash = ((hash << 5) - hash) + sp.slug.charCodeAt(i);
                hash |= 0;
            }
            return (Math.abs(hash) % 2) === 1;
        }

        document.querySelectorAll('.pose-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                document.querySelectorAll('.pose-btn').forEach(b => b.classList.remove('active'));
                e.target.classList.add('active');
                activePoseMode = e.target.dataset.mode;
                if (currentView === 'collage' && currentSpeciesData.length > 0) {
                    packCollage(currentSpeciesData);
                }
            });
        });

        function renderTimeButtons() {
            timeToggle.innerHTML = '';
            const periods = currentView === 'collage' ? COLLAGE_PERIODS : STATS_PERIODS;
            const activeKey = currentView === 'collage' ? collageTimeframe : statsTimeframe;

            periods.forEach(p => {
                const btn = document.createElement('button');
                btn.className = `time-btn ${p.key === activeKey ? 'active' : ''}`;
                btn.textContent = p.label;
                btn.dataset.time = p.key;
                btn.addEventListener('click', () => {
                    if (currentView === 'collage') {
                        collageTimeframe = p.key;
                    } else {
                        statsTimeframe = p.key;
                    }
                    currentTimeOffset = 0;
                    updateStepButtonStates();
                    renderTimeButtons();
                    fetchAndUpdate(true);
                });
                timeToggle.appendChild(btn);
            });
            updateStepButtonStates();
        }

        function updateStepButtonStates() {
            const activeKey = currentView === 'collage' ? collageTimeframe : statsTimeframe;
            if (activeKey === 'all') {
                prevBtn.disabled = true;
                nextBtn.disabled = true;
            } else {
                prevBtn.disabled = false;
                nextBtn.disabled = (currentTimeOffset <= 0);
            }
        }

        prevBtn.addEventListener('click', () => {
            currentTimeOffset += 1;
            updateStepButtonStates();
            fetchAndUpdate(true);
        });

        nextBtn.addEventListener('click', () => {
            if (currentTimeOffset > 0) {
                currentTimeOffset -= 1;
                updateStepButtonStates();
                fetchAndUpdate(true);
            }
        });

        [chkBirdnet, chkPerch, chkFrigate, chkBat].forEach(chk => {
            chk.addEventListener('change', () => {
                drawTimelineChart(currentTimelineData);
            });
        });

        chkTotal.addEventListener('change', () => {
            if (chkTotal.checked) mutedSeries.delete('__total__');
            else mutedSeries.add('__total__');
            renderTimelineLegend(currentSpeciesData);
            drawTimelineChart(currentTimelineData);
        });
        chkTaxonAll.addEventListener('change', () => {
            chkTaxonBirds.checked = chkTaxonAll.checked;
            chkTaxonMammals.checked = chkTaxonAll.checked;
            refreshSpeciesListDisplay();
        });
        [chkTaxonBirds, chkTaxonMammals].forEach(chk => chk.addEventListener('change', () => {
            chkTaxonAll.checked = chkTaxonBirds.checked && chkTaxonMammals.checked;
            refreshSpeciesListDisplay();
        }));

        toggleViewBtn.addEventListener('click', () => {
            currentTimeOffset = 0;
            if (currentView === 'collage') {
                currentView = 'stats';
                stage.style.display = 'none';
                header.style.display = 'none';
                bottomPoseContainer.style.display = 'none';
                statsView.style.display = 'block';
                toggleViewBtn.textContent = 'Collage';
            } else {
                currentView = 'collage';
                statsView.style.display = 'none';
                stage.style.display = 'block';
                header.style.display = 'flex';
                bottomPoseContainer.style.display = 'block';
                toggleViewBtn.textContent = 'Statistics';
            }
            renderTimeButtons();
            fetchAndUpdate(true);
        });

        async function loadMasks() {
            try {
                const res = await fetch('/api/masks?ts=' + Date.now());
                if (res.ok) silhouetteMasks = await res.json();
            } catch (e) {
                console.warn("Masks not loaded:", e);
            }
        }

        function updateHeaderPosition() {
            if (currentView !== 'collage') return;
            const H = window.innerHeight;
            const W = window.innerWidth;
            const isPortrait = H > W;

            if (isPortrait) {
                const topBarRect = topBar.getBoundingClientRect();
                const targetHeaderTop = Math.round(topBarRect.bottom + 8);
                header.style.top = `${targetHeaderTop}px`;
            } else {
                header.style.top = '20px';
            }
        }

        function escapeHTML(value) {
            return String(value == null ? '' : value).replace(/[&<>"']/g, c => ({'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'}[c]));
        }
        function isBat(sp) {
            const text = [sp.common_name, sp.scientific_name, sp.target_type, sp.taxon, sp.type].join(' ').toLowerCase();
            return /\bbat\b|pipistrelle|noctule|serotine|barbastelle|nyctalus|myotis|plecotus|rhinolophus|eptesicus|hypsugo|miniopterus|pipistrellus/i.test(text);
        }
        function getDisplayName(sp) {
            // Scientific-name-to-English recovery supplied by the later bat patches.
            const batNames = {"nyctalus leisleri": "Lesser Noctule / Leisler's Bat", "nyctalus noctula": "Noctule Bat", "pipistrellus pipistrellus": "Common Pipistrelle", "pipistrellus pygmaeus": "Soprano Pipistrelle", "pipistrellus nathusii": "Nathusius's Pipistrelle", "myotis nattereri": "Natterer's Bat", "myotis daubentonii": "Daubenton's Bat", "myotis mystacinus": "Whiskered Bat", "myotis brandtii": "Brandt's Bat", "myotis alcathoe": "Alcathoe Bat", "myotis bechsteinii": "Bechstein's Bat", "myotis myotis": "Greater Mouse-eared Bat", "plecotus auritus": "Brown Long-eared Bat", "plecotus austriacus": "Grey Long-eared Bat", "eptesicus serotinus": "Serotine Bat", "rhinolophus ferrumequinum": "Greater Horseshoe Bat", "rhinolophus hipposideros": "Lesser Horseshoe Bat", "barbastella barbastellus": "Barbastelle Bat", "miniopterus schreibersii": "Schreibers's Bent-winged Bat", "hypsugo savii": "Savi's Pipistrelle"};
            const key = String(sp.scientific_name || sp.common_name || '').trim().toLowerCase();
            return batNames[key] || sp.common_name || sp.scientific_name || 'Unknown';
        }
        function getTargetIcon(targetType, sp = {}) {
            if (targetType === 'bat' || isBat(sp)) return '🦇 ';
            if (targetType === 'mammal') return '🦔 ';
            return '🐦 ';
        }
        function getSpeciesTaxonCategory(sp) {
            if (isBat(sp)) return 'mammal';
            const type = String(sp.target_type || sp.taxon || sp.type || '').toLowerCase();
            const name = String(sp.common_name || '').toLowerCase();
            if (type === 'mammal' || type === 'wildlife' || /hedgehog|fox|badger|squirrel|mouse|rat\b|deer|muntjac|stoat|weasel|rabbit|hare|otter|mole|shrew|vole/.test(name)) return 'mammal';
            return 'bird';
        }

        function showTooltip(x, y, sp) {
            tooltip.style.display = 'block';
            const tooltipWidth = 200;
            const posX = Math.min(x + 14, window.innerWidth - tooltipWidth - 10);
            const posY = Math.max(10, y - 60);

            tooltip.style.left = `${posX}px`;
            tooltip.style.top = `${posY}px`;

            document.getElementById('tt-name').textContent = `${getTargetIcon(sp.target_type, sp)}${getDisplayName(sp)}`;
            document.getElementById('tt-sub').textContent = sp.scientific_name;
            document.getElementById('tt-count').textContent = `${sp.count} detection${sp.count > 1 ? 's' : ''} • Last seen: ${sp.last_seen}`;

            if (sp.dwell) {
                document.getElementById('tt-dwell').textContent = `Dwell: ~${sp.dwell.total_dwell_min}m (${sp.dwell.behavior})`;
            } else {
                document.getElementById('tt-dwell').textContent = '';
            }
        }

        function hideTooltip() {
            tooltip.style.display = 'none';
            document.querySelectorAll('.avian-tile').forEach(el => el.classList.remove('hovered'));
        }

        function findBirdAtPoint(clientX, clientY) {
            const rect = stage.getBoundingClientRect();
            const relX = clientX - rect.left;
            const relY = clientY - rect.top;

            for (let i = placedTileRecords.length - 1; i >= 0; i--) {
                const item = placedTileRecords[i];
                if (
                    relX >= item.x &&
                    relX <= item.x + item.w &&
                    relY >= item.y &&
                    relY <= item.y + item.h
                ) {
                    const localX = (relX - item.x) / item.w;
                    const localY = (relY - item.y) / item.h;

                    if (item.maskData && item.maskData.bitmap && item.maskData.bitmap.length > 0) {
                        const rawW = item.maskData.w;
                        const rawH = item.maskData.h;
                        const srcY = Math.min(rawH - 1, Math.max(0, Math.floor(localY * rawH)));
                        const srcX = Math.min(rawW - 1, Math.max(0, Math.floor(localX * rawW)));
                        const rowStr = item.maskData.bitmap[srcY] || "";
                        if (rowStr.charCodeAt(srcX) === 49) {
                            return item;
                        }
                    } else {
                        const nx = localX - 0.5;
                        const ny = localY - 0.5;
                        if (nx * nx + ny * ny <= 0.22) {
                            return item;
                        }
                    }
                }
            }
            return null;
        }
        function handleStagePointerMove(e) {
            if (currentView !== 'collage') return;
            const clientX = e.touches ? e.touches[0].clientX : e.clientX;
            const clientY = e.touches ? e.touches[0].clientY : e.clientY;

            const hit = findBirdAtPoint(clientX, clientY);
            document.querySelectorAll('.avian-tile').forEach(el => el.classList.remove('hovered'));

            if (hit) {
                hit.element.classList.add('hovered');
                showTooltip(clientX, clientY, hit.species);
            } else {
                hideTooltip();
            }
        }

        stage.addEventListener('mousemove', handleStagePointerMove);
        stage.addEventListener('mouseleave', hideTooltip);
        stage.addEventListener('touchstart', (e) => {
            handleStagePointerMove(e);
            if (touchTimeout) clearTimeout(touchTimeout);
            touchTimeout = setTimeout(hideTooltip, 4500);
        }, { passive: true });

                // AVIAN_DESKTOP_COLLAGE_FILL_V2: uniform enlargement after mask-safe packing.
        // The live packer still controls animal ratios; this only fills spare PC space.
        function maximizeDesktopCollage() {
            const W = window.innerWidth, H = window.innerHeight;
            if (W < 1000 || W < H * 1.10 || !placedTileRecords || !placedTileRecords.length) return;
            // Each call follows a fresh packCollage: never multiply an already scaled layout.
            const tiles = placedTileRecords.filter(t => t && t.element && t.w > 0 && t.h > 0);
            if (!tiles.length) return;
            const oldLeft = Math.min(...tiles.map(t => t.x));
            const oldTop = Math.min(...tiles.map(t => t.y));
            const oldRight = Math.max(...tiles.map(t => t.x + t.w));
            const oldBottom = Math.max(...tiles.map(t => t.y + t.h));
            const bw = oldRight - oldLeft, bh = oldBottom - oldTop;
            if (!(bw > 0 && bh > 0)) return;
            const headerBottom = header && header.getBoundingClientRect ? header.getBoundingClientRect().bottom : 84;
            const padX = Math.max(18, Math.min(46, W * 0.025));
            const upper = Math.max(108, headerBottom + 12);
            const lower = H - 64;
            const aw = W - 2 * padX, ah = lower - upper;
            if (!(aw > 0 && ah > 0)) return;
            const fit = Math.min(aw / bw, ah / bh);
            // Desktop only, uniform across EVERY animal, no taxon or pose exceptions.
            const zoom = Math.min(2.35, fit * 0.975);
            if (zoom <= 1.025) return;
            const destX = padX + (aw - bw * zoom) / 2;
            const destY = upper + (ah - bh * zoom) / 2;
            for (const t of tiles) {
                const x = destX + (t.x - oldLeft) * zoom;
                const y = destY + (t.y - oldTop) * zoom;
                const w = t.w * zoom, h = t.h * zoom;
                t.element.style.left = x + 'px';
                t.element.style.top = y + 'px';
                t.element.style.width = w + 'px';
                t.element.style.height = h + 'px';
                // Keep pointer/tooltip hit testing in the same geometry as the DOM.
                t.x = x; t.y = y; t.w = w; t.h = h;
            }
        }

function packCollage(speciesList, packingAttempt = 0) {
            stage.innerHTML = '';
            placedTileRecords = [];
            updateHeaderPosition();

            if (!speciesList || speciesList.length === 0) {
                stage.innerHTML = '<div class="empty-banner">No wildlife visitors logged in this window.</div>';
                return;
            }
            const W = window.innerWidth;
            const H = window.innerHeight;
            const isPortrait = H > W;

            const CELL = 4;
            const gridW = Math.ceil(W / CELL);
            const gridH = Math.ceil(H / CELL);
            const occGrid = new Uint8Array(gridW * gridH);
            let packingFailed = false;
            const recoveryScale = Math.pow(0.82, packingAttempt);

            const headerRect = header.getBoundingClientRect();
            const minCollageTopPx = Math.round(headerRect.bottom + 10);
            const topMarginG = Math.max(1, Math.ceil(minCollageTopPx / CELL));
            const bottomMarginG = Math.ceil(55 / CELL);

            const availablePackingHeight = H - minCollageTopPx - 65;
            const centerGX = gridW / 2;
            const centerGY = isPortrait 
                ? Math.round((minCollageTopPx + availablePackingHeight / 2) / CELL)
                : Math.round(gridH / 2 + 4);

            const totalScore = speciesList.reduce((sum, sp) => sum + Math.pow(sp.count, 0.65), 0);
            const countFactor = Math.max(1, speciesList.length);
            const densityScale = isPortrait ? Math.min(1.0, 5.2 / countFactor) : Math.min(1.0, 7.5 / countFactor);

            const budgetRatio = isPortrait 
                ? Math.min(0.70, Math.max(0.38, 0.02 * countFactor + 0.40))
                : Math.min(0.48, Math.max(0.28, 0.03 * countFactor + 0.26));

            const totalCanvasArea = W * (availablePackingHeight > 0 ? availablePackingHeight : H * 0.60) * budgetRatio;

            const minTileW = isPortrait ? Math.max(58, W * 0.16 * Math.max(0.75, densityScale)) : Math.max(55, Math.min(W, H) * 0.12);
            const maxTileW = isPortrait ? Math.min(W * 0.68, 320 * Math.max(0.65, densityScale)) : Math.min(350, Math.min(W, H) * 0.45);

            const alpha = isPortrait ? 0.62 : 1.35;
            speciesList.forEach((sp, idx) => {
                if (packingFailed) return;
                const score = Math.pow(sp.count, 0.65);
                const targetArea = (score / totalScore) * totalCanvasArea;

                const isFlight = shouldShowFlight(sp);
                const activeSlugKey = isFlight ? (sp.flight_mask_key || `${sp.slug}_flight`) : (sp.image_mask_key || sp.slug);
                const activeImageUrl = isFlight ? sp.flight_image_url : sp.image_url;
                /* AVIAN_GEMINI_AREA_RATIO_V1 */
                // Gemini final iteration: relative intended area is count ^ 0.65.
                // All taxa use this same rule; no bird/bat/mammal multiplier.
                const maskData = silhouetteMasks[activeSlugKey] || silhouetteMasks[sp.slug] || silhouetteMasks.__missing__;
                const reportedAspect = (maskData && Number(maskData.aspect)) || 1.0;
                const aspect = Number.isFinite(reportedAspect) && reportedAspect > 0 ? reportedAspect : 1.0;
                // Apply limits to AREA, not WIDTH; tall/narrow illustrations must
                // never get a huge canvas just because their minimum width was 55px.
                const minAreaPx2 = Math.pow(Math.min(minTileW, 40), 2);
                const maxAreaPx2 = Math.pow(maxTileW, 2);
                const boundedAreaPx2 = Math.max(minAreaPx2, Math.min(maxAreaPx2, Math.max(1, targetArea)));
                // Preserve source aspect ratio while making image area match count.
                const idealWidth = Math.sqrt(boundedAreaPx2 * aspect);
                const idealHeight = idealWidth / aspect;
                // Same long-side cap for every animal to prevent extreme vertical
                // or horizontal artwork from dominating the stage.
                const maxSide = Math.min(maxTileW, Math.max(90, availablePackingHeight * 0.48));
                const sideFit = Math.min(1, maxSide / Math.max(idealWidth, idealHeight));
                const widthPx = Math.max(4, idealWidth * sideFit * recoveryScale);
                const heightPx = widthPx / aspect;


                const tileGW = Math.max(1, Math.round(widthPx / CELL));
                const tileGH = Math.max(1, Math.round(heightPx / CELL));

                const itemMask = new Uint8Array(tileGW * tileGH);
                let maskSolidCount = 0;

                if (maskData && maskData.bitmap && maskData.bitmap.length > 0) {
                    const rawW = maskData.w;
                    const rawH = maskData.h;
                    for (let ty = 0; ty < tileGH; ty++) {
                        const srcY = Math.min(rawH - 1, Math.floor((ty / tileGH) * rawH));
                        const rowStr = maskData.bitmap[srcY] || "";
                        for (let tx = 0; tx < tileGW; tx++) {
                            const srcX = Math.min(rawW - 1, Math.floor((tx / tileGW) * rawW));
                            if (rowStr.charCodeAt(srcX) === 49) {
                                itemMask[ty * tileGW + tx] = 1;
                                maskSolidCount++;
                            }
                        }
                    }
                }
                if (maskSolidCount < 3) {
                    for (let ty = 0; ty < tileGH; ty++) {
                        for (let tx = 0; tx < tileGW; tx++) {
                            const nx = (tx / tileGW) - 0.5;
                            const ny = (ty / tileGH) - 0.5;
                            if (nx * nx + ny * ny <= 0.22) {
                                itemMask[ty * tileGW + tx] = 1;
                            }
                        }
                    }
                }

                let foundSpot = false;
                let bestGX = Math.round(centerGX - tileGW / 2);
                let bestGY = Math.round(centerGY - tileGH / 2);

                const maxSteps = 12000;
                let step = 0;
                let angle = idx * 1.618;

                while (step < maxSteps) {
                    const r = 1.6 * Math.pow(step, 0.52);
                    const candidateGX = Math.round(centerGX + (r * Math.cos(angle) * alpha) - (tileGW / 2));
                    const candidateGY = Math.round(centerGY + (r * Math.sin(angle)) - (tileGH / 2));

                    if (
                        candidateGX >= 1 &&
                        candidateGX + tileGW <= gridW - 1 &&
                        candidateGY >= topMarginG &&
                        candidateGY + tileGH <= gridH - bottomMarginG
                    ) {
                        let collides = false;
                        for (let ty = 0; ty < tileGH; ty++) {
                            const gy = candidateGY + ty;
                            const rowOffset = gy * gridW;
                            const maskRowOffset = ty * tileGW;

                            for (let tx = 0; tx < tileGW; tx++) {
                                if (itemMask[maskRowOffset + tx] === 1) {
                                    const gx = candidateGX + tx;
                                    if (occGrid[rowOffset + gx] === 1) {
                                        collides = true;
                                        break;
                                    }
                                }
                            }
                            if (collides) break;
                        }

                        if (!collides) {
                            bestGX = candidateGX;
                            bestGY = candidateGY;
                            foundSpot = true;
                            break;
                        }
                    }

                    angle += 0.08;
                    step++;
                }

                if (!foundSpot) {
                    for (let testY = topMarginG; testY <= gridH - tileGH - bottomMarginG; testY += 2) {
                        for (let testX = 1; testX <= gridW - tileGW - 1; testX += 2) {
                            let collides = false;
                            for (let ty = 0; ty < tileGH; ty++) {
                                const rowOffset = (testY + ty) * gridW;
                                const maskRowOffset = ty * tileGW;
                                for (let tx = 0; tx < tileGW; tx++) {
                                    if (itemMask[maskRowOffset + tx] === 1 && occGrid[rowOffset + testX + tx] === 1) {
                                        collides = true;
                                        break;
                                    }
                                }
                                if (collides) break;
                            }
                            if (!collides) {
                                bestGX = testX;
                                bestGY = testY;
                                foundSpot = true;
                                break;
                            }
                        }
                        if (foundSpot) break;
                    }
                }

                if (!foundSpot) { packingFailed = true; return; }

                for (let ty = 0; ty < tileGH; ty++) {
                    const gy = bestGY + ty;
                    const maskRowOffset = ty * tileGW;
                    for (let tx = 0; tx < tileGW; tx++) {
                        if (itemMask[maskRowOffset + tx] === 1) {
                            const gx = bestGX + tx;
                            for (let dy = -2; dy <= 2; dy++) {
                                const py = gy + dy;
                                if (py >= 0 && py < gridH) {
                                    const rOffset = py * gridW;
                                    for (let dx = -2; dx <= 2; dx++) {
                                        const px = gx + dx;
                                        if (px >= 0 && px < gridW) {
                                            occGrid[rOffset + px] = 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                const posX = bestGX * CELL;
                const posY = bestGY * CELL;

                const el = document.createElement('div');
                el.className = 'avian-tile';
                el.style.width = `${widthPx}px`;
                el.style.height = `${heightPx}px`;
                el.style.left = `${posX}px`;
                el.style.top = `${posY}px`;
                el.style.zIndex = Math.max(1, 50 - idx);

                const img = document.createElement('img');
                img.src = activeImageUrl;
                img.alt = getDisplayName(sp);
                img.onerror = () => { img.onerror = null; img.src = "/api/missing-art.svg?name=" + encodeURIComponent(getDisplayName(sp)); };
                el.appendChild(img);

                stage.appendChild(el);

                placedTileRecords.push({
                    element: el,
                    species: sp,
                    x: posX,
                    y: posY,
                    w: widthPx,
                    h: heightPx,
                    maskData: maskData
                });
            });
            if (packingFailed) {
                if (packingAttempt < 16) packCollage(speciesList, packingAttempt + 1);
                else packFallbackGrid(speciesList, minCollageTopPx);
            }
        
            if (!packingFailed) maximizeDesktopCollage();
        }
        function packFallbackGrid(speciesList, top) {
            // AVIAN_GEMINI_WEIGHTED_FALLBACK_V1
            const W = window.innerWidth;
            const H = window.innerHeight;
            const margin = 8;
            const usableH = Math.max(10, H - top - 65);
            const weights = speciesList.map(sp => Math.pow(Math.max(0, Number(sp.count) || 0), 0.65));
            const total = Math.max(1, weights.reduce((a, b) => a + b, 0));
            const totalArea = W * usableH * 0.40;
            let chosen = null;
            for (let attempt = 0; attempt < 80 && chosen === null; attempt++) {
                const shrink = Math.pow(0.87, attempt);
                const spacing = Math.max(1, 8 * shrink);
                let x = margin, y = top + margin, rowH = 0;
                const tiles = [];
                let fits = true;
                for (let i = 0; i < speciesList.length; i++) {
                    const sp = speciesList[i];
                    const flight = shouldShowFlight(sp);
                    const key = flight ? (sp.flight_mask_key || `${sp.slug}_flight`) : (sp.image_mask_key || sp.slug);
                    const mask = silhouetteMasks[key] || silhouetteMasks[sp.slug] || silhouetteMasks.__missing__;
                    const aspect = (mask && Number(mask.aspect) > 0) ? Number(mask.aspect) : 1;
                    const area = (weights[i] / total) * totalArea;
                    const w = Math.sqrt(Math.max(1, area) * aspect) * shrink;
                    const ht = w / aspect;
                    if (w > W - 2 * margin || ht > usableH - 2 * margin) { fits = false; break; }
                    if (x + w > W - margin) {
                        x = margin;
                        y += rowH + spacing;
                        rowH = 0;
                    }
                    if (y + ht > H - 55 - margin) { fits = false; break; }
                    tiles.push({sp, flight, mask, x, y, w, ht});
                    x += w + spacing;
                    rowH = Math.max(rowH, ht);
                }
                if (fits && tiles.length === speciesList.length) chosen = tiles;
            }
            if (!chosen) {
                console.error('Collage cannot fit all species even after uniform scaling.');
                return;
            }
            stage.innerHTML = '';
            placedTileRecords = [];
            chosen.forEach((item, i) => {
                const el = document.createElement('div');
                el.className = 'avian-tile';
                Object.assign(el.style, {left: item.x+'px', top: item.y+'px', width:item.w+'px', height:item.ht+'px', zIndex:String(Math.max(1,50-i))});
                const img = document.createElement('img');
                img.src = item.flight ? item.sp.flight_image_url : item.sp.image_url;
                img.alt = getDisplayName(item.sp);
                el.appendChild(img);
                stage.appendChild(el);
                placedTileRecords.push({element:el, species:item.sp, x:item.x, y:item.y, w:item.w, h:item.ht, maskData:item.mask});
            });
        
            maximizeDesktopCollage();
        }
        function refreshSpeciesListDisplay() {
            const listUl = document.getElementById('species-list-ul');
            const listSub = document.getElementById('stats-list-subtitle');
            const speciesList = rawSpeciesList.filter(sp => getSpeciesTaxonCategory(sp) === 'mammal' ? chkTaxonMammals.checked : chkTaxonBirds.checked);
            listUl.innerHTML = '';
            const visibleCalls = speciesList.reduce((sum, sp) => sum + sp.count, 0);
            listSub.textContent = `${speciesList.length} Species • ${visibleCalls} Detections`;
            if (!speciesList.length) {
                listUl.innerHTML = '<li style="color:var(--subtext-ink);padding:20px;text-align:center;grid-column:1/-1;">No wildlife visitors in this selection.</li>';
                return;
            }
            speciesList.forEach(sp => {
                const pct = rawTotalCalls > 0 ? Math.round((sp.count / rawTotalCalls) * 100) : 0;

                let badgeHtml = '';
                if (sp.birdnet && !isBat(sp)) {
                    badgeHtml += '<span class="src-badge src-birdnet">BirdNET</span>';
                }
                if (sp.perch) {
                    badgeHtml += '<span class="src-badge src-perch">Perch</span>';
                }
                if (sp.frigate) {
                    badgeHtml += '<span class="src-badge src-frigate">Frigate</span>';
                }
                if (sp.bat_classifier || isBat(sp)) {
                    badgeHtml += '<span class="src-badge src-bat_classifier">Bat Classifier</span>';
                }
                if (!badgeHtml) {
                    badgeHtml = '<span class="src-badge src-birdnet">Audio</span>';
                }

                let dwellHtml = '';
                if (sp.dwell) {
                    dwellHtml = `<span class="dwell-badge" title="${sp.dwell.sessions_count} visit bouts • Total: ${sp.dwell.total_dwell_min} min">⏳ ${sp.dwell.avg_dwell_min}m/visit (${sp.dwell.behavior})</span>`;
                }

                let compHtml = '';
                if (sp.companion) {
                    compHtml = `<span class="companion-badge" title="Arrived together ${sp.companion.count} times in 90s window">🤝 with ${escapeHTML(sp.companion.companion)}</span>`;
                }

                let heatmapCells = '';
                const maxHourVisits = Math.max(...(sp.hourly_activity || [1]), 1);
                (sp.hourly_activity || []).forEach((cnt, hr) => {
                    let lvl = '';
                    if (cnt > 0) {
                        const r = cnt / maxHourVisits;
                        lvl = r > 0.7 ? 'hm-max' : r > 0.4 ? 'hm-3' : r > 0.15 ? 'hm-2' : 'hm-1';
                    }
                    heatmapCells += `<div class="hm-cell ${lvl}" title="${hr}:00 - ${cnt} visits"></div>`;
                });

                const li = document.createElement('li');
                li.className = 'species-item';
                li.innerHTML = `
                    <div class="sp-info">
                        <img class="sp-thumb" src="${escapeHTML(sp.image_url)}" alt="${escapeHTML(getDisplayName(sp))}">
                        <div style="flex:1; min-width:0;">
                            <div class="sp-name-row">
                                <span class="sp-name">${getTargetIcon(sp.target_type, sp)}${escapeHTML(getDisplayName(sp))}</span>
                            </div>
                            <div class="sp-sci">${escapeHTML(sp.scientific_name)}</div>
                            <div class="source-badges">
                                ${badgeHtml}
                                ${dwellHtml}
                                ${compHtml}
                            </div>
                            <div class="sp-heatmap-row" title="24-Hour Activity Profile (00:00 to 23:00)">${heatmapCells}</div>
                        </div>
                    </div>
                    <div class="sp-badge">
                        <span>${sp.count}</span>
                        <span class="sp-pct">${pct}%</span>
                    </div>
                `;
                listUl.appendChild(li);
            });

        }
        function renderStatsView(speciesList, totalCalls, timelineSeries, statsDateStr, topFlocks) {
            document.getElementById('stats-period-label').textContent = statsDateStr;
            rawSpeciesList = speciesList || [];
            rawTotalCalls = totalCalls || 0;
            refreshSpeciesListDisplay();
            renderFlocks(topFlocks || []);
            renderTimelineLegend(rawSpeciesList);
            drawTimelineChart(timelineSeries || []);
        }

        function renderFlocks(flocks) {
            const grid = document.getElementById('flock-grid');
            grid.innerHTML = '';
            if (!flocks || flocks.length === 0) {
                grid.innerHTML = '<div style="color:var(--subtext-ink); font-size:12px; grid-column:1/-1; padding:6px 0;">No strong multi-species co-occurrences detected in this window.</div>';
                return;
            }
            flocks.slice(0, 6).forEach(f => {
                const card = document.createElement('div');
                card.className = 'flock-card';
                card.innerHTML = `
                    <div class="flock-pair">🐾 ${escapeHTML(f.species_a)} + ${escapeHTML(f.species_b)}</div>
                    <div class="flock-count">${f.co_occurrences} joint arrivals</div>
                `;
                grid.appendChild(card);
            });
        }

        function getSpeciesColor(speciesName, speciesList) {
            const idx = speciesList.findIndex(s => s.common_name === speciesName);
            if (idx === -1) return '#6d6054';
            return EDO_PALETTE[idx % EDO_PALETTE.length];
        }

        function renderTimelineLegend(speciesList) {
            timelineLegend.innerHTML = '';
            if (!speciesList || speciesList.length === 0) return;

            const totalPill = document.createElement('div');
            totalPill.className = `legend-pill ${mutedSeries.has('__total__') ? 'muted' : ''}`;
            totalPill.innerHTML = `<span class="legend-dot" style="background:${TOTAL_COLOR};"></span> <span>Total (Filtered Sources)</span>`;
            totalPill.addEventListener('click', () => {
                if (mutedSeries.has('__total__')) mutedSeries.delete('__total__');
                else mutedSeries.add('__total__');
                chkTotal.checked = !mutedSeries.has('__total__');
                renderTimelineLegend(speciesList);
                drawTimelineChart(currentTimelineData);
            });
            timelineLegend.appendChild(totalPill);

            speciesList.forEach((sp, idx) => {
                const color = EDO_PALETTE[idx % EDO_PALETTE.length];
                const pill = document.createElement('div');
                pill.className = `legend-pill ${mutedSeries.has(sp.common_name) ? 'muted' : ''}`;
                pill.innerHTML = `<span class="legend-dot" style="background:${color};"></span> <span>${escapeHTML(getDisplayName(sp))}</span>`;
                pill.addEventListener('click', () => {
                    if (mutedSeries.has(sp.common_name)) mutedSeries.delete(sp.common_name);
                    else mutedSeries.add(sp.common_name);
                    renderTimelineLegend(speciesList);
                    drawTimelineChart(currentTimelineData);
                });
                timelineLegend.appendChild(pill);
            });
        }

        function getSpeciesFilteredCount(bucket, spName) {
            const spData = (bucket.species_sources && bucket.species_sources[spName]) ? bucket.species_sources[spName] : null;
            if (!spData) return 0;

            let c = 0;
            if (chkBirdnet.checked) c += (spData.birdnet || 0);
            if (chkPerch.checked) c += (spData.perch || 0);
            if (chkFrigate.checked) c += (spData.frigate || 0);
            if (chkBat.checked) c += (spData.bat_classifier || 0);
            return c;
        }

        function getBucketFilteredTotal(bucket) {
            let tot = 0;
            for (const spName of Object.keys(bucket.species_sources || {})) {
                tot += getSpeciesFilteredCount(bucket, spName);
            }
            return tot;
        }

        function drawTimelineChart(timelineSeries) {
            const ctx = timelineCanvas.getContext('2d');
            const dpr = window.devicePixelRatio || 1;
            const rect = timelineCanvas.getBoundingClientRect();

            timelineCanvas.width = rect.width * dpr;
            timelineCanvas.height = rect.height * dpr;
            ctx.scale(dpr, dpr);

            const W = rect.width;
            const H = rect.height;
            ctx.clearRect(0, 0, W, H);

            if (!timelineSeries || timelineSeries.length === 0) {
                ctx.fillStyle = "#8c7e72";
                ctx.font = "14px Inter";
                ctx.textAlign = "center";
                ctx.fillText("No timeline activity recorded", W / 2, H / 2);
                return;
            }

            const paddingLeft = 35;
            const paddingRight = 25;
            const paddingTop = 20;
            const paddingBottom = 35;

            const chartW = W - paddingLeft - paddingRight;
            const chartH = H - paddingTop - paddingBottom;

            let maxVal = 1;
            timelineSeries.forEach(d => {
                const bTot = getBucketFilteredTotal(d);
                if (!mutedSeries.has('__total__') && bTot > maxVal) maxVal = bTot;
                for (const sp of currentSpeciesData) {
                    const c = getSpeciesFilteredCount(d, sp.common_name);
                    if (!mutedSeries.has(sp.common_name) && c > maxVal) maxVal = c;
                }
            });
            maxVal = Math.max(maxVal, 4);

            ctx.strokeStyle = "rgba(44, 37, 35, 0.08)";
            ctx.lineWidth = 1;
            ctx.fillStyle = "#8c7e72";
            ctx.font = "10px Inter";
            ctx.textAlign = "right";
            ctx.textBaseline = "middle";

            for (let i = 0; i <= 4; i++) {
                const y = paddingTop + chartH - (i / 4) * chartH;
                const v = Math.round((i / 4) * maxVal);
                ctx.beginPath();
                ctx.moveTo(paddingLeft, y);
                ctx.lineTo(paddingLeft + chartW, y);
                ctx.stroke();
                ctx.fillText(v, paddingLeft - 8, y);
            }

            const xStep = chartW / Math.max(1, timelineSeries.length - 1);

            currentSpeciesData.forEach(sp => {
                if (mutedSeries.has(sp.common_name)) return;
                const spColor = getSpeciesColor(sp.common_name, currentSpeciesData);
                const points = timelineSeries.map((d, i) => {
                    const cnt = getSpeciesFilteredCount(d, sp.common_name);
                    const x = paddingLeft + i * xStep;
                    const y = paddingTop + chartH - (cnt / maxVal) * chartH;
                    return { x, y, count: cnt };
                });

                ctx.beginPath();
                points.forEach((pt, i) => {
                    if (i === 0) ctx.moveTo(pt.x, pt.y);
                    else {
                        const prev = points[i - 1];
                        const cx = (prev.x + pt.x) / 2;
                        ctx.bezierCurveTo(cx, prev.y, cx, pt.y, pt.x, pt.y);
                    }
                });
                ctx.strokeStyle = spColor;
                ctx.lineWidth = 1.8;
                ctx.setLineDash([3, 2]);
                ctx.stroke();
                ctx.setLineDash([]);

                points.forEach(pt => {
                    if (pt.count > 0) {
                        ctx.fillStyle = spColor;
                        ctx.beginPath();
                        ctx.arc(pt.x, pt.y, 2.5, 0, Math.PI * 2);
                        ctx.fill();
                    }
                });
            });

            if (!mutedSeries.has('__total__')) {
                const totalPoints = timelineSeries.map((d, i) => {
                    const bTot = getBucketFilteredTotal(d);
                    const x = paddingLeft + i * xStep;
                    const y = paddingTop + chartH - (bTot / maxVal) * chartH;
                    return { x, y, count: bTot };
                });

                ctx.beginPath();
                ctx.moveTo(totalPoints[0].x, paddingTop + chartH);
                totalPoints.forEach((pt, i) => {
                    if (i === 0) ctx.lineTo(pt.x, pt.y);
                    else {
                        const prev = totalPoints[i - 1];
                        const cx = (prev.x + pt.x) / 2;
                        ctx.bezierCurveTo(cx, prev.y, cx, pt.y, pt.x, pt.y);
                    }
                });
                ctx.lineTo(totalPoints[totalPoints.length - 1].x, paddingTop + chartH);
                ctx.closePath();

                const areaGrad = ctx.createLinearGradient(0, paddingTop, 0, paddingTop + chartH);
                areaGrad.addColorStop(0, "rgba(44, 37, 35, 0.22)");
                areaGrad.addColorStop(1, "rgba(44, 37, 35, 0.01)");
                ctx.fillStyle = areaGrad;
                ctx.fill();

                ctx.beginPath();
                totalPoints.forEach((pt, i) => {
                    if (i === 0) ctx.moveTo(pt.x, pt.y);
                    else {
                        const prev = totalPoints[i - 1];
                        const cx = (prev.x + pt.x) / 2;
                        ctx.bezierCurveTo(cx, prev.y, cx, pt.y, pt.x, pt.y);
                    }
                });
                ctx.strokeStyle = TOTAL_COLOR;
                ctx.lineWidth = 2.8;
                ctx.stroke();

                totalPoints.forEach(pt => {
                    if (pt.count > 0) {
                        ctx.fillStyle = "#f59e0b";
                        ctx.beginPath();
                        ctx.arc(pt.x, pt.y, 3.5, 0, Math.PI * 2);
                        ctx.fill();
                        ctx.strokeStyle = TOTAL_COLOR;
                        ctx.lineWidth = 1.5;
                        ctx.stroke();
                    }
                });
            }

            const stepLabel = Math.max(1, Math.ceil(timelineSeries.length / 8));
            timelineSeries.forEach((d, i) => {
                if (i % stepLabel === 0 || i === timelineSeries.length - 1) {
                    const x = paddingLeft + i * xStep;
                    ctx.fillStyle = "#6d6054";
                    ctx.font = "10px Inter";
                    ctx.textAlign = "center";
                    ctx.textBaseline = "top";
                    ctx.fillText(d.label, x, paddingTop + chartH + 10);
                }
            });

            if (activeHoverIndex !== null && activeHoverIndex >= 0 && activeHoverIndex < timelineSeries.length) {
                const hoverX = paddingLeft + activeHoverIndex * xStep;
                const d = timelineSeries[activeHoverIndex];

                ctx.beginPath();
                ctx.strokeStyle = "rgba(44, 37, 35, 0.45)";
                ctx.lineWidth = 1.2;
                ctx.setLineDash([4, 3]);
                ctx.moveTo(hoverX, paddingTop);
                ctx.lineTo(hoverX, paddingTop + chartH);
                ctx.stroke();
                ctx.setLineDash([]);

                if (!mutedSeries.has('__total__')) {
                    const bTot = getBucketFilteredTotal(d);
                    const totalY = paddingTop + chartH - (bTot / maxVal) * chartH;
                    ctx.beginPath();
                    ctx.arc(hoverX, totalY, 5.5, 0, Math.PI * 2);
                    ctx.fillStyle = "#f59e0b";
                    ctx.fill();
                    ctx.strokeStyle = TOTAL_COLOR;
                    ctx.lineWidth = 2;
                    ctx.stroke();
                }

                currentSpeciesData.forEach(sp => {
                    const c = getSpeciesFilteredCount(d, sp.common_name);
                    if (!mutedSeries.has(sp.common_name) && c > 0) {
                        const spColor = getSpeciesColor(sp.common_name, currentSpeciesData);
                        const spY = paddingTop + chartH - (c / maxVal) * chartH;
                        ctx.beginPath();
                        ctx.arc(hoverX, spY, 4.5, 0, Math.PI * 2);
                        ctx.fillStyle = spColor;
                        ctx.fill();
                        ctx.strokeStyle = "#ffffff";
                        ctx.lineWidth = 1.5;
                        ctx.stroke();
                    }
                });
            }
        }

        function handleTimelineMove(evt) {
            if (!currentTimelineData || currentTimelineData.length === 0) return;
            const rect = timelineCanvas.getBoundingClientRect();
            const clientX = evt.touches ? evt.touches[0].clientX : evt.clientX;
            const clientY = evt.touches ? evt.touches[0].clientY : evt.clientY;

            const paddingLeft = 35;
            const paddingRight = 25;
            const chartW = rect.width - paddingLeft - paddingRight;
            const mouseX = clientX - rect.left;

            if (mouseX < paddingLeft - 10 || mouseX > rect.width - paddingRight + 10) {
                hideTimelineTooltip();
                return;
            }

            const xRel = Math.max(0, Math.min(chartW, mouseX - paddingLeft));
            const idx = Math.round((xRel / chartW) * (currentTimelineData.length - 1));

            if (idx >= 0 && idx < currentTimelineData.length) {
                activeHoverIndex = idx;
                drawTimelineChart(currentTimelineData);
                showTimelineTooltip(clientX, clientY, currentTimelineData[idx]);
            }
        }
        function showTimelineTooltip(clientX, clientY, bucket) {
            const wrapperRect = timelineWrapper.getBoundingClientRect();
            let x = clientX - wrapperRect.left + 16;
            let y = clientY - wrapperRect.top - 20;

            if (x + 200 > wrapperRect.width) x = x - 220;
            if (y < 10) y = 10;

            timelineTooltip.style.left = `${x}px`;
            timelineTooltip.style.top = `${y}px`;
            timelineTooltip.style.display = 'block';

            let speciesRows = '';
            let activeItems = [];

            for (const sp of currentSpeciesData) {
                const c = getSpeciesFilteredCount(bucket, sp.common_name);
                if (c > 0) {
                    activeItems.push({ name: sp.common_name, count: c });
                }
            }
            activeItems.sort((a,b) => b.count - a.count);

            if (activeItems.length > 0) {
                activeItems.forEach(item => {
                    const color = getSpeciesColor(item.name, currentSpeciesData);
                    speciesRows += `
                        <div class="tt-row">
                            <span class="tt-sp-name">
                                <span class="tt-sp-dot" style="background:${color};"></span>
                                ${escapeHTML(item.name)}
                            </span>
                            <span class="tt-sp-val">${item.count}</span>
                        </div>
                    `;
                });
            } else {
                speciesRows = `<div style="color:#a89a8b; font-size:11px; margin-top:2px;">No detections for selected sources.</div>`;
            }

            const bTot = getBucketFilteredTotal(bucket);

            timelineTooltip.innerHTML = `
                <div class="tt-time">🕒 ${bucket.label}</div>
                <div class="tt-total">Total: ${bTot}</div>
                <div>${speciesRows}</div>
            `;
        }

        function hideTimelineTooltip() {
            activeHoverIndex = null;
            timelineTooltip.style.display = 'none';
            drawTimelineChart(currentTimelineData);
        }

        timelineWrapper.addEventListener('mousemove', handleTimelineMove);
        timelineWrapper.addEventListener('mouseleave', hideTimelineTooltip);
        timelineWrapper.addEventListener('touchmove', handleTimelineMove, { passive: true });
        timelineWrapper.addEventListener('touchend', hideTimelineTooltip);

        async function fetchAndUpdate(forceRedraw = false) {
            const sequence = ++fetchSequence;
            const errorBanner = document.getElementById('connection-error');
            try {
                const activeTf = currentView === 'collage' ? collageTimeframe : statsTimeframe;
                const res = await fetch(`/api/species?timeframe=${encodeURIComponent(activeTf)}&offset=${currentTimeOffset}&ts=${Date.now()}`);
                const data = await res.json();
                if (sequence !== fetchSequence) return;
                if (!res.ok) throw new Error(data.error || `Server returned ${res.status}`);
                if (!Array.isArray(data.species)) throw new Error('Invalid species response');
                errorBanner.style.display = 'none';
                document.getElementById('period-label').textContent = data.date_str;
                document.getElementById('stats-chip').textContent = `${data.species.length} Species • ${data.total_calls} Detections`;
                currentSpeciesData = data.species;
                currentTimelineData = data.timeline_series || [];
                currentFlocksData = data.top_flocks || [];
                if (currentView === 'collage') {
                    await loadMasks();
                    if (sequence !== fetchSequence) return;
                    packCollage(data.species);
                } else {
                    renderStatsView(data.species, data.total_calls, currentTimelineData, data.stats_date_str, currentFlocksData);
                }
            } catch (err) {
                if (sequence !== fetchSequence) return;
                errorBanner.textContent = 'Data unavailable: ' + err.message + '. Check bridge configuration and InfluxDB. Any previous display is not current.';
                errorBanner.style.display = 'block';
                document.getElementById('stats-chip').textContent = 'Data unavailable';
            }
        }

        window.addEventListener('resize', () => {
            if (currentView === 'collage') {
                if (currentSpeciesData.length > 0) packCollage(currentSpeciesData);
            } else {
                drawTimelineChart(currentTimelineData);
            }
        });

        renderTimeButtons();
        if (new URLSearchParams(window.location.search).get('view') === 'statistics') {
            toggleViewBtn.click();
        }
        loadMasks().then(() => {
            fetchAndUpdate(true);
            setInterval(() => fetchAndUpdate(false), 20000);
        });
    </script>

    <!-- AVIAN_GITHUB_LINK_V1 -->
    <a id="avian-github-link" href="https://github.com/ranolph" target="_blank"
       rel="noopener noreferrer" aria-label="ranolph on GitHub (opens in a new tab)"
       title="ranolph on GitHub">
        <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false">
            <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82a7.65 7.65 0 0 1 4 0c1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.01 8.01 0 0 0 16 8c0-4.42-3.58-8-8-8Z"/>
        </svg>
        <span>GitHub</span>
    </a>
</body>
</html>
"""

# --- HTTP service: original routes, with explicit errors and safe artwork paths. ---
class AvianHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class AvianWebHandler(gf_settings.GardenSettingsMixin, BaseHTTPRequestHandler):
    server_version = "GardenFriends"

    def log_message(self, fmt, *args):
        LOG.debug("HTTP %s", fmt % args)

    def send_content(self, content: bytes, content_type: str, status=200, cache="no-store"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", cache)
        self.send_header("X-Content-Type-Options", "nosniff")
        # Preserved read-only API compatibility with the original dashboard.
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(content)

    def send_json(self, value, status=200):
        self.send_content(json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8"),
                          "application/json; charset=utf-8", status)

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        try:
            self.handle_get()
        except (BrokenPipeError, ConnectionResetError):
            return
        except Exception as exc:
            LOG.warning("HTTP request failed: %s", safe_error(exc))
            try:
                self.send_json({"error": safe_error(exc)}, 503)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def handle_get(self):
        if self.handle_garden_request():
            return
        parsed = urllib.parse.urlsplit(self.path)
        path = urllib.parse.unquote(parsed.path)
        params = urllib.parse.parse_qs(parsed.query)
        if path == "/":
            self.send_content(HTML_PAGE.encode("utf-8"), "text/html; charset=utf-8")
            return
        if path in {"/api/health", "/healthz"}:
            connected = bool(mqtt_client and mqtt_client.is_connected())
            self.send_json({
                "status": "running", "version": APP_VERSION, "read_only": READ_ONLY,
                "settings_revision": gf_settings.active_revision(),
                "day_boundary": "00:00", "timezone": TIMEZONE,
                "influx_configured": bool(INFLUX_TOKEN), "mqtt_connected": connected,
                "queue_depth": message_queue.qsize(), "last_query_error": last_query_error,
                "note": "Process status, not proof that upstream detection or database writes are healthy.",
            })
            return
        if path == "/api/species":
            timeframe = params.get("timeframe", ["today"])[0]
            try:
                offset = int(params.get("offset", ["0"])[0])
                get_observation_window(timeframe, offset)
            except (ValueError, OverflowError):
                self.send_json({"error": "Invalid timeframe or offset (expected 0..10000)."}, 400)
                return
            species, label, stats_label, total, timeline, flocks = get_active_species(timeframe, offset)
            self.send_json({"species": species, "date_str": label, "stats_date_str": stats_label,
                            "total_calls": total, "timeline_series": timeline, "top_flocks": flocks})
            return
        if path == "/api/masks":
            self.send_json(artwork_index()[1])
            return
        if path == "/api/missing-art.svg":
            name = html.escape(params.get("name", ["Artwork missing"])[0][:64])
            # This is a labelled placeholder, not reconstructed or generated artwork.
            svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="480" height="300" viewBox="0 0 480 300">
<rect x="3" y="3" width="474" height="294" rx="24" fill="#f4eedb" stroke="#c6b894" stroke-width="4"/>
<text x="240" y="110" text-anchor="middle" fill="#7d7065" font-family="sans-serif" font-size="23">Artwork not restored</text>
<foreignObject x="24" y="140" width="432" height="140"><div xmlns="http://www.w3.org/1999/xhtml" style="font:700 25px sans-serif;text-align:center;color:#2c2523;overflow-wrap:anywhere">{name}</div></foreignObject></svg>'''
            self.send_content(svg.encode("utf-8"), "image/svg+xml; charset=utf-8", cache="public, max-age=300")
            return
        if path in {"/favicon.ico", "/favicon.png"}:
            name = "favicon.png"
        elif path.startswith("/art/"):
            name = path[5:]
        else:
            self.send_json({"error": "Not found"}, 404)
            return
        # Only single image filenames under ART_DIR are served. Never .env, Python,
        # symlinks, '..', subdirectories or the snapshot metadata sidecar.
        if (not name or name in {".", ".."} or "/" in name or "\\" in name
                or Path(name).suffix.lower() not in {".png", ".jpg", ".jpeg", ".webp"}):
            self.send_json({"error": "Not found"}, 404)
            return
        target = (SNAPSHOT_DIR if name == "latest_visual.jpg" else Path(ART_DIR)) / name
        if target.is_symlink() or not target.is_file():
            self.send_json({"error": "Artwork not found"}, 404)
            return
        if name == "latest_visual.jpg":
            requested_event = params.get("event", [""])[0]
            with state_lock:
                if requested_event and requested_event != latest_snapshot_event:
                    self.send_json({"error": "Snapshot not available for this event yet"}, 404)
                    return
                data = target.read_bytes()
            cache = "no-store"
        else:
            data, cache = target.read_bytes(), "public, max-age=60"
        self.send_content(data, mimetypes.guess_type(name)[0] or "application/octet-stream", cache=cache)


def clear_query_cache():
    global query_generation
    with query_lock:
        query_generation += 1
        query_cache.clear()


def write_to_influx(source, target_type, common_name, scientific_name, confidence,
                    model, device, event_id=None):
    """Keep the original measurement, tag set, field and ingestion timestamps.

    No bucket/organisation creation, record deletion or historical backfill occurs.
    Event IDs stay in the bridge's deduplication state, not additional Influx tags.
    """
    if READ_ONLY:
        return False
    norm_c, norm_s = normalize_species(common_name, scientific_name)
    conf = valid_confidence(confidence)
    if not norm_c or conf is None:
        return False
    if write_api is None or Point is None:
        raise RuntimeError("InfluxDB write client is not configured")
    target_type = get_target_type(norm_c, norm_s)
    when = datetime.datetime.now(UTC)
    point = (Point("wildlife_detection")
             .tag("source", str(source)).tag("target_type", target_type)
             .tag("common_name", norm_c).tag("scientific_name", norm_s or "")
             .tag("model", str(model)).tag("device", str(device))
             .field("confidence", conf).time(when, WritePrecision.NS))
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
    clear_query_cache()
    LOG.info("[INFLUXDB] Logged: [%s] (%s) %s [%.3f]", source, target_type, norm_c, conf)
    # A secondary artwork/MQTT problem must not turn a successful DB write into
    # a retry and a duplicate point. DB exceptions above still reach the caller.
    try:
        publish_ha_detection(source, target_type, norm_c, norm_s, conf, model, device,
                             when.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S"), event_id)
    except Exception as exc:
        LOG.warning("Detection saved, but HA publication failed: %s", safe_error(exc))
    summary_needed.set()
    return True


def prune_event_state():
    now = time.monotonic()
    with state_lock:
        for event_id, state in list(pending_frigate_scores.items()):
            if now - state.get("updated", now) > 1800:
                pending_frigate_scores.pop(event_id, None)
        for event_id, stamp in list(logged_event_times.items()):
            if now - stamp > 86400:
                logged_event_times.pop(event_id, None)
                logged_events.discard(event_id)
        if len(logged_event_times) > 50000:
            for event_id in sorted(logged_event_times, key=logged_event_times.get)[:10000]:
                logged_event_times.pop(event_id, None)
                logged_events.discard(event_id)
        if len(pending_frigate_scores) > 10000:
            for event_id in sorted(pending_frigate_scores, key=lambda k: pending_frigate_scores[k].get("updated", 0))[:2000]:
                pending_frigate_scores.pop(event_id, None)


def process_frigate_visual(event_id, description_text, score, camera):
    if READ_ONLY or not event_id:
        return False
    norm_c, norm_s = parse_species_description(description_text)
    if not norm_c:
        return False
    conf = valid_confidence(score)
    target_type = get_target_type(norm_c, norm_s)
    threshold = FRIGATE_MAMMAL_MIN_SCORE if target_type == "mammal" else FRIGATE_MIN_SCORE
    if conf is None or conf < threshold:
        return False
    event_id = str(event_id)
    with state_lock:
        if event_id in logged_events or event_id in inflight_events:
            return False
        inflight_events.add(event_id)
    try:
        saved = write_to_influx("visual_frigate", target_type, norm_c, norm_s, conf,
                                "frigate_gemini", camera or "garden_camera", event_id)
        if saved:
            with state_lock:
                logged_events.add(event_id)
                logged_event_times[event_id] = time.monotonic()
                pending_frigate_scores.pop(event_id, None)
        return saved
    except Exception as exc:
        LOG.warning("[FRIGATE WRITE FAILED] Event %s remains eligible for retry: %s", event_id, safe_error(exc))
        return False
    finally:
        with state_lock:
            inflight_events.discard(event_id)


def detector_score(event: dict) -> float | None:
    """Frigate object-detection score only, not a Gemini/sub-label score."""
    scores = []
    for obj in (event, event.get("data"), event.get("snapshot")):
        if not isinstance(obj, dict):
            continue
        for key in ("top_score", "score"):
            score = valid_confidence(obj.get(key))
            if score is not None:
                scores.append(score)
    return max(scores) if scores else None


def event_description(event: dict) -> str:
    for obj in (event, event.get("data"), event.get("snapshot")):
        if not isinstance(obj, dict):
            continue
        desc = obj.get("description")
        if isinstance(desc, str) and desc.strip():
            return desc
        label = obj.get("sub_label")
        if isinstance(label, (list, tuple)):
            label = label[0] if label else ""
        if isinstance(label, str) and label.strip():
            return label
    return ""


def cache_frigate_event(event_id, event: dict, description="", camera="") -> dict:
    event_id = str(event_id)
    score = detector_score(event)
    with state_lock:
        state = pending_frigate_scores.setdefault(event_id, {})
        state["updated"] = time.monotonic()
        if score is not None:
            state["score"] = max(score, state.get("score", 0.0))
        desc = description or event_description(event)
        if desc:
            state["description"] = desc
        cam = event.get("camera") or camera
        if isinstance(cam, str) and cam:
            state["camera"] = cam
        if event.get("false_positive") is True:
            state["false_positive"] = True
        elif event.get("false_positive") is False:
            state["false_positive"] = False
        return dict(state)


def try_cached_frigate(event_id: str) -> bool:
    with state_lock:
        if event_id in logged_events:
            return True
        state = dict(pending_frigate_scores.get(event_id, {}))
    if state.get("false_positive"):
        return False
    return process_frigate_visual(event_id, state.get("description", ""),
                                  state.get("score"), state.get("camera", "garden_camera"))


# Bound the fallback network work instead of launching an unlimited thread per
# event during an outage. A successful later MQTT update can still complete it.
frigate_fetch_slots = threading.BoundedSemaphore(4)


def fetch_frigate_score_and_process(event_id, desc="", camera=""):
    if READ_ONLY or not event_id:
        return
    event_id = str(event_id)
    cache_frigate_event(event_id, {}, desc, camera)
    with state_lock:
        if event_id in logged_events or event_id in fetching_frigate:
            return
        if not frigate_fetch_slots.acquire(blocking=False):
            LOG.warning("Frigate fallback queue is busy; event %s remains pending", event_id)
            return
        fetching_frigate.add(event_id)

    def worker():
        try:
            encoded = urllib.parse.quote(event_id, safe="")
            for delay in (0, 2, 5):
                if stop_event.wait(delay):
                    return
                if try_cached_frigate(event_id):
                    return
                for base in FRIGATE_URLS:
                    with state_lock:
                        if event_id in logged_events:
                            return
                    try:
                        with urllib.request.urlopen(frigate_request(base, f"/api/events/{encoded}"), timeout=3) as response:
                            raw = response.read(2 * 1024 * 1024 + 1)
                        if len(raw) > 2 * 1024 * 1024:
                            continue
                        event = json.loads(raw.decode("utf-8"))
                        if not isinstance(event, dict):
                            continue
                        cache_frigate_event(event_id, event, camera=camera)
                        if try_cached_frigate(event_id):
                            return
                    except Exception:
                        continue
            LOG.info("[FRIGATE PENDING] %s: no acceptable species/score yet; awaiting later updates", event_id)
        finally:
            with state_lock:
                fetching_frigate.discard(event_id)
            frigate_fetch_slots.release()

    threading.Thread(target=worker, name="frigate-event", daemon=True).start()


def process_birdnet_payload(payload: dict):
    common = first_present(payload, "species", "CommonName", "cname", "comName", "common_name", "Species", "name")
    scientific = first_present(payload, "scientific_name", "ScientificName", "sname", "sciName") or ""
    # Some publishers use a species object. Flat names above are the PDF's schema.
    if isinstance(common, dict):
        species = common
        common = first_present(species, "common_name", "commonName", "CommonName", "name")
        scientific = first_present(species, "scientific_name", "scientificName", "ScientificName") or scientific
    norm_c, norm_s = normalize_species(common, scientific)
    if not norm_c:
        return False
    value = first_present(payload, "confidence", "Confidence", "conf", "score")
    if value is None:
        value = BIRDNET_MISSING_CONFIDENCE
    confidence = valid_confidence(value)
    if confidence is None:
        LOG.info("[AUDIO IGNORED] Missing/invalid 0..1 confidence for %s", norm_c)
        return False
    model_id = str(first_present(payload, "model_id", "best_model_id", "Model", "model") or "").casefold()
    target_type = get_target_type(norm_c, norm_s)
    if target_type == "bat" or "bat_classifier" in model_id:
        source, model = "audio_bat_classifier", "Bat Classifier"
    elif "perch" in model_id:
        source, model = "audio_perch", "Google-Perch"
    else:
        source, model = "audio_birdnet", "BirdNET-Go"
    device = first_present(payload, "device", "source_node", "station", "Device") or "clippy_mic"
    return write_to_influx(source, target_type, norm_c, norm_s, confidence, model, str(device))


def handle_message(topic: str, raw_payload):
    """Process an MQTT message outside the MQTT networking thread."""
    if READ_ONLY:
        return
    if topic == HA_DISCOVERY_PREFIX + "/status":
        if raw_payload in (b"online", "online"):
            publish_ha_discovery()
            summary_needed.set()
        return
    if "soundlevel" in topic.lower() or "stats" in topic.lower() or topic.startswith("wildlife/"):
        return
    try:
        if isinstance(raw_payload, dict):
            payload = raw_payload
        else:
            if len(raw_payload) > 2 * 1024 * 1024:
                return
            payload = json.loads(raw_payload)
        if not isinstance(payload, dict):
            return
        if topic == FRIGATE_TOPIC_PREFIX or topic.startswith(FRIGATE_TOPIC_PREFIX + "/"):
            suffix = topic[len(FRIGATE_TOPIC_PREFIX):].lstrip("/")
            after = payload.get("after")
            event = after if isinstance(after, dict) else payload
            prune_event_state()
            if suffix == "reviews":
                # Review IDs are NOT event IDs. A review groups detection IDs.
                data = event.get("data") if isinstance(event.get("data"), dict) else {}
                ids = data.get("detections", [])
                if not isinstance(ids, list):
                    return
                for event_id in ids:
                    if isinstance(event_id, str):
                        fetch_frigate_score_and_process(event_id, camera=event.get("camera", ""))
                return
            if suffix not in {"events", "tracked_object_update"}:
                return
            event_id = event.get("id") or payload.get("id")
            if not isinstance(event_id, (str, int)) or not str(event_id):
                return
            event_id = str(event_id)
            if suffix == "tracked_object_update" and payload.get("type") not in {None, "description", "sub_label"}:
                return
            state = cache_frigate_event(event_id, event, camera=payload.get("camera", ""))
            if try_cached_frigate(event_id):
                return
            # Negative AI descriptions and explicit false positives do not need
            # more reads. Description-less object events simply populate cache.
            if state.get("description") and not state.get("false_positive"):
                common, _ = parse_species_description(state["description"])
                if common:
                    fetch_frigate_score_and_process(event_id)
        elif "birdnet" in topic.casefold() or any(topic_matches(pattern, topic) for pattern in BIRDNET_TOPICS):
            process_birdnet_payload(payload)
    except (ValueError, TypeError, UnicodeError):
        LOG.debug("Ignored malformed MQTT message on %s", topic)
    except Exception as exc:
        LOG.warning("[MESSAGE ERROR] %s: %s", topic, safe_error(exc))


def topic_matches(pattern: str, topic: str) -> bool:
    # Minimal MQTT wildcard matcher also makes configured audio topics testable
    # without importing Paho or connecting a real broker.
    parts, actual = pattern.split("/"), topic.split("/")
    for i, part in enumerate(parts):
        if part == "#":
            return i == len(parts)-1
        if i >= len(actual) or (part != "+" and part != actual[i]):
            return False
    return len(parts) == len(actual)


def on_message(client, userdata, msg):
    try:
        message_queue.put_nowait((str(msg.topic), bytes(msg.payload)))
    except queue.Full:
        LOG.error("MQTT input queue full: a message was dropped. Inspect database availability/load.")


def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code != 0:
        LOG.error("MQTT connection rejected: %s", reason_code)
        return
    LOG.info("Garden Friends connected to MQTT.")
    topics = [(FRIGATE_TOPIC_PREFIX + "/" + suffix, 1)
              for suffix in ("events", "tracked_object_update", "reviews")]
    topics += [(topic, 1) for topic in BIRDNET_TOPICS]
    if HA_ENABLED:
        topics.append((HA_DISCOVERY_PREFIX + "/status", 1))
    client.subscribe(topics)
    publish_ha_discovery()
    summary_needed.set()


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    if not stop_event.is_set():
        LOG.warning("MQTT disconnected (%s); automatic reconnect enabled", reason_code)


def message_worker():
    while not stop_event.is_set():
        try:
            topic, payload = message_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        try:
            handle_message(topic, payload)
        finally:
            message_queue.task_done()


def initialize_services():
    global influx_client, influx_query_api, write_api, mqtt_client, Point, WritePrecision, mqtt
    try:
        from influxdb_client import InfluxDBClient, Point as InfluxPoint, WritePrecision as Precision
        from influxdb_client.client.write_api import SYNCHRONOUS
        from PIL import Image  # noqa: F401
        if not READ_ONLY:
            import paho.mqtt.client as mqtt_module
    except ImportError as exc:
        raise RuntimeError("Install requirements.txt in the Python environment running main.py") from exc
    Point, WritePrecision = InfluxPoint, Precision
    if INFLUX_TOKEN:
        influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG,
                                      timeout=INFLUX_TIMEOUT_MS)
        influx_query_api = influx_client.query_api()
        if not READ_ONLY:
            write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    else:
        LOG.warning("INFLUX_TOKEN is empty. Website will report a configuration error, not invented detections.")
    if not READ_ONLY:
        mqtt = mqtt_module
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
        if MQTT_USERNAME:
            mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        if MQTT_TLS:
            mqtt_client.tls_set(ca_certs=MQTT_CA_FILE or None)
        mqtt_client.on_connect = on_connect
        mqtt_client.on_disconnect = on_disconnect
        mqtt_client.on_message = on_message
        mqtt_client.reconnect_delay_set(min_delay=1, max_delay=60)
        mqtt_client.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_start()
        threading.Thread(target=message_worker, name="wildlife-messages", daemon=True).start()
        threading.Thread(target=update_ha_summary_loop, name="wildlife-summary", daemon=True).start()


def check_configuration():
    """Local validation only: no connections, data writes or secret values."""
    if not 1 <= WEB_PORT <= 65535 or not 1 <= MQTT_PORT <= 65535:
        raise ValueError("Ports must be between 1 and 65535")
    if not (0 <= FRIGATE_MIN_SCORE <= 1 and 0 <= FRIGATE_MAMMAL_MIN_SCORE <= 1):
        raise ValueError("Frigate thresholds must be between 0 and 1")
    for url in [INFLUX_URL, *FRIGATE_URLS, *([PUBLIC_BASE_URL] if PUBLIC_BASE_URL else [])]:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Service URLs must be absolute http:// or https:// URLs")
        if parsed.username or parsed.password:
            raise ValueError("Do not embed credentials in service URLs; use the token variables")
    if BIRDNET_MISSING_CONFIDENCE and valid_confidence(BIRDNET_MISSING_CONFIDENCE) is None:
        raise ValueError("BIRDNET_MISSING_CONFIDENCE must be blank or a number between 0 and 1")
    installed = {}
    for package in ("paho-mqtt", "influxdb-client", "Pillow", "tzdata"):
        try:
            installed[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            installed[package] = "NOT INSTALLED"
    return {"version": APP_VERSION, "web_port": WEB_PORT, "read_only": READ_ONLY,
            "influx_token_present": bool(INFLUX_TOKEN), "art_directory": ART_DIR,
            "art_directory_exists": Path(ART_DIR).is_dir(), "packages": installed,
            "note": "Local configuration only; no connectivity or live pipeline test was performed."}


def main(argv=None):
    global READ_ONLY
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true", help="Validate local configuration and show dependency versions; do not connect")
    parser.add_argument("--read-only", action="store_true", help="Serve existing Influx records only; no MQTT connection or DB writes")
    parser.add_argument("--rebuild-masks", action="store_true", help="Rebuild masks.json from existing PNGs, then exit (writes only artwork metadata)")
    args = parser.parse_args(argv)
    if args.read_only:
        READ_ONLY = True
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(),
                        format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    config = check_configuration()
    if args.check:
        print(json.dumps(config, indent=2))
        return 0
    if args.rebuild_masks:
        print(f"Rebuilt masks for {rebuild_masks()} PNG files in {ART_DIR}")
        return 0
    gf_settings.initialize_admin()
    server = AvianHTTPServer((WEB_BIND, WEB_PORT), AvianWebHandler)
    try:
        try:
            initialize_services()
        except Exception as exc:
            # Keep the settings page available to repair a failed connection.
            LOG.error("Connection startup failed: %s. Open Settings to correct the connection.", safe_error(exc))
        LOG.info("[COLLAGE ENGINE] Running on port %d; read_only=%s", WEB_PORT, READ_ONLY)
        LOG.info("Artwork directory: %s; set PUBLIC_BASE_URL for Home Assistant image URLs", ART_DIR)

        def shutdown(signum, frame):
            stop_event.set()
            summary_needed.set()
            threading.Thread(target=server.shutdown, daemon=True).start()

        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, shutdown)
        server.serve_forever(poll_interval=0.5)
    finally:
        stop_event.set()
        summary_needed.set()
        server.server_close()
        if mqtt_client:
            mqtt_client.disconnect()
            mqtt_client.loop_stop()
        if write_api:
            write_api.close()
        if influx_client:
            influx_client.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, ValueError, OSError) as exc:
        print(f"Startup failed: {safe_error(exc)}", file=sys.stderr)
        sys.exit(1)
