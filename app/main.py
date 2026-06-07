import asyncio
import json
import re
import unicodedata
from collections import deque

import yaml
from asyncio_mqtt import Client, MqttError
from curl_cffi.requests import AsyncSession

SOFASCORE_HOME = "https://www.sofascore.com/"
LIVE_URL = "https://api.sofascore.com/api/v1/sport/football/events/live"

PAGE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}

API_HEADERS = {
    "User-Agent": PAGE_HEADERS["User-Agent"],
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": SOFASCORE_HOME,
    "Origin": "https://www.sofascore.com",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}

with open("config.yaml", "r") as f:
    CONFIG = yaml.safe_load(f)

MQTT_HOST = CONFIG["mqtt"]["host"]
MQTT_PORT = CONFIG["mqtt"]["port"]
MQTT_USER = CONFIG["mqtt"].get("username")
MQTT_PASS = CONFIG["mqtt"].get("password")

LED_TOPIC = CONFIG.get("led", {}).get("topic", "all")
TEAMS = CONFIG.get("teams", [])
POLL_INTERVAL = CONFIG.get("poll_interval", 15)

LIVE_PREVIEW = CONFIG.get("live_preview", {})
LIVE_PREVIEW_ENABLED = LIVE_PREVIEW.get("enabled", True)
LIVE_PREVIEW_TOPIC = LIVE_PREVIEW.get("topic", "sports/football/live")
LIVE_PREVIEW_LIMIT = int(LIVE_PREVIEW.get("limit", 5))
LIVE_PREVIEW_LED = LIVE_PREVIEW.get("led", True)
FILTER_DISPLAY_EVENTS = LIVE_PREVIEW.get("filter_display_events", True)

COUNTRY_SECONDS = float(LIVE_PREVIEW.get("country_seconds", LIVE_PREVIEW.get("context_seconds", 3)))
LEAGUE_SECONDS = float(LIVE_PREVIEW.get("league_seconds", LIVE_PREVIEW.get("context_seconds", 3)))
DISPLAY_SECONDS = float(LIVE_PREVIEW.get("display_seconds", 5))
BLINK_COUNT = int(LIVE_PREVIEW.get("blink_count", 3))
BLINK_INTERVAL = float(LIVE_PREVIEW.get("blink_interval", 0.35))
BLANK_BETWEEN_SECONDS = float(LIVE_PREVIEW.get("blank_between_seconds", 0.25))
MAX_DISPLAY_CHARS = int(LIVE_PREVIEW.get("max_display_chars", 120))
SHOW_STATUS_ON_LED = LIVE_PREVIEW.get("show_status_on_led", False)
NO_LIVE_TEXT = LIVE_PREVIEW.get("no_live_text", "BRAK MECZOW LIVE")
DISPLAY_WIDTH_CHARS = int(LIVE_PREVIEW.get("display_width_chars", 16))
STATIC_HOLD_SECONDS = float(LIVE_PREVIEW.get("static_hold_seconds", 3))
SCROLL_CHARS_PER_SECOND = float(LIVE_PREVIEW.get("scroll_chars_per_second", 5))
SCROLL_END_PAUSE_SECONDS = float(LIVE_PREVIEW.get("scroll_end_pause_seconds", 1))
USE_FULL_TEAM_NAMES = LIVE_PREVIEW.get("use_full_team_names", False)
LEAGUE_MAX_CHARS = int(LIVE_PREVIEW.get("league_max_chars", DISPLAY_WIDTH_CHARS))
GOAL_TEXT_SECONDS = float(LIVE_PREVIEW.get("goal_text_seconds", 0.8))
GOAL_TEAM_SECONDS = float(LIVE_PREVIEW.get("goal_team_seconds", 2.0))

last_scores = {}
last_all_live_scores = {}
known_incidents = set()
known_periods = set()
initialized_events = set()
current_live_events = []
priority_messages = deque()
priority_signal = asyncio.Event()
sofascore_session_ready = False
live_scores_initialized = False

SHORT_NAMES = {
    "Juventus": "JUV",
    "Torino": "TOR",
    "Lech Poznań": "LEC",
    "Lech Poznan": "LEC",
    "Legia Warszawa": "LEG",
    "AC Milan": "MIL",
    "Cagliari": "CAG",
    "Ecuador": "ECU",
    "Guatemala": "GUA",
}

COUNTRY_NAMES_PL = {
    "Argentina": "ARGENTYNA",
    "Austria": "AUSTRIA",
    "Belgium": "BELGIA",
    "Bolivia": "BOLIWIA",
    "Brazil": "BRAZYLIA",
    "Chile": "CHILE",
    "Colombia": "KOLUMBIA",
    "Croatia": "CHORWACJA",
    "Czech Republic": "CZECHY",
    "Denmark": "DANIA",
    "Ecuador": "EKWADOR",
    "England": "ANGLIA",
    "France": "FRANCJA",
    "Germany": "NIEMCY",
    "Greece": "GRECJA",
    "Guatemala": "GWATEMALA",
    "Italy": "WLOCHY",
    "Mexico": "MEKSYK",
    "Netherlands": "HOLANDIA",
    "Norway": "NORWEGIA",
    "Paraguay": "PARAGWAJ",
    "Peru": "PERU",
    "Poland": "POLSKA",
    "Portugal": "PORTUGALIA",
    "Scotland": "SZKOCJA",
    "Spain": "HISZPANIA",
    "Sweden": "SZWECJA",
    "Switzerland": "SZWAJCARIA",
    "Turkey": "TURCJA",
    "Ukraine": "UKRAINA",
    "USA": "USA",
    "World": "SWIAT",
}

LEAGUE_SHORT_NAMES = {
    "Int. Friendly Games": "TOWARZYSKI",
    "International Friendly Games": "TOWARZYSKI",
    "UEFA Nations League": "NATIONS",
    "World Championship": "MUNDIAL",
    "World Cup": "MUNDIAL",
    "European Championship": "EURO",
    "Copa América": "COPA",
    "Copa America": "COPA",
    "Ekstraklasa": "EKSTRAKLASA",
    "Premier League": "PREMIER",
    "LaLiga": "LALIGA",
    "LaLiga EA Sports": "LALIGA",
    "Serie A": "SERIE A",
    "Bundesliga": "BUNDESLIGA",
    "Ligue 1": "LIGUE 1",
    "Eredivisie": "EREDIVISIE",
    "Primeira Liga": "LIGA POR",
    "Süper Lig": "SUPER LIG",
    "Super Lig": "SUPER LIG",
    "Pro League": "PRO LEAGUE",
    "Liga Profesional de Fútbol": "LIGA ARG",
    "Liga Profesional": "LIGA ARG",
    "Brasileirão Série A": "SERIE A BR",
    "Brasileirao Serie A": "SERIE A BR",
    "Serie A, Primera Etapa": "SERIE A ECU",
    "Liga MX, Apertura": "LIGA MX",
    "Liga MX, Clausura": "LIGA MX",
}

TEAM_PREFIX_STOPWORDS = {
    "AC", "AFC", "CA", "CD", "CF", "CS", "FC", "FK", "IF", "NK", "SC", "SK", "TS",
    "CLUB", "ATLETICO", "ATLÉTICO", "ATHLETIC", "DE", "DEL", "DA", "DO", "DOS", "THE",
}

LEAGUE_WORDS_TO_REMOVE = {
    "NACIONAL", "NATIONAL", "PROFESIONAL", "PROFESSIONAL", "CHAMPIONSHIP", "TOURNAMENT",
    "LEAGUE", "LIGA", "DE", "DEL", "DA", "DO", "DOS", "THE", "FOOTBALL", "FUTBOL", "FÚTBOL",
}

TOP_LEAGUES_BY_COUNTRY = {
    "Argentina": {"Liga Profesional", "Liga Profesional de Fútbol", "Primera División"},
    "Austria": {"Bundesliga"},
    "Belgium": {"Pro League", "First Division A"},
    "Brazil": {"Brasileirão Série A", "Brasileirao Serie A", "Serie A"},
    "Chile": {"Primera División", "Primera Division"},
    "Colombia": {"Primera A, Apertura", "Primera A, Clausura", "Primera A"},
    "Croatia": {"HNL"},
    "Czech Republic": {"1. Liga", "First League"},
    "Denmark": {"Superliga"},
    "Ecuador": {"LigaPro Serie A", "Serie A, Primera Etapa", "Serie A, Segunda Etapa", "Serie A"},
    "England": {"Premier League"},
    "France": {"Ligue 1"},
    "Germany": {"Bundesliga"},
    "Greece": {"Super League"},
    "Italy": {"Serie A"},
    "Mexico": {"Liga MX, Apertura", "Liga MX, Clausura", "Liga MX"},
    "Netherlands": {"Eredivisie"},
    "Norway": {"Eliteserien"},
    "Poland": {"Ekstraklasa"},
    "Portugal": {"Primeira Liga"},
    "Scotland": {"Premiership"},
    "Spain": {"LaLiga", "LaLiga EA Sports", "Primera División"},
    "Sweden": {"Allsvenskan"},
    "Switzerland": {"Super League"},
    "Turkey": {"Süper Lig", "Super Lig"},
    "Ukraine": {"Premier League"},
    "USA": {"MLS"},
}

INTERNATIONAL_KEYWORDS = (
    "friendly", "nations league", "world cup", "world championship", "european championship",
    "euro", "copa america", "qualification", "qualifiers", "africa cup", "asian cup",
    "concacaf", "uefa", "fifa",
)

EXCLUDED_INTERNATIONAL_KEYWORDS = (
    "club friendly", "club world", "u19", "u20", "u21", "u23", "women", "womens", "reserve",
)


def strip_accents(text):
    text = str(text or "")
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def norm_key(text):
    text = strip_accents(text).lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def normalize_display(text):
    text = " ".join(str(text or "").split())
    if MAX_DISPLAY_CHARS > 0 and len(text) > MAX_DISPLAY_CHARS:
        return text[:MAX_DISPLAY_CHARS].rstrip()
    return text


def fit_to_display(text):
    text = normalize_display(text)
    width = max(1, DISPLAY_WIDTH_CHARS)
    if len(text) > width:
        return text[:width].rstrip()
    return text


def clean_token(token):
    token = strip_accents(token)
    token = re.sub(r"[^A-Za-z0-9]", "", token)
    return token.upper()


def short_name(name):
    if not name:
        return "???"

    if name in SHORT_NAMES:
        return SHORT_NAMES[name]

    cleaned = str(name)
    cleaned = re.sub(r"\bClub\s+Atl[eé]tico\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bAtl[eé]tico\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bClub\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.replace("&", " ").replace("-", " ")

    tokens = [clean_token(t) for t in cleaned.split()]
    tokens = [t for t in tokens if t and t not in TEAM_PREFIX_STOPWORDS]

    if not tokens:
        tokens = [clean_token(t) for t in str(name).split() if clean_token(t)]

    if not tokens:
        return "???"

    token = tokens[0]
    if len(token) >= 3:
        return token[:3]

    if len(tokens) > 1:
        joined = "".join(tokens)
        return joined[:3].ljust(3, "?")

    return token[:3].ljust(3, "?")


def compact_league_name(name):
    name = normalize_display(name or "Liga")

    if name in LEAGUE_SHORT_NAMES:
        return LEAGUE_SHORT_NAMES[name]

    limit = max(6, LEAGUE_MAX_CHARS)
    if len(name) <= limit:
        return name

    ascii_name = strip_accents(name)
    words = re.split(r"\s+", ascii_name.replace(".", ""))
    useful = []

    for word in words:
        token = clean_token(word)
        if not token or token in LEAGUE_WORDS_TO_REMOVE:
            continue
        useful.append(word.upper()[:8])

    compact = " ".join(useful[:2]).strip()
    if not compact:
        compact = ascii_name.upper()

    if len(compact) > limit:
        compact = compact[:limit].rstrip()

    return compact or name[:limit]


def get_raw_tournament_info(ev):
    tournament = ev.get("tournament", {}) or {}
    category = tournament.get("category", {}) or {}
    unique_tournament = tournament.get("uniqueTournament", {}) or {}

    league_raw = tournament.get("name") or unique_tournament.get("name") or "Liga"
    unique_league_raw = unique_tournament.get("name") or league_raw
    country_raw = category.get("name") or category.get("country", {}).get("name") or ""

    return country_raw, league_raw, unique_league_raw


def get_tournament_info(ev):
    country_raw, league_raw, _ = get_raw_tournament_info(ev)
    country = COUNTRY_NAMES_PL.get(country_raw, country_raw.upper() if country_raw else "")
    league = compact_league_name(league_raw)
    return normalize_display(country or "INNE"), normalize_display(league or "Liga")


def is_international_event(ev):
    country_raw, league_raw, unique_league_raw = get_raw_tournament_info(ev)
    text = norm_key(f"{country_raw} {league_raw} {unique_league_raw}")

    if any(bad in text for bad in EXCLUDED_INTERNATIONAL_KEYWORDS):
        return False

    if norm_key(country_raw) == "world" and any(word in text for word in INTERNATIONAL_KEYWORDS):
        return True

    return any(word in text for word in INTERNATIONAL_KEYWORDS)


def is_top_league_event(ev):
    country_raw, league_raw, unique_league_raw = get_raw_tournament_info(ev)
    country_rules = TOP_LEAGUES_BY_COUNTRY.get(country_raw)
    if not country_rules:
        return False

    league_keys = {norm_key(league_raw), norm_key(unique_league_raw)}
    allowed_keys = {norm_key(name) for name in country_rules}
    return bool(league_keys & allowed_keys)


def should_display_event(ev):
    return is_international_event(ev) or is_top_league_event(ev)


def filter_display_events(events):
    if not FILTER_DISPLAY_EVENTS:
        return list(events)

    filtered = [ev for ev in events if should_display_event(ev)]
    print(f"[FILTER] display events: {len(filtered)}/{len(events)}", flush=True)
    return filtered


def calculate_display_seconds(text, minimum_seconds):
    text_len = len(str(text))
    width = max(1, DISPLAY_WIDTH_CHARS)

    if text_len <= width:
        return minimum_seconds

    extra_chars = text_len - width
    scroll_seconds = extra_chars / max(0.5, SCROLL_CHARS_PER_SECOND)
    return max(minimum_seconds, STATIC_HOLD_SECONDS + scroll_seconds + SCROLL_END_PAUSE_SECONDS)


def get_score(ev):
    hs = ev.get("homeScore", {}).get("current", 0)
    aw = ev.get("awayScore", {}).get("current", 0)
    return f"{hs}:{aw}"


def parse_score(score):
    try:
        left, right = str(score).split(":", 1)
        return int(left), int(right)
    except Exception:
        return None, None


def guess_scoring_team(ev, old_score):
    old_home, old_away = parse_score(old_score)
    new_home, new_away = parse_score(get_score(ev))
    home = ev.get("homeTeam", {}).get("name", "")
    away = ev.get("awayTeam", {}).get("name", "")

    if old_home is not None and new_home is not None and new_home > old_home:
        return home
    if old_away is not None and new_away is not None and new_away > old_away:
        return away
    return ""


def get_status_text(ev):
    status = ev.get("status", {}) or {}
    status_desc = status.get("description") or status.get("type", "")
    if status_desc:
        return status_desc
    return "live"


def get_team_name(name):
    if USE_FULL_TEAM_NAMES:
        return name or "???"
    return short_name(name)


def format_score_event(ev):
    home = ev.get("homeTeam", {}).get("name", "HOME")
    away = ev.get("awayTeam", {}).get("name", "AWAY")
    base = f"{get_team_name(home)} {get_score(ev)} {get_team_name(away)}"

    if SHOW_STATUS_ON_LED:
        base = f"{base} ({get_status_text(ev)})"
    return normalize_display(base)


def format_event(ev, include_status=True):
    base = format_score_event(ev)
    if include_status and not SHOW_STATUS_ON_LED:
        return f"{base} ({get_status_text(ev)})"
    return base


def get_display_parts(ev):
    country, league = get_tournament_info(ev)
    score = format_score_event(ev)
    return country, league, score


def format_full_display_event(ev):
    country, league, score = get_display_parts(ev)
    return normalize_display(f"{country} | {league} | {score}")


def enqueue_priority_message(text):
    text = normalize_display(text)
    if not text:
        return

    priority_messages.append({"type": "text", "text": text})
    priority_signal.set()
    print(f"[DISPLAY PRIORITY QUEUED] {text}", flush=True)


def enqueue_priority_event(ev, prefix="GOAL", goal_team=None):
    country, league, score = get_display_parts(ev)

    if prefix == "GOAL":
        team_text = fit_to_display(goal_team or guess_scoring_team(ev, last_all_live_scores.get(str(ev.get("id")), "")) or "GOAL")
        priority_messages.append({"type": "goal", "team": team_text, "score": score})
        priority_signal.set()
        print(f"[DISPLAY PRIORITY QUEUED] GOAL -> {team_text} -> {score}", flush=True)
        return

    priority_score = normalize_display(f"{prefix} {score}")
    priority_messages.append({"type": "sequence", "country": country, "league": league, "text": priority_score})
    priority_signal.set()
    print(f"[DISPLAY PRIORITY QUEUED] {country} -> {league} -> {priority_score}", flush=True)


async def publish_json(client, topic, payload, retain=False):
    await client.publish(topic, json.dumps(payload, ensure_ascii=False), qos=0, retain=retain)
    print("[MQTT JSON]", topic, payload, flush=True)


async def publish_led(client, text, retain=False):
    await client.publish(LED_TOPIC, text, qos=1, retain=retain)
    print("[MQTT LED]", LED_TOPIC, repr(text), flush=True)


async def clear_led(client):
    await publish_led(client, " ", retain=True)
    await asyncio.sleep(BLANK_BETWEEN_SECONDS)


async def prepare_sofascore_session(session, force=False):
    global sofascore_session_ready

    if sofascore_session_ready and not force:
        return

    try:
        resp = await session.get(SOFASCORE_HOME, headers=PAGE_HEADERS, timeout=15)
        body = resp.text or ""
        print(f"[HTTP INIT] SofaScore homepage status {resp.status_code}", flush=True)
        if resp.status_code != 200:
            print(f"[HTTP INIT BODY] {body[:300]}", flush=True)
        sofascore_session_ready = True
    except Exception as e:
        print("[HTTP INIT ERROR]", e, flush=True)
        sofascore_session_ready = False


async def fetch_json(session, url, retry=True):
    global sofascore_session_ready

    try:
        await prepare_sofascore_session(session)
        resp = await session.get(url, headers=API_HEADERS, timeout=15)

        if resp.status_code != 200:
            body = resp.text or ""
            print(f"[HTTP ERROR] {resp.status_code} {url}", flush=True)
            print(f"[HTTP BODY] {body[:500]}", flush=True)

            if resp.status_code == 403 and retry:
                print("[HTTP RETRY] refreshing SofaScore session and retrying once", flush=True)
                sofascore_session_ready = False
                await prepare_sofascore_session(session, force=True)
                await asyncio.sleep(1)
                return await fetch_json(session, url, retry=False)

            return {}

        return resp.json()
    except Exception as e:
        print("[FETCH ERROR]", e, flush=True)
        return {}


async def fetch_live_events(session):
    data = await fetch_json(session, LIVE_URL)
    events = data.get("events", [])
    print(f"[DEBUG] live events: {len(events)}", flush=True)
    return events


async def publish_live_preview(client, raw_events, display_events):
    if not LIVE_PREVIEW_ENABLED:
        return

    preview_events = display_events[:LIVE_PREVIEW_LIMIT]
    items = []

    for ev in preview_events:
        home = ev.get("homeTeam", {}).get("name", "HOME")
        away = ev.get("awayTeam", {}).get("name", "AWAY")
        country, league, score_text = get_display_parts(ev)
        item = {
            "id": ev.get("id"),
            "country": country,
            "league": league,
            "home": home,
            "away": away,
            "home_short": short_name(home),
            "away_short": short_name(away),
            "score": get_score(ev),
            "status": get_status_text(ev),
            "line": format_event(ev, include_status=True),
            "display_country": country,
            "display_country_seconds": round(calculate_display_seconds(country, COUNTRY_SECONDS), 2),
            "display_league": league,
            "display_league_seconds": round(calculate_display_seconds(league, LEAGUE_SECONDS), 2),
            "display_score": score_text,
            "display_score_seconds": round(calculate_display_seconds(score_text, DISPLAY_SECONDS), 2),
            "display_sequence": [country, league, score_text],
            "display": format_full_display_event(ev),
        }
        items.append(item)

    payload = {
        "type": "live_preview",
        "count": len(raw_events),
        "filtered_count": len(display_events),
        "shown": len(items),
        "filter": "internationals_and_top_leagues_only",
        "display_mode": "country_then_short_league_then_short_score",
        "display_width_chars": DISPLAY_WIDTH_CHARS,
        "league_max_chars": LEAGUE_MAX_CHARS,
        "team_mode": "short_3_letters",
        "goal_priority_mode": "GOAL, team, GOAL x3, score",
        "matches": items,
    }

    await publish_json(client, LIVE_PREVIEW_TOPIC, payload, retain=True)

    if not display_events:
        print("[LIVE PREVIEW] no selected live matches after filter", flush=True)
        return

    text = " | ".join(item["display"] for item in items)
    print(f"[LIVE PREVIEW] {text}", flush=True)


async def detect_live_score_changes(events):
    global live_scores_initialized

    seen_keys = set()

    for ev in events:
        event_id = ev.get("id")
        if not event_id:
            continue

        key = str(event_id)
        seen_keys.add(key)
        score = get_score(ev)
        old_score = last_all_live_scores.get(key)

        if live_scores_initialized and old_score is not None and old_score != score:
            goal_team = guess_scoring_team(ev, old_score)
            enqueue_priority_event(ev, prefix="GOAL", goal_team=goal_team)
            print(f"[SCORE CHANGE] {key}: {old_score} -> {score}", flush=True)

        last_all_live_scores[key] = score

    for key in list(last_all_live_scores.keys()):
        if key not in seen_keys:
            last_all_live_scores.pop(key, None)

    if not live_scores_initialized:
        live_scores_initialized = True
        print(f"[SCORE INIT] initialized {len(last_all_live_scores)} live scores", flush=True)


async def show_text_for_seconds(client, text, seconds, interruptible=True):
    await publish_led(client, text, retain=True)

    if interruptible:
        try:
            await asyncio.wait_for(priority_signal.wait(), timeout=seconds)
            priority_signal.clear()
            await clear_led(client)
            return False
        except asyncio.TimeoutError:
            return True

    await asyncio.sleep(seconds)
    return True


async def show_text_dynamic(client, text, minimum_seconds, interruptible=True):
    text = normalize_display(text)
    seconds = calculate_display_seconds(text, minimum_seconds)
    print(f"[DISPLAY WAIT] {seconds:.2f}s for {repr(text)}", flush=True)
    return await show_text_for_seconds(client, text, seconds, interruptible=interruptible)


async def show_match_sequence(client, country, league, score, interruptible=True):
    sequence = [(country, COUNTRY_SECONDS), (league, LEAGUE_SECONDS), (score, DISPLAY_SECONDS)]

    for text, seconds in sequence:
        if not text:
            continue

        completed = await show_text_dynamic(client, text, seconds, interruptible=interruptible)
        if not completed:
            return False

        await clear_led(client)

    return True


async def show_goal_priority(client, message):
    team = fit_to_display(message.get("team") or "GOAL")
    score = fit_to_display(message.get("score") or "")

    print(f"[DISPLAY PRIORITY GOAL] GOAL -> {team} -> GOAL x{BLINK_COUNT} -> {score}", flush=True)

    await show_text_for_seconds(client, "GOAL", GOAL_TEXT_SECONDS, interruptible=False)
    await clear_led(client)

    if team and team != "GOAL":
        await show_text_for_seconds(client, team, GOAL_TEAM_SECONDS, interruptible=False)
        await clear_led(client)

    for _ in range(BLINK_COUNT):
        await show_text_for_seconds(client, "GOAL", GOAL_TEXT_SECONDS, interruptible=False)
        await clear_led(client)

    if score:
        await show_text_for_seconds(client, score, DISPLAY_SECONDS, interruptible=False)
        await clear_led(client)


async def show_priority_text(client, message):
    if isinstance(message, dict):
        msg_type = message.get("type")
        if msg_type == "goal":
            await show_goal_priority(client, message)
            return
        if msg_type == "sequence":
            country = message.get("country")
            league = message.get("league")
            text = message.get("text")
        else:
            country = None
            league = None
            text = message.get("text")
    elif isinstance(message, tuple):
        country, league, text = message
    else:
        country, league, text = None, None, message

    print(f"[DISPLAY PRIORITY] {country or ''} {league or ''} {text}", flush=True)

    if country:
        await show_text_dynamic(client, country, COUNTRY_SECONDS, interruptible=False)
        await clear_led(client)

    if league:
        await show_text_dynamic(client, league, LEAGUE_SECONDS, interruptible=False)
        await clear_led(client)

    for _ in range(BLINK_COUNT):
        await publish_led(client, fit_to_display(text), retain=True)
        await asyncio.sleep(BLINK_INTERVAL)
        await publish_led(client, " ", retain=True)
        await asyncio.sleep(BLINK_INTERVAL)

    await show_text_for_seconds(client, fit_to_display(text), DISPLAY_SECONDS, interruptible=False)
    await clear_led(client)


async def display_live_rotation(client):
    index = 0
    last_no_live_sent = False

    while True:
        if not LIVE_PREVIEW_LED:
            await asyncio.sleep(1)
            continue

        if priority_messages:
            message = priority_messages.popleft()
            priority_signal.clear()
            await show_priority_text(client, message)
            continue

        events = list(current_live_events)

        if not events:
            if not last_no_live_sent:
                await publish_led(client, normalize_display(NO_LIVE_TEXT), retain=True)
                last_no_live_sent = True
            await asyncio.sleep(5)
            continue

        last_no_live_sent = False

        if index >= len(events):
            index = 0

        ev = events[index]
        country, league, score_text = get_display_parts(ev)
        country_wait = calculate_display_seconds(country, COUNTRY_SECONDS)
        league_wait = calculate_display_seconds(league, LEAGUE_SECONDS)
        score_wait = calculate_display_seconds(score_text, DISPLAY_SECONDS)
        print(
            f"[DISPLAY ROTATE] {index + 1}/{len(events)} "
            f"{country} ({country_wait:.2f}s) -> "
            f"{league} ({league_wait:.2f}s) -> "
            f"{score_text} ({score_wait:.2f}s)",
            flush=True,
        )
        index += 1

        await show_match_sequence(client, country, league, score_text, interruptible=True)


async def preload_incidents(session, event_id):
    url = f"https://api.sofascore.com/api/v1/event/{event_id}/incidents"
    data = await fetch_json(session, url)

    count = 0

    for inc in data.get("incidents", []):
        inc_id = inc.get("id")
        if not inc_id:
            continue

        key = f"{event_id}:{inc_id}"
        known_incidents.add(key)
        count += 1

    print(f"[INIT] preloaded {count} old incidents for event {event_id}", flush=True)


async def handle_incidents(session, client, team, event_id, score):
    url = f"https://api.sofascore.com/api/v1/event/{event_id}/incidents"
    data = await fetch_json(session, url)

    for inc in data.get("incidents", []):
        inc_id = inc.get("id")
        inc_type = inc.get("incidentType")

        if not inc_id:
            continue

        key = f"{event_id}:{inc_id}"

        if key in known_incidents:
            continue

        known_incidents.add(key)

        if inc_type == "goal":
            minute = inc.get("time", "")
            player = inc.get("player", {}).get("name", "")
            goal_team = inc.get("team", {}).get("name", "")

            payload = {"type": "goal", "team": goal_team, "player": player, "minute": minute, "score": score}
            await publish_json(client, f"{team['mqtt_prefix']}/goal", payload)
            matching_event = next((ev for ev in current_live_events if str(ev.get("id")) == str(event_id)), None)
            if matching_event:
                enqueue_priority_event(matching_event, prefix="GOAL", goal_team=goal_team)
            else:
                enqueue_priority_message(f"GOAL {short_name(goal_team)} {score}")

        elif inc_type == "card":
            color = inc.get("color", "yellow")
            minute = inc.get("time", "")
            player = inc.get("player", {}).get("name", "")
            card_team = inc.get("team", {}).get("name", "")

            payload = {"type": "card", "team": card_team, "player": player, "minute": minute, "color": color}
            await publish_json(client, f"{team['mqtt_prefix']}/card", payload)

            if color == "red":
                enqueue_priority_message(f"RED {short_name(card_team)} {minute}'")


async def process_team(session, client, team, events):
    found = False

    for ev in events:
        home_team = ev.get("homeTeam", {})
        away_team = ev.get("awayTeam", {})

        home_id = home_team.get("id")
        away_id = away_team.get("id")

        if team["id"] not in [home_id, away_id]:
            continue

        found = True
        event_id = ev.get("id")
        if not event_id:
            continue

        event_key = str(event_id)
        home = home_team.get("name", "HOME")
        away = away_team.get("name", "AWAY")
        score = get_score(ev)
        status = ev.get("status", {})
        status_type = status.get("type", "")
        status_desc = status.get("description", "")

        if event_key not in initialized_events:
            initialized_events.add(event_key)
            last_scores[event_key] = score
            payload = {"type": "score", "home": home, "away": away, "score": score, "status": status_desc}
            await publish_json(client, f"{team['mqtt_prefix']}/live", payload)
            await preload_incidents(session, event_id)
            print(f"[INIT] {home} {score} {away}", flush=True)
            continue

        old_score = last_scores.get(event_key)

        if old_score != score:
            goal_team = guess_scoring_team(ev, old_score)
            last_scores[event_key] = score
            payload = {"type": "score", "home": home, "away": away, "score": score, "status": status_desc}
            await publish_json(client, f"{team['mqtt_prefix']}/live", payload)
            enqueue_priority_event(ev, prefix="GOAL", goal_team=goal_team)

        period_key = f"{event_id}:{status_type}:{status_desc}"

        if period_key not in known_periods:
            known_periods.add(period_key)

            if "Halftime" in status_desc:
                enqueue_priority_event(ev, prefix="HT")
            elif "Ended" in status_desc or status_type == "finished":
                enqueue_priority_event(ev, prefix="FT")

        await handle_incidents(session, client, team, event_id, score)

    if not found:
        print(f"[INFO] no live match for {team['name']}", flush=True)


async def poll_loop(session, client):
    global current_live_events

    while True:
        raw_events = await fetch_live_events(session)
        display_events = filter_display_events(raw_events)
        current_live_events = display_events

        await publish_live_preview(client, raw_events, display_events)
        await detect_live_score_changes(display_events)

        for team in TEAMS:
            await process_team(session, client, team, raw_events)

        await asyncio.sleep(POLL_INTERVAL)


async def main():
    print("Starting SofaScore MQTT Bridge...", flush=True)

    while True:
        try:
            async with Client(hostname=MQTT_HOST, port=MQTT_PORT, username=MQTT_USER, password=MQTT_PASS) as client:
                print(f"Connected to MQTT {MQTT_HOST}:{MQTT_PORT}", flush=True)

                async with AsyncSession(impersonate="chrome124", timeout=15) as session:
                    display_task = asyncio.create_task(display_live_rotation(client))
                    try:
                        await poll_loop(session, client)
                    finally:
                        display_task.cancel()
                        try:
                            await display_task
                        except asyncio.CancelledError:
                            pass

        except MqttError as e:
            print("[MQTT ERROR]", e, flush=True)
            await asyncio.sleep(5)

        except Exception as e:
            print("[MAIN ERROR]", e, flush=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
