import asyncio
import json
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
MQTT_USER = CONFIG["mqtt"]["username"]
MQTT_PASS = CONFIG["mqtt"]["password"]

LED_TOPIC = CONFIG.get("led", {}).get("topic", "all")
TEAMS = CONFIG.get("teams", [])
POLL_INTERVAL = CONFIG.get("poll_interval", 15)

LIVE_PREVIEW = CONFIG.get("live_preview", {})
LIVE_PREVIEW_ENABLED = LIVE_PREVIEW.get("enabled", True)
LIVE_PREVIEW_TOPIC = LIVE_PREVIEW.get("topic", "sports/football/live")
LIVE_PREVIEW_LIMIT = int(LIVE_PREVIEW.get("limit", 5))
LIVE_PREVIEW_LED = LIVE_PREVIEW.get("led", True)
DISPLAY_SECONDS = float(LIVE_PREVIEW.get("display_seconds", 5))
CONTEXT_SECONDS = float(LIVE_PREVIEW.get("context_seconds", 3))
BLINK_COUNT = int(LIVE_PREVIEW.get("blink_count", 3))
BLINK_INTERVAL = float(LIVE_PREVIEW.get("blink_interval", 0.35))
BLANK_BETWEEN_SECONDS = float(LIVE_PREVIEW.get("blank_between_seconds", 0.25))
MAX_DISPLAY_CHARS = int(LIVE_PREVIEW.get("max_display_chars", 96))
SHOW_STATUS_ON_LED = LIVE_PREVIEW.get("show_status_on_led", False)
NO_LIVE_TEXT = LIVE_PREVIEW.get("no_live_text", "BRAK MECZOW LIVE")
DISPLAY_WIDTH_CHARS = int(LIVE_PREVIEW.get("display_width_chars", 16))
STATIC_HOLD_SECONDS = float(LIVE_PREVIEW.get("static_hold_seconds", 3))
SCROLL_CHARS_PER_SECOND = float(LIVE_PREVIEW.get("scroll_chars_per_second", 5))
SCROLL_END_PAUSE_SECONDS = float(LIVE_PREVIEW.get("scroll_end_pause_seconds", 1))

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
    "Lech Poznań": "LPO",
    "Lech Poznan": "LPO",
    "AC Milan": "MIL",
    "Cagliari": "CAG",
    "Ecuador": "ECU",
    "Guatemala": "GUA",
}

COUNTRY_NAMES_PL = {
    "Argentina": "ARGENTYNA",
    "Bolivia": "BOLIWIA",
    "Brazil": "BRAZYLIA",
    "Chile": "CHILE",
    "Colombia": "KOLUMBIA",
    "Ecuador": "EKWADOR",
    "Guatemala": "GWATEMALA",
    "Paraguay": "PARAGWAJ",
    "Peru": "PERU",
    "Poland": "POLSKA",
    "Spain": "HISZPANIA",
    "Switzerland": "SZWAJCARIA",
    "USA": "USA",
    "World": "SWIAT",
}


def short_name(name):
    if not name:
        return "???"

    clean = name.replace("Club Atletico", "CA").replace("Atlético", "Atl")
    return SHORT_NAMES.get(name, clean.upper()[:3])


def normalize_display(text):
    text = " ".join(str(text).split())
    if MAX_DISPLAY_CHARS > 0 and len(text) > MAX_DISPLAY_CHARS:
        return text[:MAX_DISPLAY_CHARS].rstrip()
    return text


def trim_display(text):
    return normalize_display(text)


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


def get_status_text(ev):
    status = ev.get("status", {})
    status_desc = status.get("description") or status.get("type", "")
    time_data = ev.get("time", {}) or {}
    current_period_start = time_data.get("currentPeriodStartTimestamp")

    if status_desc:
        return status_desc
    if current_period_start:
        return "live"
    return "live"


def get_tournament_info(ev):
    tournament = ev.get("tournament", {}) or {}
    category = tournament.get("category", {}) or {}

    league = tournament.get("name") or tournament.get("uniqueTournament", {}).get("name") or "Liga"
    country = category.get("name") or category.get("country", {}).get("name") or ""
    country = COUNTRY_NAMES_PL.get(country, country.upper() if country else "")

    return country, league


def format_event(ev, include_status=True):
    home = ev.get("homeTeam", {}).get("name", "HOME")
    away = ev.get("awayTeam", {}).get("name", "AWAY")
    base = f"{short_name(home)} {get_score(ev)} {short_name(away)}"

    if include_status:
        return f"{base} ({get_status_text(ev)})"
    return base


def format_context_event(ev):
    country, league = get_tournament_info(ev)
    if country and league:
        return normalize_display(f"{country} - {league}")
    if league:
        return normalize_display(league)
    return normalize_display(country or "MECZ LIVE")


def format_score_event(ev):
    home = ev.get("homeTeam", {}).get("name", "HOME")
    away = ev.get("awayTeam", {}).get("name", "AWAY")
    base = f"{short_name(home)} {get_score(ev)} {short_name(away)}"

    if SHOW_STATUS_ON_LED:
        base = f"{base} ({get_status_text(ev)})"
    return normalize_display(base)


def format_full_display_event(ev):
    country, league = get_tournament_info(ev)
    score = format_score_event(ev)
    prefix = " - ".join(part for part in [country, league] if part)
    if prefix:
        return normalize_display(f"{prefix}: {score}")
    return score


def format_display_event(ev):
    return format_score_event(ev)


def enqueue_priority_message(text):
    text = normalize_display(text)
    if not text:
        return

    priority_messages.append((None, text))
    priority_signal.set()
    print(f"[DISPLAY PRIORITY QUEUED] {text}", flush=True)


def enqueue_priority_event(ev, prefix="GOAL"):
    context = format_context_event(ev)
    score = format_score_event(ev)
    text = normalize_display(f"{prefix} {score}")
    priority_messages.append((context, text))
    priority_signal.set()
    print(f"[DISPLAY PRIORITY QUEUED] {context} -> {text}", flush=True)


async def publish_json(client, topic, payload, retain=False):
    await client.publish(
        topic,
        json.dumps(payload, ensure_ascii=False),
        qos=0,
        retain=retain
    )
    print("[MQTT JSON]", topic, payload, flush=True)


async def publish_led(client, text, retain=False):
    await client.publish(
        LED_TOPIC,
        text,
        qos=1,
        retain=retain
    )
    print("[MQTT LED]", LED_TOPIC, repr(text), flush=True)


async def prepare_sofascore_session(session, force=False):
    global sofascore_session_ready

    if sofascore_session_ready and not force:
        return

    try:
        resp = await session.get(
            SOFASCORE_HOME,
            headers=PAGE_HEADERS,
            timeout=15,
        )
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

        resp = await session.get(
            url,
            headers=API_HEADERS,
            timeout=15,
        )

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


async def publish_live_preview(client, events):
    if not LIVE_PREVIEW_ENABLED:
        return

    preview_events = events[:LIVE_PREVIEW_LIMIT]
    items = []

    for ev in preview_events:
        home = ev.get("homeTeam", {}).get("name", "HOME")
        away = ev.get("awayTeam", {}).get("name", "AWAY")
        country, league = get_tournament_info(ev)
        context = format_context_event(ev)
        score = format_score_event(ev)
        item = {
            "id": ev.get("id"),
            "country": country,
            "league": league,
            "home": home,
            "away": away,
            "score": get_score(ev),
            "status": get_status_text(ev),
            "line": format_event(ev, include_status=True),
            "display_context": context,
            "display_context_seconds": round(calculate_display_seconds(context, CONTEXT_SECONDS), 2),
            "display_score": score,
            "display_score_seconds": round(calculate_display_seconds(score, DISPLAY_SECONDS), 2),
            "display": format_full_display_event(ev),
        }
        items.append(item)

    payload = {
        "type": "live_preview",
        "count": len(events),
        "shown": len(items),
        "display_mode": "league_then_score_dynamic_wait",
        "display_width_chars": DISPLAY_WIDTH_CHARS,
        "static_hold_seconds": STATIC_HOLD_SECONDS,
        "scroll_chars_per_second": SCROLL_CHARS_PER_SECOND,
        "matches": items,
    }

    await publish_json(client, LIVE_PREVIEW_TOPIC, payload, retain=True)

    if not events:
        print("[LIVE PREVIEW] no live matches or API returned no data", flush=True)
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
        last_all_live_scores[key] = score

        if not live_scores_initialized:
            continue

        if old_score is not None and old_score != score:
            enqueue_priority_event(ev, prefix="GOAL")
            print(f"[SCORE CHANGE] {key}: {old_score} -> {score}", flush=True)

    # remove finished/disappeared live events from score memory
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
            await publish_led(client, " ", retain=True)
            await asyncio.sleep(BLANK_BETWEEN_SECONDS)
            return False
        except asyncio.TimeoutError:
            return True

    await asyncio.sleep(seconds)
    return True


async def show_text_dynamic(client, text, minimum_seconds, interruptible=True):
    seconds = calculate_display_seconds(text, minimum_seconds)
    print(f"[DISPLAY WAIT] {seconds:.2f}s for {repr(text)}", flush=True)
    return await show_text_for_seconds(client, text, seconds, interruptible=interruptible)


async def show_priority_text(client, message):
    if isinstance(message, tuple):
        context, text = message
    else:
        context, text = None, message

    print(f"[DISPLAY PRIORITY] {context or ''} {text}", flush=True)

    if context:
        await show_text_dynamic(client, context, CONTEXT_SECONDS, interruptible=False)
        await publish_led(client, " ", retain=True)
        await asyncio.sleep(BLANK_BETWEEN_SECONDS)

    for _ in range(BLINK_COUNT):
        await publish_led(client, text, retain=True)
        await asyncio.sleep(BLINK_INTERVAL)
        await publish_led(client, " ", retain=True)
        await asyncio.sleep(BLINK_INTERVAL)

    await show_text_dynamic(client, text, DISPLAY_SECONDS, interruptible=False)
    await publish_led(client, " ", retain=True)
    await asyncio.sleep(BLANK_BETWEEN_SECONDS)


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
        context_text = format_context_event(ev)
        score_text = format_score_event(ev)
        context_wait = calculate_display_seconds(context_text, CONTEXT_SECONDS)
        score_wait = calculate_display_seconds(score_text, DISPLAY_SECONDS)
        print(
            f"[DISPLAY ROTATE] {index + 1}/{len(events)} "
            f"{context_text} ({context_wait:.2f}s) -> {score_text} ({score_wait:.2f}s)",
            flush=True,
        )
        index += 1

        context_completed = await show_text_dynamic(
            client,
            context_text,
            CONTEXT_SECONDS,
            interruptible=True,
        )

        if not context_completed:
            continue

        await publish_led(client, " ", retain=True)
        await asyncio.sleep(BLANK_BETWEEN_SECONDS)

        score_completed = await show_text_dynamic(
            client,
            score_text,
            DISPLAY_SECONDS,
            interruptible=True,
        )

        if score_completed:
            await publish_led(client, " ", retain=True)
            await asyncio.sleep(BLANK_BETWEEN_SECONDS)


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

            payload = {
                "type": "goal",
                "team": goal_team,
                "player": player,
                "minute": minute,
                "score": score
            }

            await publish_json(client, f"{team['mqtt_prefix']}/goal", payload)
            enqueue_priority_message(f"GOAL {short_name(goal_team)} {score}")

        elif inc_type == "card":
            color = inc.get("color", "yellow")
            minute = inc.get("time", "")
            player = inc.get("player", {}).get("name", "")
            card_team = inc.get("team", {}).get("name", "")

            payload = {
                "type": "card",
                "team": card_team,
                "player": player,
                "minute": minute,
                "color": color
            }

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

            payload = {
                "type": "score",
                "home": home,
                "away": away,
                "score": score,
                "status": status_desc
            }

            await publish_json(client, f"{team['mqtt_prefix']}/live", payload)
            await preload_incidents(session, event_id)

            print(f"[INIT] {home} {score} {away}", flush=True)
            continue

        old_score = last_scores.get(event_key)

        if old_score != score:
            last_scores[event_key] = score

            payload = {
                "type": "score",
                "home": home,
                "away": away,
                "score": score,
                "status": status_desc
            }

            await publish_json(client, f"{team['mqtt_prefix']}/live", payload)
            enqueue_priority_event(ev, prefix="GOAL")

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
        events = await fetch_live_events(session)
        current_live_events = events

        await publish_live_preview(client, events)
        await detect_live_score_changes(events)

        for team in TEAMS:
            await process_team(session, client, team, events)

        await asyncio.sleep(POLL_INTERVAL)


async def main():
    print("Starting SofaScore MQTT Bridge...", flush=True)

    while True:
        try:
            async with Client(
                hostname=MQTT_HOST,
                port=MQTT_PORT,
                username=MQTT_USER,
                password=MQTT_PASS
            ) as client:

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
