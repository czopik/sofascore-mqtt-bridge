import asyncio
import json
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

last_scores = {}
known_incidents = set()
known_periods = set()
initialized_events = set()
last_live_preview_text = None
sofascore_session_ready = False

SHORT_NAMES = {
    "Juventus": "JUV",
    "Torino": "TOR",
    "Lech Poznań": "LPO",
    "Lech Poznan": "LPO",
    "AC Milan": "MIL",
    "Cagliari": "CAG",
}


def short_name(name):
    if not name:
        return "???"
    return SHORT_NAMES.get(name, name.upper()[:3])


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


def format_event(ev):
    home = ev.get("homeTeam", {}).get("name", "HOME")
    away = ev.get("awayTeam", {}).get("name", "AWAY")
    return f"{short_name(home)} {get_score(ev)} {short_name(away)} ({get_status_text(ev)})"


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
    print("[MQTT LED]", LED_TOPIC, text, flush=True)


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
    global last_live_preview_text

    if not LIVE_PREVIEW_ENABLED:
        return

    preview_events = events[:LIVE_PREVIEW_LIMIT]
    items = []

    for ev in preview_events:
        home = ev.get("homeTeam", {}).get("name", "HOME")
        away = ev.get("awayTeam", {}).get("name", "AWAY")
        item = {
            "id": ev.get("id"),
            "home": home,
            "away": away,
            "score": get_score(ev),
            "status": get_status_text(ev),
            "line": format_event(ev),
        }
        items.append(item)

    payload = {
        "type": "live_preview",
        "count": len(events),
        "shown": len(items),
        "matches": items,
    }

    await publish_json(client, LIVE_PREVIEW_TOPIC, payload, retain=True)

    if not events:
        print("[LIVE PREVIEW] no live matches or API returned no data", flush=True)
        return

    text = " | ".join(item["line"] for item in items)
    print(f"[LIVE PREVIEW] {text}", flush=True)

    if LIVE_PREVIEW_LED and text != last_live_preview_text:
        last_live_preview_text = text
        await publish_led(client, text[:250], retain=True)


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

            led = f"GOAL {short_name(goal_team)} {score}"
            await publish_led(client, led)

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
                led = f"RED {short_name(card_team)} {minute}'"
                await publish_led(client, led)


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

            led = f"{short_name(home)} {score} {short_name(away)}"
            await publish_led(client, led, retain=True)

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

            led = f"{short_name(home)} {score} {short_name(away)}"
            await publish_led(client, led, retain=True)

        period_key = f"{event_id}:{status_type}:{status_desc}"

        if period_key not in known_periods:
            known_periods.add(period_key)

            if "Halftime" in status_desc:
                await publish_led(client, f"HT {short_name(home)} {score} {short_name(away)}")

            elif "Ended" in status_desc or status_type == "finished":
                await publish_led(client, f"FT {short_name(home)} {score} {short_name(away)}")

        await handle_incidents(session, client, team, event_id, score)

    if not found:
        print(f"[INFO] no live match for {team['name']}", flush=True)


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
                    while True:
                        events = await fetch_live_events(session)
                        await publish_live_preview(client, events)

                        for team in TEAMS:
                            await process_team(session, client, team, events)

                        await asyncio.sleep(POLL_INTERVAL)

        except MqttError as e:
            print("[MQTT ERROR]", e, flush=True)
            await asyncio.sleep(5)

        except Exception as e:
            print("[MAIN ERROR]", e, flush=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
