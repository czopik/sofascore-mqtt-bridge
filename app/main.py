import asyncio
import json
import aiohttp
import yaml
from asyncio_mqtt import Client, MqttError

SOFASCORE_HOME = "https://www.sofascore.com/"

PAGE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
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
TEAMS = CONFIG["teams"]
POLL_INTERVAL = CONFIG.get("poll_interval", 15)

last_scores = {}
known_incidents = set()
known_periods = set()
initialized_events = set()
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


async def publish_json(client, topic, payload):
    await client.publish(
        topic,
        json.dumps(payload, ensure_ascii=False),
        qos=0,
        retain=False
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
        async with session.get(
            SOFASCORE_HOME,
            headers=PAGE_HEADERS,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            body = await resp.text()
            print(f"[HTTP INIT] SofaScore homepage status {resp.status}", flush=True)
            if resp.status != 200:
                print(f"[HTTP INIT BODY] {body[:300]}", flush=True)

            sofascore_session_ready = True

    except Exception as e:
        print("[HTTP INIT ERROR]", e, flush=True)
        sofascore_session_ready = False


async def fetch_json(session, url, retry=True):
    global sofascore_session_ready

    try:
        await prepare_sofascore_session(session)

        async with session.get(
            url,
            headers=API_HEADERS,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                print(f"[HTTP ERROR] {resp.status} {url}", flush=True)
                print(f"[HTTP BODY] {body[:500]}", flush=True)

                if resp.status == 403 and retry:
                    print("[HTTP RETRY] refreshing SofaScore session and retrying once", flush=True)
                    sofascore_session_ready = False
                    await prepare_sofascore_session(session, force=True)
                    await asyncio.sleep(1)
                    return await fetch_json(session, url, retry=False)

                return {}

            return await resp.json()

    except Exception as e:
        print("[FETCH ERROR]", e, flush=True)
        return {}


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


async def process_team(session, client, team):
    live_url = "https://api.sofascore.com/api/v1/sport/football/events/live"
    data = await fetch_json(session, live_url)
    events = data.get("events", [])

    print(f"[DEBUG] live events: {len(events)}", flush=True)

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

        hs = ev.get("homeScore", {}).get("current", 0)
        aw = ev.get("awayScore", {}).get("current", 0)
        score = f"{hs}:{aw}"

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

                timeout = aiohttp.ClientTimeout(total=15)
                connector = aiohttp.TCPConnector(ssl=False)
                async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                    while True:
                        for team in TEAMS:
                            await process_team(session, client, team)

                        await asyncio.sleep(POLL_INTERVAL)

        except MqttError as e:
            print("[MQTT ERROR]", e, flush=True)
            await asyncio.sleep(5)

        except Exception as e:
            print("[MAIN ERROR]", e, flush=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
