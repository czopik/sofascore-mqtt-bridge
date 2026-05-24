# SofaScore MQTT Bridge

Bridge between SofaScore live football API and MQTT LED displays.

## Features

- live scores
- goal notifications
- red cards
- halftime/fulltime messages
- MQTT JSON events
- MQTT LED text messages
- Docker support

## Run

```bash
docker compose up -d --build
```

## MQTT topics

JSON:

- sports/football/test/live
- sports/football/test/goal
- sports/football/test/card

LED:

- all

## Example LED messages

- TOR 1:2 JUV
- GOAL JUV 1:3
- RED MIL 76'
- HT TOR 1:0 JUV
- FT TOR 1:2 JUV
