import requests
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def get_table_tennis_events():
    """Coleta eventos de tênis de mesa para teste"""
    API_KEY = os.getenv("BETSAPI_API_KEY")
    if not API_KEY:
        return None

    LEAGUES = {
        10048210: "Czech Liga Pro",
        10068516: "Challenger Series TT",
        10073432: "TT Cup",
        10073465: "TT Elite Series",
    }

    today = datetime.now().strftime("%Y%m%d")
    events = {}

    for league_id, league_name in LEAGUES.items():
        url = "https://api.betsapi.com/v1/bet365/upcoming"
        params = {
            "token": API_KEY,
            "sport_id": 92,
            "league_id": league_id,
            "day": today,
        }

        try:
            response = requests.get(url, params=params)
            data = response.json()

            if data.get("success") == 1 and data.get("results"):
                event = data["results"][0]
                events[league_name] = {
                    "id": event.get("id"),
                    "home": event.get("home", {}).get("name"),
                    "away": event.get("away", {}).get("name"),
                    "time": datetime.fromtimestamp(int(event.get("time", 0))).strftime(
                        "%Y-%m-%d %H:%M"
                    )
                    if event.get("time")
                    else None,
                    "time_status": event.get("time_status"),
                }

        except Exception:
            continue

    return events


def get_table_tennis_results(events):
    """Busca resultados completos dos eventos de tênis de mesa"""
    API_KEY = os.getenv("BETSAPI_API_KEY")
    if not API_KEY or not events:
        return None

    results = {}

    for league_name, event_data in events.items():
        event_id = event_data["id"]

        url = "https://api.betsapi.com/v1/bet365/result"
        params = {"token": API_KEY, "event_id": event_id}

        try:
            response = requests.get(url, params=params)
            data = response.json()

            if data.get("success") == 1 and data.get("results"):
                result = data["results"][0]

                results[league_name] = {
                    "id": result.get("id"),
                    "time": result.get("time"),
                    "time_status": result.get("time_status"),
                    "league": result.get("league", {}),
                    "home": result.get("home", {}),
                    "away": result.get("away", {}),
                    "ss": result.get("ss"),
                    "scores": result.get("scores", {}),
                    "stats": result.get("stats", {}),
                    "events": result.get("events", []),
                    "extra": result.get("extra", {}),
                    "timer": result.get("timer"),
                    "periods": result.get("periods", []),
                }

        except Exception:
            results[league_name] = {"error": "Falha na requisição"}

    return results


def display_results(results):
    """Exibe a estrutura completa dos resultados"""
    if not results:
        print("Nenhum resultado encontrado")
        return

    for league_name, result in results.items():
        print(f"\n{'=' * 60}")
        print(f"LIGA: {league_name}")
        print(f"{'=' * 60}")

        for key, value in result.items():
            print(f"{key}: {value}")


if __name__ == "__main__":
    events = get_table_tennis_events()
    if events:
        results = get_table_tennis_results(events)
        display_results(results)
    else:
        print("Nenhum evento encontrado")
