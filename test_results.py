import requests
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def search_specific_event(event_id):
    """Busca um evento específico pela API"""
    API_KEY = os.getenv("BETSAPI_API_KEY")
    if not API_KEY:
        print("API_KEY não encontrada!")
        return None

    # Primeiro tenta buscar resultado
    url = "https://api.betsapi.com/v1/bet365/result"
    params = {"token": API_KEY, "event_id": event_id}

    try:
        response = requests.get(url, params=params)
        data = response.json()

        print(f"\n=== RESULTADO EVENT_ID: {event_id} ===")
        print(f"Success: {data.get('success')}")
        print(f"Paging: {data.get('paging')}")

        if data.get("success") == 1 and data.get("results"):
            result = data["results"][0]
            print(f"ID: {result.get('id')}")
            print(f"Time Status: {result.get('time_status')}")
            print(f"SS (Score): {result.get('ss')}")
            print(f"Home: {result.get('home', {}).get('name')}")
            print(f"Away: {result.get('away', {}).get('name')}")
            print(f"Scores: {result.get('scores')}")
            return result
        else:
            print(f"Nenhum resultado encontrado ou erro: {data}")
            return None

    except Exception as e:
        print(f"Erro na requisição: {e}")
        return None


def search_event_info(event_id):
    """Busca informações do evento (upcoming/inplay)"""
    API_KEY = os.getenv("BETSAPI_API_KEY")
    if not API_KEY:
        return None

    # Tenta buscar como evento upcoming
    url = "https://api.betsapi.com/v1/bet365/event"
    params = {"token": API_KEY, "FI": event_id}

    try:
        response = requests.get(url, params=params)
        data = response.json()

        print(f"\n=== INFO EVENT_ID: {event_id} ===")
        print(f"Success: {data.get('success')}")

        if data.get("success") == 1 and data.get("results"):
            result = data["results"][0]
            print(f"ID: {result.get('id')}")
            print(f"Time: {result.get('time')}")
            print(f"Time Status: {result.get('time_status')}")
            print(f"Home: {result.get('home', {}).get('name')}")
            print(f"Away: {result.get('away', {}).get('name')}")
            return result
        else:
            print(f"Evento não encontrado como upcoming: {data}")
            return None

    except Exception as e:
        print(f"Erro na requisição info: {e}")
        return None


def main():
    """Busca todos os event_ids pendentes"""
    pending_event_ids = [
        181588970,  # Josef Medek vs Jan Zajicek
        181585379,  # Daniel Murawski vs Damian Wasilewski
        181588993,  # Josef Medek vs Michal Vavrecka
        181585398,  # Grzegorz Adamiak vs Mateusz Golebiowski
        181589029,  # Michal Regner vs Dan Volhejn
        181589028,  # Frantisek Briza vs Kyryl Darin
        181589040,  # Jiri Louda vs Martin Sychra
        181589107,  # Jiri Plachy vs Daniel Tuma
    ]

    print(f"Buscando {len(pending_event_ids)} eventos específicos...")

    for event_id in pending_event_ids:
        print(f"\n{'=' * 60}")

        # Primeiro busca resultado
        result = search_specific_event(event_id)

        # Se não encontrou resultado, busca info do evento
        if result is None:
            info = search_event_info(event_id)
            if info is None:
                print(f"Event ID {event_id} não encontrado em nenhuma endpoint!")

        print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
