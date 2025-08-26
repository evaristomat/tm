import requests
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()


def get_event_ids_from_leagues():
    """Coleta um event_id de cada liga de tênis de mesa"""

    # Configurações
    API_KEY = os.getenv("BETSAPI_API_KEY")
    if not API_KEY:
        print("❌ BETSAPI_API_KEY não encontrada nas variáveis de ambiente")
        return

    # Ligações de tênis de mesa
    LEAGUES = {
        10048210: "Czech Liga Pro",
        10068516: "Challenger Series TT",
        10073432: "TT Cup",
        10073465: "TT Elite Series",
    }

    # Data de hoje
    today = datetime.now().strftime("%Y%m%d")

    print("=" * 60)
    print("COLETANDO EVENT IDs PARA TESTE DE RESULTADOS")
    print("=" * 60)

    event_ids = {}

    for league_id, league_name in LEAGUES.items():
        print(f"\n🔍 Buscando evento para: {league_name}")

        # Fazer a requisição
        url = "https://api.betsapi.com/v1/bet365/upcoming"
        params = {
            "token": API_KEY,
            "sport_id": 92,  # Tênis de mesa
            "league_id": league_id,
            "day": today,
        }

        try:
            response = requests.get(url, params=params)
            data = response.json()

            if (
                data.get("success") == 1
                and "results" in data
                and len(data["results"]) > 0
            ):
                # Pegar o primeiro evento da lista
                event = data["results"][0]
                event_id = event.get("id")
                home_team = event.get("home", {}).get("name", "N/A")
                away_team = event.get("away", {}).get("name", "N/A")
                event_time = event.get("time")
                time_status = event.get("time_status")

                # Converter timestamp para data/hora legível
                if event_time:
                    try:
                        event_time = datetime.fromtimestamp(int(event_time)).strftime(
                            "%Y-%m-%d %H:%M"
                        )
                    except:
                        event_time = "N/A"

                event_ids[league_name] = {
                    "id": event_id,
                    "home_team": home_team,
                    "away_team": away_team,
                    "time": event_time,
                    "time_status": time_status,
                }

                print(f"   ✅ Evento encontrado:")
                print(f"      ID: {event_id}")
                print(f"      Partida: {home_team} vs {away_team}")
                print(f"      Horário: {event_time}")
                print(
                    f"      Status: {time_status} (0=Pré-jogo, 1=Ao vivo, 3=Finalizado)"
                )
            else:
                print(f"   ❌ Nenhum evento encontrado para {league_name} hoje")

        except Exception as e:
            print(f"   ❌ Erro na requisição: {e}")

    return event_ids


def test_results_endpoint(event_data):
    """Testa o endpoint de resultados com os event_ids coletados"""

    API_KEY = os.getenv("BETSAPI_API_KEY")
    if not API_KEY:
        print("❌ BETSAPI_API_KEY não encontrada nas variáveis de ambiente")
        return

    print("\n" + "=" * 60)
    print("TESTANDO ENDPOINT DE RESULTADOS")
    print("=" * 60)

    # Criar lista de event_ids para testar múltiplos de uma vez
    event_ids = [str(data["id"]) for data in event_data.values()]
    event_ids_str = ",".join(event_ids)

    print(f"🎯 Testando resultados para eventos: {event_ids_str}")

    # Fazer a requisição para o endpoint de resultados
    url = "https://api.betsapi.com/v1/bet365/result"
    params = {
        "token": API_KEY,
        "event_id": event_ids_str,  # Múltiplos event_ids separados por vírgula
    }

    try:
        response = requests.get(url, params=params)
        data = response.json()

        print(f"Status: {data.get('success', 'N/A')}")

        if data.get("success") == 1:
            # Verificar se há resultados disponíveis
            if "results" in data and len(data["results"]) > 0:
                print(f"✅ Resultados encontrados: {len(data['results'])}")

                for result in data["results"]:
                    event_id = result.get("id")
                    score = result.get("ss", "N/A")
                    time_status = result.get("time_status", "N/A")

                    # Encontrar informações do evento
                    event_info = None
                    for league, info in event_data.items():
                        if str(info["id"]) == str(event_id):
                            event_info = info
                            break

                    league_name = league if event_info else "Desconhecido"

                    print(f"\n📊 Resultado para {league_name}:")
                    print(f"   Event ID: {event_id}")
                    if event_info:
                        print(
                            f"   Partida: {event_info['home_team']} vs {event_info['away_team']}"
                        )
                    print(f"   Placar: {score}")
                    print(f"   Status: {time_status}")

                    # Informações adicionais se disponíveis
                    if "timer" in result:
                        print(f"   Timer: {result['timer']}")
                    if "scores" in result:
                        print(f"   Scores: {result['scores']}")
            else:
                print(f"ℹ️  Nenhum resultado encontrado para os eventos")
        else:
            print(f"❌ Erro na resposta da API")
            if "error" in data:
                print(f"Mensagem: {data.get('error')}")

    except Exception as e:
        print(f"❌ Erro na requisição: {e}")


def test_individual_results(event_data):
    """Testa o endpoint de resultados para cada evento individualmente"""

    API_KEY = os.getenv("BETSAPI_API_KEY")
    if not API_KEY:
        print("❌ BETSAPI_API_KEY não encontrada nas variáveis de ambiente")
        return

    print("\n" + "=" * 60)
    print("TESTANDO ENDPOINT DE RESULTADOS INDIVIDUALMENTE")
    print("=" * 60)

    for league_name, data in event_data.items():
        event_id = data["id"]

        print(f"\n🎯 Testando resultado para: {league_name} (Event ID: {event_id})")

        # Fazer a requisição para o endpoint de resultados
        url = "https://api.betsapi.com/v1/bet365/result"
        params = {"token": API_KEY, "event_id": event_id}

        try:
            response = requests.get(url, params=params)
            data_result = response.json()

            print(f"   Status: {data_result.get('success', 'N/A')}")

            if data_result.get("success") == 1:
                # Verificar se há resultados disponíveis
                if "results" in data_result and len(data_result["results"]) > 0:
                    result = data_result["results"][0]
                    score = result.get("ss", "N/A")
                    time_status = result.get("time_status", "N/A")

                    print(f"   ✅ Resultado encontrado:")
                    print(f"      Placar: {score}")
                    print(f"      Status: {time_status}")

                    # Informações adicionais se disponíveis
                    if "timer" in result:
                        print(f"      Timer: {result['timer']}")
                    if "scores" in result:
                        print(f"      Scores: {result['scores']}")
                else:
                    print(f"   ℹ️  Nenhum resultado encontrado para este evento")
            else:
                print(f"   ❌ Erro na resposta da API")
                if "error" in data_result:
                    print(f"   Mensagem: {data_result.get('error')}")

        except Exception as e:
            print(f"   ❌ Erro na requisição: {e}")


if __name__ == "__main__":
    # Coletar event_ids
    event_data = get_event_ids_from_leagues()

    # Testar endpoint de resultados se encontramos eventos
    if event_data:
        # Primeiro teste com múltiplos eventos de uma vez
        test_results_endpoint(event_data)

        # Depois teste individual para cada evento
        test_individual_results(event_data)
    else:
        print("\n❌ Não foi possível encontrar eventos para testar")
