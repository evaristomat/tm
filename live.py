import asyncio
import os

import httpx
from dotenv import load_dotenv

load_dotenv()


async def listar_e_monitorar():
    api_key = os.getenv("BETSAPI_API_KEY")

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(
            "https://api.b365api.com/v1/bet365/inplay_filter",
            params={"sport_id": 92, "token": api_key},
        )

        eventos = response.json()

        print("\n" + "=" * 60)
        print("JOGOS AO VIVO")
        print("=" * 60)

        jogos_validos = []
        for evento in eventos.get("results", []):
            if evento["league"]["name"] != "TT Cup":
                jogos_validos.append(evento)
                print(
                    f"{len(jogos_validos)}. {evento['home']['name']} vs {evento['away']['name']}"
                )
                print(f"   Liga: {evento['league']['name']}")
                print(f"   Placar: {evento['ss']}")
                print()

        if not jogos_validos:
            print("Nenhum jogo disponível")
            return

        escolha = input("Escolha o número do jogo (ou Enter para o primeiro): ")
        idx = int(escolha) - 1 if escolha else 0

        evento_escolhido = jogos_validos[idx]

        response2 = await client.get(
            "https://api.b365api.com/v1/bet365/event",
            params={"FI": evento_escolhido["id"], "token": api_key},
        )

        detalhes = response2.json()
        if detalhes.get("success") != 1:
            print("Erro ao buscar detalhes")
            return

        home_name = ""
        away_name = ""

        for item in detalhes["results"][0]:
            if item.get("type") == "TE":
                if item.get("ID") == "1":
                    home_name = item.get("NA")
                elif item.get("ID") == "2":
                    away_name = item.get("NA")

        # Conta games vencidos
        home_sets = 0
        away_sets = 0
        for item in detalhes["results"][0]:
            if item.get("type") == "ST" and item.get("NA") == "Wins Game":
                if not item.get("GM"):  # Stats gerais do match
                    if item.get("TE") == "0":
                        home_sets += 1
                    elif item.get("TE") == "1":
                        away_sets += 1

        game_numero = home_sets + away_sets + 1
        placar_game = evento_escolhido["ss"]

        # Pega TODOS os pontos com GM (qualquer game)
        todos_pontos = []
        for item in detalhes["results"][0]:
            if (
                item.get("type") == "ST"
                and item.get("GM")
                and "Point" in item.get("NA", "")
                and "Winner" in item.get("NA", "")
            ):
                todos_pontos.append(
                    {
                        "game": item.get("GM"),
                        "ponto": item["NA"],
                        "placar": item.get("SS", ""),
                        "vencedor": item.get("TE", ""),
                    }
                )

        # Filtra apenas do game atual
        pontos_game_atual = [p for p in todos_pontos if p["game"] == str(game_numero)]

        print("\n" + "=" * 60)
        print(f"{home_name} vs {away_name}")
        print("=" * 60)
        print(f"PLACAR EM SETS: {home_sets} - {away_sets}")
        print(f"GAME {game_numero} ATUAL: {placar_game}")
        print("=" * 60)

        if pontos_game_atual:
            print(f"\nÚLTIMOS 10 PONTOS DO GAME {game_numero}:")
            for p in pontos_game_atual[-10:]:
                venc = home_name if p["vencedor"] == "0" else away_name
                print(f"{p['ponto']}: {p['placar']} → {venc}")
        else:
            print(f"\nNenhum ponto encontrado para o Game {game_numero}")
            print(f"Total de pontos em todos os games: {len(todos_pontos)}")

        print("=" * 60)


asyncio.run(listar_e_monitorar())
