import asyncio
from bet365_client import Bet365Client


async def discover_table_tennis_leagues():
    client = Bet365Client()
    try:
        sport_id = 92
        leagues = set()
        page = 1

        while True:
            print(f"Obtendo página {page}...")
            response = await client.upcoming(sport_id=sport_id, page=page)

            if not response.get("success", 1) or "results" not in response:
                print("Resposta inválida ou sem resultados.")
                print(f"Resposta: {response}")
                break

            results = response["results"]
            if not results:
                print("Nenhum resultado encontrado. Parando.")
                break

            for event in results:
                league_info = event.get("league")
                if league_info:
                    league_id = league_info["id"]
                    league_name = league_info["name"]
                    leagues.add((league_id, league_name))
                    print(f"Encontrada liga: {league_id} - {league_name}")

            # Verifica se há mais páginas
            pager = response.get("pager", {})
            if page >= pager.get("total", 1):
                break

            page += 1
            await asyncio.sleep(1)  # Respeitar rate limit

        # Salvar ligas encontradas
        with open("table_tennis_leagues.txt", "w", encoding="utf-8") as f:
            for league_id, league_name in sorted(leagues):
                f.write(f"{league_id}: {league_name}\n")
                print(f"{league_id}: {league_name}")

    except Exception as e:
        print(f"Erro: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(discover_table_tennis_leagues())
