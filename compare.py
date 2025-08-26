import sqlite3
import pandas as pd
from datetime import datetime
import random


class DetailedPlayerStatsAnalyzer:
    def __init__(
        self, tm_db_path="tm_data.db", results_db_path="table_tennis_results.db"
    ):
        self.tm_db_path = tm_db_path
        self.results_db_path = results_db_path

    def get_random_match_from_tm_db(self):
        """Obtém um jogo aleatório do banco tm_data.db para análise"""
        conn = sqlite3.connect(self.tm_db_path)

        # Buscar jogos com time_status = 3 (finalizado)
        query = """
        SELECT id, league_name, home_team, away_team, time 
        FROM events 
        WHERE time_status = 3 
        ORDER BY time DESC 
        LIMIT 100
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        if df.empty:
            print("❌ Nenhum jogo finalizado encontrado no tm_data.db")
            return None

        # Selecionar um jogo aleatório
        match = df.sample(n=1).iloc[0].to_dict()
        match["event_time"] = datetime.fromtimestamp(match["time"])

        print("🎯 JOGO ALEATÓRIO SELECIONADO PARA ANÁLISE:")
        print(f"   ID: {match['id']}")
        print(f"   Liga: {match['league_name']}")
        print(f"   Partida: {match['home_team']} vs {match['away_team']}")
        print(f"   Data: {match['event_time']}")

        return match

    def get_player_last_10_matches(self, player_name, before_date):
        """Busca os últimos 10 jogos de um jogador antes de uma data específica"""
        conn = sqlite3.connect(self.results_db_path)

        # Buscar jogos onde o jogador participou (como home ou away)
        query = """
        SELECT 
            event_id, event_time, league_name, 
            home_name, away_name, score, bestofsets
        FROM events 
        WHERE (home_name = ? OR away_name = ?) 
        AND event_time < ?
        ORDER BY event_time DESC 
        LIMIT 10
        """

        df = pd.read_sql_query(
            query, conn, params=(player_name, player_name, before_date.timestamp())
        )
        conn.close()

        return df

    def get_detailed_scores(self, event_id):
        """Obtém os scores detalhados por set para um evento específico"""
        conn = sqlite3.connect(self.results_db_path)

        query = """
        SELECT set_number, home_score, away_score 
        FROM event_scores 
        WHERE event_id = ?
        ORDER BY set_number
        """

        df = pd.read_sql_query(query, conn, params=(event_id,))
        conn.close()

        return df

    def calculate_detailed_stats(self, player_name, matches_df):
        """Calcula estatísticas detalhadas incluindo total de games e sets ganhos"""
        if matches_df.empty:
            return {
                "total_matches": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0,
                "total_games_played": 0,
                "avg_games_per_match": 0,
                "matches_with_at_least_one_set": 0,
                "percentage_with_at_least_one_set": 0,
                "games_per_match_list": [],  # Lista de totais de games por partida
                "recent_matches": [],
            }

        stats = {
            "total_matches": len(matches_df),
            "wins": 0,
            "losses": 0,
            "total_games_played": 0,
            "matches_with_at_least_one_set": 0,
            "games_per_match_list": [],  # Lista de totais de games por partida
            "recent_matches": [],
        }

        for _, match in matches_df.iterrows():
            # Determinar se o jogador é home ou away
            is_home = match["home_name"] == player_name

            # Obter scores detalhados por set
            detailed_scores = self.get_detailed_scores(match["event_id"])

            match_stats = {
                "opponent": match["away_name"] if is_home else match["home_name"],
                "score": match["score"],
                "sets_won": 0,
                "total_games": 0,
                "won_at_least_one_set": False,
            }

            # Calcular estatísticas por set
            for _, set_score in detailed_scores.iterrows():
                home_score = set_score["home_score"]
                away_score = set_score["away_score"]

                # Contar games totais
                match_stats["total_games"] += home_score + away_score

                # Verificar se o jogador ganhou este set
                if (is_home and home_score > away_score) or (
                    not is_home and away_score > home_score
                ):
                    match_stats["sets_won"] += 1

            # Adicionar à lista de games por partida
            stats["games_per_match_list"].append(match_stats["total_games"])

            # Verificar se ganhou pelo menos um set
            match_stats["won_at_least_one_set"] = match_stats["sets_won"] > 0
            if match_stats["won_at_least_one_set"]:
                stats["matches_with_at_least_one_set"] += 1

            # Adicionar ao total de games
            stats["total_games_played"] += match_stats["total_games"]

            # Verificar quem ganhou o jogo
            score = match["score"]
            if score and "-" in score:
                try:
                    home_score, away_score = map(int, score.split("-"))

                    # Verificar quem ganhou
                    if (is_home and home_score > away_score) or (
                        not is_home and away_score > home_score
                    ):
                        stats["wins"] += 1
                    else:
                        stats["losses"] += 1
                except ValueError:
                    # Se não for possível converter para int, pular este jogo
                    pass

            # Adicionar às estatísticas recentes
            stats["recent_matches"].append(match_stats)

        # Calcular métricas derivadas
        stats["win_rate"] = (
            stats["wins"] / stats["total_matches"] * 100
            if stats["total_matches"] > 0
            else 0
        )
        stats["avg_games_per_match"] = (
            stats["total_games_played"] / stats["total_matches"]
            if stats["total_matches"] > 0
            else 0
        )
        stats["percentage_with_at_least_one_set"] = (
            stats["matches_with_at_least_one_set"] / stats["total_matches"] * 100
            if stats["total_matches"] > 0
            else 0
        )

        return stats

    def analyze_match(self):
        """Analisa um jogo e as estatísticas dos últimos 10 jogos de cada participante"""
        # Obter um jogo aleatório do tm_data.db
        match = self.get_random_match_from_tm_db()
        if not match:
            return

        print("\n" + "=" * 60)
        print("ANÁLISE DETALHADA DOS ÚLTIMOS 10 JOGOS")
        print("=" * 60)

        # Buscar últimos 10 jogos de cada jogador antes deste jogo
        home_player = match["home_team"]
        away_player = match["away_team"]
        match_date = match["event_time"]

        print(f"\n🔍 Buscando últimos 10 jogos de {home_player}...")
        home_matches = self.get_player_last_10_matches(home_player, match_date)
        print(f"   Encontrados {len(home_matches)} jogos")

        print(f"🔍 Buscando últimos 10 jogos de {away_player}...")
        away_matches = self.get_player_last_10_matches(away_player, match_date)
        print(f"   Encontrados {len(away_matches)} jogos")

        # Calcular estatísticas detalhadas
        home_stats = self.calculate_detailed_stats(home_player, home_matches)
        away_stats = self.calculate_detailed_stats(away_player, away_matches)

        # Exibir resultados
        print(f"\n📊 ESTATÍSTICAS DETALHADAS DE {home_player}:")
        print(f"   Total de jogos: {home_stats['total_matches']}")
        print(f"   Vitórias: {home_stats['wins']} ({home_stats['win_rate']:.1f}%)")
        print(f"   Derrotas: {home_stats['losses']}")
        print(f"   Total de games jogados: {home_stats['total_games_played']}")
        print(
            f"   Média de games por partida: {home_stats['avg_games_per_match']:.1f} - DEBUG: {home_stats['games_per_match_list']}"
        )
        print(
            f"   Partidas com pelo menos 1 set ganho: {home_stats['matches_with_at_least_one_set']} ({home_stats['percentage_with_at_least_one_set']:.1f}%)"
        )

        print(f"\n📊 ESTATÍSTICAS DETALHADAS DE {away_player}:")
        print(f"   Total de jogos: {away_stats['total_matches']}")
        print(f"   Vitórias: {away_stats['wins']} ({away_stats['win_rate']:.1f}%)")
        print(f"   Derrotas: {away_stats['losses']}")
        print(f"   Total de games jogados: {away_stats['total_games_played']}")
        print(
            f"   Média de games por partida: {away_stats['avg_games_per_match']:.1f} - DEBUG: {away_stats['games_per_match_list']}"
        )
        print(
            f"   Partidas com pelo menos 1 set ganho: {away_stats['matches_with_at_least_one_set']} ({away_stats['percentage_with_at_least_one_set']:.1f}%)"
        )

        # Exemplo de análise para over/under
        print(f"\n🎯 ANÁLISE PARA OVER/UNDER 75.5 GAMES:")

        # Para o jogador da casa
        if home_stats["games_per_match_list"]:
            over_count = sum(
                1 for games in home_stats["games_per_match_list"] if games > 75.5
            )
            under_count = len(home_stats["games_per_match_list"]) - over_count
            print(
                f"   {home_player}: {over_count} jogos OVER, {under_count} jogos UNDER"
            )

            # Mostrar cada jogo individualmente
            print(f"   Detalhes {home_player}:")
            for i, games in enumerate(home_stats["games_per_match_list"]):
                result = "OVER" if games > 75.5 else "UNDER"
                print(f"     Jogo {i + 1}: {games} games → {result}")

        # Para o jogador visitante
        if away_stats["games_per_match_list"]:
            over_count = sum(
                1 for games in away_stats["games_per_match_list"] if games > 75.5
            )
            under_count = len(away_stats["games_per_match_list"]) - over_count
            print(
                f"   {away_player}: {over_count} jogos OVER, {under_count} jogos UNDER"
            )

            # Mostrar cada jogo individualmente
            print(f"   Detalhes {away_player}:")
            for i, games in enumerate(away_stats["games_per_match_list"]):
                result = "OVER" if games > 75.5 else "UNDER"
                print(f"     Jogo {i + 1}: {games} games → {result}")

        # Salvar resultados em um arquivo CSV
        self.save_detailed_stats_to_csv(
            home_player, home_stats, away_player, away_stats, match
        )

    def save_detailed_stats_to_csv(
        self, home_player, home_stats, away_player, away_stats, match
    ):
        """Salva as estatísticas detalhadas em um arquivo CSV"""
        import csv
        from datetime import datetime

        filename = (
            f"detailed_player_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "player",
                "matches",
                "wins",
                "losses",
                "win_rate",
                "total_games",
                "avg_games_per_match",
                "matches_with_at_least_one_set",
                "percentage_with_at_least_one_set",
                "games_list",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerow(
                {
                    "player": home_player,
                    "matches": home_stats["total_matches"],
                    "wins": home_stats["wins"],
                    "losses": home_stats["losses"],
                    "win_rate": f"{home_stats['win_rate']:.1f}%",
                    "total_games": home_stats["total_games_played"],
                    "avg_games_per_match": f"{home_stats['avg_games_per_match']:.1f}",
                    "matches_with_at_least_one_set": home_stats[
                        "matches_with_at_least_one_set"
                    ],
                    "percentage_with_at_least_one_set": f"{home_stats['percentage_with_at_least_one_set']:.1f}%",
                    "games_list": str(home_stats["games_per_match_list"]),
                }
            )

            writer.writerow(
                {
                    "player": away_player,
                    "matches": away_stats["total_matches"],
                    "wins": away_stats["wins"],
                    "losses": away_stats["losses"],
                    "win_rate": f"{away_stats['win_rate']:.1f}%",
                    "total_games": away_stats["total_games_played"],
                    "avg_games_per_match": f"{away_stats['avg_games_per_match']:.1f}",
                    "matches_with_at_least_one_set": away_stats[
                        "matches_with_at_least_one_set"
                    ],
                    "percentage_with_at_least_one_set": f"{away_stats['percentage_with_at_least_one_set']:.1f}%",
                    "games_list": str(away_stats["games_per_match_list"]),
                }
            )

        print(f"\n💾 Estatísticas detalhadas salvas em {filename}")


def main():
    # Inicializar analisador
    analyzer = DetailedPlayerStatsAnalyzer()

    # Executar análise
    analyzer.analyze_match()


if __name__ == "__main__":
    main()
