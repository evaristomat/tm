import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import random
import logging
from colorama import Fore, Style, init

# Inicializar colorama para logs coloridos
init(autoreset=True)

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("bet_value_analyzer")


class EnhancedBetValueAnalyzer:
    def __init__(
        self, tm_db_path="tm_data.db", results_db_path="table_tennis_results.db"
    ):
        self.tm_db_path = tm_db_path
        self.results_db_path = results_db_path
        self.leagues = {
            10048210: "Czech Liga Pro",
            10068516: "Challenger Series TT",
            10073432: "TT Cup",
            10073465: "TT Elite Series",
        }

    def get_random_upcoming_matches(self, matches_per_league=1):
        """Obtém jogos futuros aleatórios de cada liga"""
        conn = sqlite3.connect(self.tm_db_path)

        upcoming_matches = []

        for league_id, league_name in self.leagues.items():
            query = """
            SELECT id, league_name, home_team, away_team, time 
            FROM events 
            WHERE time_status = 0 AND league_id = ?
            ORDER BY RANDOM()
            LIMIT ?
            """

            df = pd.read_sql_query(query, conn, params=(league_id, matches_per_league))

            for _, row in df.iterrows():
                match = {
                    "event_id": row["id"],
                    "league_name": row["league_name"],
                    "home_team": row["home_team"],
                    "away_team": row["away_team"],
                    "event_time": datetime.fromtimestamp(row["time"]),
                }
                upcoming_matches.append(match)

        conn.close()
        return upcoming_matches

    def get_match_odds(self, event_id):
        """Obtém as odds para um evento específico"""
        conn = sqlite3.connect(self.tm_db_path)

        query = """
        SELECT market_type, selection, odds, handicap_value
        FROM match_odds 
        WHERE event_id = ? 
        AND market_type IN ('To Win', 'Total')
        ORDER BY 
            CASE WHEN selection = 'Home' THEN 1
                 WHEN selection = 'Away' THEN 2
                 WHEN selection LIKE 'Over%' THEN 3
                 WHEN selection LIKE 'Under%' THEN 4
                 ELSE 5 END
        """

        df = pd.read_sql_query(query, conn, params=(event_id,))
        conn.close()

        return df

    def get_player_last_10_matches(self, player_name):
        """Busca os últimos 10 jogos de um jogador"""
        conn = sqlite3.connect(self.results_db_path)

        query = """
        SELECT 
            event_id, event_time, home_name, away_name, score
        FROM events 
        WHERE (home_name = ? OR away_name = ?) 
        ORDER BY event_time DESC 
        LIMIT 10
        """

        df = pd.read_sql_query(query, conn, params=(player_name, player_name))
        conn.close()

        return df

    def get_head_to_head_stats(self, player1, player2):
        """Obtém estatísticas de confrontos diretos entre dois jogadores"""
        conn = sqlite3.connect(self.results_db_path)

        query = """
        SELECT 
            event_id, home_name, away_name, score
        FROM events 
        WHERE ((home_name = ? AND away_name = ?) 
               OR (home_name = ? AND away_name = ?))
        AND time_status = 3
        ORDER BY event_time DESC
        """

        df = pd.read_sql_query(query, conn, params=(player1, player2, player2, player1))
        conn.close()

        if df.empty:
            return {
                "total_matches": 0,
                "player1_wins": 0,
                "player2_wins": 0,
                "win_rate_player1": 0,
            }

        player1_wins = 0
        player2_wins = 0

        for _, row in df.iterrows():
            score = row["score"]
            if score and "-" in score:
                try:
                    home_score, away_score = map(int, score.split("-"))

                    if row["home_name"] == player1:
                        if home_score > away_score:
                            player1_wins += 1
                        else:
                            player2_wins += 1
                    else:
                        if away_score > home_score:
                            player1_wins += 1
                        else:
                            player2_wins += 1
                except ValueError:
                    continue

        total_matches = len(df)
        win_rate_player1 = player1_wins / total_matches if total_matches > 0 else 0

        return {
            "total_matches": total_matches,
            "player1_wins": player1_wins,
            "player2_wins": player2_wins,
            "win_rate_player1": win_rate_player1,
        }

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

    def calculate_over_under_stats(self, games_list, line):
        """Calcula estatísticas de over/under para uma lista de games"""
        over_count = sum(1 for games in games_list if games > line)
        under_count = len(games_list) - over_count
        over_percentage = (over_count / len(games_list)) * 100 if games_list else 0

        return {
            "over_count": over_count,
            "under_count": under_count,
            "over_percentage": over_percentage,
            "total_matches": len(games_list),
        }

    def calculate_player_stats(self, player_name, matches_df):
        """Calcula estatísticas detalhadas para um jogador"""
        if matches_df.empty:
            return {
                "total_matches": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0,
                "total_games_played": 0,
                "avg_games_per_match": 0,
                "games_per_match_list": [],
            }

        stats = {
            "total_matches": len(matches_df),
            "wins": 0,
            "losses": 0,
            "total_games_played": 0,
            "games_per_match_list": [],
        }

        for _, match in matches_df.iterrows():
            # Determinar se o jogador é home ou away
            is_home = match["home_name"] == player_name

            # Obter scores detalhados por set
            detailed_scores = self.get_detailed_scores(match["event_id"])

            total_games = 0

            # Calcular total de games
            for _, set_score in detailed_scores.iterrows():
                home_score = set_score["home_score"]
                away_score = set_score["away_score"]
                total_games += home_score + away_score

            # Adicionar à lista de games por partida
            stats["games_per_match_list"].append(total_games)

            # Adicionar ao total de games
            stats["total_games_played"] += total_games

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

        return stats

    def calculate_implied_probability(self, odds):
        """Calcula a probabilidade implícita a partir das odds"""
        if odds <= 1:
            return 0
        return 1 / odds

    def calculate_estimated_roi(self, estimated_prob, odds):
        """Calcula o ROI estimado para uma aposta"""
        if estimated_prob <= 0 or odds <= 1:
            return 0

        # ROI estimado = (Probabilidade Estimada × (Odds - 1)) - (1 - Probabilidade Estimada)
        roi = (estimated_prob * (odds - 1)) - (1 - estimated_prob)
        return roi * 100  # Convertendo para porcentagem

    def analyze_bet_value(self, match, odds_df):
        """Analisa o valor das apostas com base nas estatísticas"""
        home_player = match["home_team"]
        away_player = match["away_team"]

        print(
            f"\n{Fore.CYAN}🔍 ANALISANDO JOGO: {home_player} vs {away_player}{Style.RESET_ALL}"
        )
        print(f"{Fore.CYAN}   Liga: {match['league_name']}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}   Event ID: {match['event_id']}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}   Home: {home_player}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}   Away: {away_player}{Style.RESET_ALL}")

        # Buscar estatísticas dos jogadores
        print(
            f"{Fore.BLUE}📊 Buscando estatísticas de {home_player}...{Style.RESET_ALL}"
        )
        home_matches = self.get_player_last_10_matches(home_player)
        home_stats = self.calculate_player_stats(home_player, home_matches)

        print(
            f"{Fore.BLUE}📊 Buscando estatísticas de {away_player}...{Style.RESET_ALL}"
        )
        away_matches = self.get_player_last_10_matches(away_player)
        away_stats = self.calculate_player_stats(away_player, away_matches)

        # Buscar estatísticas de confronto direto
        print(
            f"{Fore.BLUE}🤼 Buscando confrontos diretos entre {home_player} e {away_player}...{Style.RESET_ALL}"
        )
        h2h_stats = self.get_head_to_head_stats(home_player, away_player)

        # Exibir estatísticas
        print(
            f"\n{Fore.GREEN}📈 ESTATÍSTICAS DE {home_player} (HOME):{Style.RESET_ALL}"
        )
        print(
            f"   Vitórias: {home_stats['wins']}/{home_stats['total_matches']} ({home_stats['win_rate']:.1f}%)"
        )
        print(f"   Média de games por partida: {home_stats['avg_games_per_match']:.1f}")
        print(f"   Últimos 10 totais de games: {home_stats['games_per_match_list']}")

        print(
            f"\n{Fore.GREEN}📈 ESTATÍSTICAS DE {away_player} (AWAY):{Style.RESET_ALL}"
        )
        print(
            f"   Vitórias: {away_stats['wins']}/{away_stats['total_matches']} ({away_stats['win_rate']:.1f}%)"
        )
        print(f"   Média de games por partida: {away_stats['avg_games_per_match']:.1f}")
        print(f"   Últimos 10 totais de games: {away_stats['games_per_match_list']}")

        # Exibir estatísticas de confronto direto
        if h2h_stats["total_matches"] > 0:
            print(f"\n{Fore.MAGENTA}🤼 CONFRONTOS DIRETOS:{Style.RESET_ALL}")
            print(f"   Total de jogos: {h2h_stats['total_matches']}")
            print(f"   Vitórias de {home_player}: {h2h_stats['player1_wins']}")
            print(f"   Vitórias de {away_player}: {h2h_stats['player2_wins']}")
            print(
                f"   Taxa de vitória de {home_player}: {h2h_stats['win_rate_player1'] * 100:.1f}%"
            )

        # Analisar odds
        print(f"\n{Fore.YELLOW}🎯 ODDS DISPONÍVEIS:{Style.RESET_ALL}")
        for _, odd in odds_df.iterrows():
            print(
                f"   {odd['market_type']} - {odd['selection']} {odd['handicap_value']}: {odd['odds']}"
            )

        # Analisar valor das apostas
        valuable_bets = []

        for _, odd in odds_df.iterrows():
            market = odd["market_type"]
            selection = odd["selection"]
            odds_value = odd["odds"]
            handicap = odd["handicap_value"]

            # Extrair valor numérico do handicap para mercados Total
            handicap_value = None
            if market == "Total" and handicap:
                try:
                    # Remover "O " ou "U " e converter para float
                    handicap_value = float(handicap.replace("O ", "").replace("U ", ""))
                except ValueError:
                    handicap_value = None

            # Exibir informações específicas para mercados Total
            if market == "Total" and handicap_value is not None:
                print(
                    f"\n{Fore.MAGENTA}📊 ANALISANDO LINHA: {handicap} ({handicap_value}){Style.RESET_ALL}"
                )

                # Calcular estatísticas de over/under para cada jogador
                home_ou_stats = self.calculate_over_under_stats(
                    home_stats["games_per_match_list"], handicap_value
                )
                away_ou_stats = self.calculate_over_under_stats(
                    away_stats["games_per_match_list"], handicap_value
                )

                print(f"{Fore.GREEN}   {home_player} (HOME):{Style.RESET_ALL}")
                print(
                    f"      OVER {handicap_value}: {home_ou_stats['over_count']}/10 ({home_ou_stats['over_percentage']:.1f}%)"
                )
                print(
                    f"      UNDER {handicap_value}: {home_ou_stats['under_count']}/10"
                )

                print(f"{Fore.GREEN}   {away_player} (AWAY):{Style.RESET_ALL}")
                print(
                    f"      OVER {handicap_value}: {away_ou_stats['over_count']}/10 ({away_ou_stats['over_percentage']:.1f}%)"
                )
                print(
                    f"      UNDER {handicap_value}: {away_ou_stats['under_count']}/10"
                )

                # Calcular estatísticas combinadas
                all_games = (
                    home_stats["games_per_match_list"]
                    + away_stats["games_per_match_list"]
                )
                combined_ou_stats = self.calculate_over_under_stats(
                    all_games, handicap_value
                )
                print(f"{Fore.GREEN}   COMBINADO:{Style.RESET_ALL}")
                print(
                    f"      OVER {handicap_value}: {combined_ou_stats['over_count']}/20 ({combined_ou_stats['over_percentage']:.1f}%)"
                )
                print(
                    f"      UNDER {handicap_value}: {combined_ou_stats['under_count']}/20"
                )

            if market == "To Win":
                # Calcular probabilidade estimada considerando confrontos diretos
                if selection == "Home":
                    base_prob = home_stats["win_rate"] / 100

                    # Ajustar probabilidade com base nos confrontos diretos
                    if h2h_stats["total_matches"] > 0:
                        # Ponderação: 70% performance geral, 30% confrontos diretos
                        adjusted_prob = (0.7 * base_prob) + (
                            0.3 * h2h_stats["win_rate_player1"]
                        )
                    else:
                        adjusted_prob = base_prob

                    # Probabilidade implícita das odds
                    impl_prob = self.calculate_implied_probability(odds_value)

                    # Calcular edge e ROI estimado
                    edge = adjusted_prob - impl_prob
                    estimated_roi = self.calculate_estimated_roi(
                        adjusted_prob, odds_value
                    )

                    if estimated_roi > 5:  # ROI estimado de pelo menos 5%
                        valuable_bets.append(
                            {
                                "market": market,
                                "selection": selection,
                                "odds": odds_value,
                                "edge": edge,
                                "estimated_roi": estimated_roi,
                                "estimated_prob": adjusted_prob,
                                "implied_prob": impl_prob,
                            }
                        )
                        print(
                            f"{Fore.GREEN}   ✅ VALOR ENCONTRADO: {selection} @ {odds_value} (Edge: {edge:.3f}, ROI: {estimated_roi:.1f}%){Style.RESET_ALL}"
                        )
                    else:
                        print(
                            f"{Fore.RED}   ❌ Sem valor: {selection} @ {odds_value} (Edge: {edge:.3f}, ROI: {estimated_roi:.1f}%){Style.RESET_ALL}"
                        )

                elif selection == "Away":
                    base_prob = away_stats["win_rate"] / 100

                    # Ajustar probabilidade com base nos confrontos diretos
                    if h2h_stats["total_matches"] > 0:
                        # Para o away, usamos 1 - win_rate_player1 (que é do home)
                        h2h_prob_away = 1 - h2h_stats["win_rate_player1"]
                        adjusted_prob = (0.7 * base_prob) + (0.3 * h2h_prob_away)
                    else:
                        adjusted_prob = base_prob

                    # Probabilidade implícita das odds
                    impl_prob = self.calculate_implied_probability(odds_value)

                    # Calcular edge e ROI estimado
                    edge = adjusted_prob - impl_prob
                    estimated_roi = self.calculate_estimated_roi(
                        adjusted_prob, odds_value
                    )

                    if estimated_roi > 5:  # ROI estimado de pelo menos 5%
                        valuable_bets.append(
                            {
                                "market": market,
                                "selection": selection,
                                "odds": odds_value,
                                "edge": edge,
                                "estimated_roi": estimated_roi,
                                "estimated_prob": adjusted_prob,
                                "implied_prob": impl_prob,
                            }
                        )
                        print(
                            f"{Fore.GREEN}   ✅ VALOR ENCONTRADO: {selection} @ {odds_value} (Edge: {edge:.3f}, ROI: {estimated_roi:.1f}%){Style.RESET_ALL}"
                        )
                    else:
                        print(
                            f"{Fore.RED}   ❌ Sem valor: {selection} @ {odds_value} (Edge: {edge:.3f}, ROI: {estimated_roi:.1f}%){Style.RESET_ALL}"
                        )

            elif market == "Total" and handicap_value is not None:
                # Estimar probabilidade baseada em estatísticas combinadas
                all_games = (
                    home_stats["games_per_match_list"]
                    + away_stats["games_per_match_list"]
                )

                if "Over" in selection:
                    # Estimar probabilidade de Over
                    over_count = sum(1 for games in all_games if games > handicap_value)
                    total_games = len(all_games)
                    est_prob = over_count / total_games if total_games > 0 else 0

                    # Probabilidade implícita das odds
                    impl_prob = self.calculate_implied_probability(odds_value)

                    # Calcular edge e ROI estimado
                    edge = est_prob - impl_prob
                    estimated_roi = self.calculate_estimated_roi(est_prob, odds_value)

                    if estimated_roi > 5:  # ROI estimado de pelo menos 5%
                        valuable_bets.append(
                            {
                                "market": market,
                                "selection": selection,
                                "handicap": handicap,
                                "odds": odds_value,
                                "edge": edge,
                                "estimated_roi": estimated_roi,
                                "estimated_prob": est_prob,
                                "implied_prob": impl_prob,
                            }
                        )
                        print(
                            f"{Fore.GREEN}   ✅ VALOR ENCONTRADO: {selection} {handicap} @ {odds_value} (Edge: {edge:.3f}, ROI: {estimated_roi:.1f}%){Style.RESET_ALL}"
                        )
                    else:
                        print(
                            f"{Fore.RED}   ❌ Sem valor: {selection} {handicap} @ {odds_value} (Edge: {edge:.3f}, ROI: {estimated_roi:.1f}%){Style.RESET_ALL}"
                        )

                elif "Under" in selection:
                    # Estimar probabilidade de Under
                    under_count = sum(
                        1 for games in all_games if games < handicap_value
                    )
                    total_games = len(all_games)
                    est_prob = under_count / total_games if total_games > 0 else 0

                    # Probabilidade implícita das odds
                    impl_prob = self.calculate_implied_probability(odds_value)

                    # Calcular edge e ROI estimado
                    edge = est_prob - impl_prob
                    estimated_roi = self.calculate_estimated_roi(est_prob, odds_value)

                    if estimated_roi > 5:  # ROI estimado de pelo menos 5%
                        valuable_bets.append(
                            {
                                "market": market,
                                "selection": selection,
                                "handicap": handicap,
                                "odds": odds_value,
                                "edge": edge,
                                "estimated_roi": estimated_roi,
                                "estimated_prob": est_prob,
                                "implied_prob": impl_prob,
                            }
                        )
                        print(
                            f"{Fore.GREEN}   ✅ VALOR ENCONTRADO: {selection} {handicap} @ {odds_value} (Edge: {edge:.3f}, ROI: {estimated_roi:.1f}%){Style.RESET_ALL}"
                        )
                    else:
                        print(
                            f"{Fore.RED}   ❌ Sem valor: {selection} {handicap} @ {odds_value} (Edge: {edge:.3f}, ROI: {estimated_roi:.1f}%){Style.RESET_ALL}"
                        )

        return valuable_bets

    def analyze_all_leagues(self):
        """Analisa um jogo aleatório de cada liga"""
        print(
            f"{Fore.MAGENTA}======================================================================{Style.RESET_ALL}"
        )
        print(
            f"{Fore.MAGENTA}🎯 ANALISADOR DE VALOR DE APOSTAS - TÊNIS DE MESA{Style.RESET_ALL}"
        )
        print(
            f"{Fore.MAGENTA}======================================================================{Style.RESET_ALL}"
        )

        # Obter um jogo aleatório de cada liga
        upcoming_matches = self.get_random_upcoming_matches(matches_per_league=1)

        all_valuable_bets = []

        for match in upcoming_matches:
            # Obter odds para este jogo
            odds_df = self.get_match_odds(match["event_id"])

            if odds_df.empty:
                print(
                    f"{Fore.YELLOW}⚠️  Nenhuma odd encontrada para o jogo {match['home_team']} vs {match['away_team']}{Style.RESET_ALL}"
                )
                continue

            # Analisar valor das apostas
            valuable_bets = self.analyze_bet_value(match, odds_df)
            all_valuable_bets.extend(valuable_bets)

        # Resumo final
        print(
            f"\n{Fore.MAGENTA}======================================================================{Style.RESET_ALL}"
        )
        print(f"{Fore.MAGENTA}📊 RESUMO FINAL{Style.RESET_ALL}")
        print(
            f"{Fore.MAGENTA}======================================================================{Style.RESET_ALL}"
        )

        if all_valuable_bets:
            print(
                f"{Fore.GREEN}✅ {len(all_valuable_bets)} APOSTAS COM VALOR ENCONTRADAS:{Style.RESET_ALL}"
            )
            for bet in all_valuable_bets:
                if bet["market"] == "To Win":
                    print(
                        f"   {bet['market']} - {bet['selection']} @ {bet['odds']} (Edge: {bet['edge']:.3f}, ROI: {bet['estimated_roi']:.1f}%)"
                    )
                else:
                    print(
                        f"   {bet['market']} - {bet['selection']} {bet['handicap']} @ {bet['odds']} (Edge: {bet['edge']:.3f}, ROI: {bet['estimated_roi']:.1f}%)"
                    )
        else:
            print(f"{Fore.RED}❌ Nenhuma aposta com valor encontrada{Style.RESET_ALL}")

        return all_valuable_bets


def main():
    # Inicializar analisador
    analyzer = EnhancedBetValueAnalyzer()

    # Executar análise
    analyzer.analyze_all_leagues()


if __name__ == "__main__":
    main()
