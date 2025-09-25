import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, date
import logging
from colorama import Fore, Style, init

init(autoreset=True)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("bet_processor")


class BetProcessor:
    def __init__(self, tm_db_path="tm_data.db", bets_db_path="bets.db"):
        self.tm_db_path = tm_db_path
        self.bets_db_path = bets_db_path
        self.leagues = {
            10048210: "Czech Liga Pro",
            10068516: "Challenger Series TT",
            10073432: "TT Cup",
            10073465: "TT Elite Series",
        }
        self.init_bets_db()

    def init_bets_db(self):
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER NOT NULL,
            league_name TEXT NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            event_time TIMESTAMP NOT NULL,
            bet_type TEXT NOT NULL,
            selection TEXT NOT NULL,
            handicap REAL,
            odds REAL NOT NULL,
            fair_odds REAL NOT NULL,
            estimated_roi REAL NOT NULL,
            result INTEGER,
            profit REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            actual_result TEXT,
            UNIQUE(event_id, bet_type, selection, handicap)
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_events (
            event_id INTEGER PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS update_bets_timestamp
        AFTER UPDATE ON bets
        FOR EACH ROW
        BEGIN
            UPDATE bets SET updated_at = CURRENT_TIMESTAMP
            WHERE id = OLD.id;
        END;
        """)

        conn.commit()
        conn.close()

    def get_processed_event_ids(self):
        """Busca IDs de eventos já processados"""
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT event_id FROM processed_events")
        processed_ids = {row[0] for row in cursor.fetchall()}

        conn.close()
        return processed_ids

    def mark_event_processed(self, event_id):
        """Marca um evento como processado"""
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        try:
            cursor.execute(
                "INSERT OR IGNORE INTO processed_events (event_id) VALUES (?)",
                (event_id,),
            )
            conn.commit()
        except Exception as e:
            logger.error(f"Erro ao marcar evento {event_id}: {e}")

        conn.close()

    def get_all_upcoming_matches(self):
        conn = sqlite3.connect(self.tm_db_path)
        today = date.today()
        processed_events = self.get_processed_event_ids()

        upcoming_matches = []

        for league_id, league_name in self.leagues.items():
            if processed_events:
                placeholders = ",".join("?" * len(processed_events))
                query = f"""
                SELECT id, league_name, home_team, away_team, time 
                FROM events 
                WHERE time_status = 0 AND league_id = ?
                AND date(datetime(time, 'unixepoch')) >= date(?)
                AND id NOT IN ({placeholders})
                """
                params = [league_id, today] + list(processed_events)
            else:
                query = """
                SELECT id, league_name, home_team, away_team, time 
                FROM events 
                WHERE time_status = 0 AND league_id = ?
                AND date(datetime(time, 'unixepoch')) >= date(?)
                """
                params = [league_id, today]

            df = pd.read_sql_query(query, conn, params=params)

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
        conn = sqlite3.connect("table_tennis_results.db")

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
        conn = sqlite3.connect("table_tennis_results.db")

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
        conn = sqlite3.connect("table_tennis_results.db")

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
            is_home = match["home_name"] == player_name

            detailed_scores = self.get_detailed_scores(match["event_id"])

            total_games = 0

            for _, set_score in detailed_scores.iterrows():
                home_score = set_score["home_score"]
                away_score = set_score["away_score"]
                total_games += home_score + away_score

            stats["games_per_match_list"].append(total_games)
            stats["total_games_played"] += total_games

            score = match["score"]
            if score and "-" in score:
                try:
                    home_score, away_score = map(int, score.split("-"))

                    if (is_home and home_score > away_score) or (
                        not is_home and away_score > home_score
                    ):
                        stats["wins"] += 1
                    else:
                        stats["losses"] += 1
                except ValueError:
                    pass

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
        if odds <= 1:
            return 0
        return 1 / odds

    def calculate_estimated_roi(self, estimated_prob, odds):
        if estimated_prob <= 0 or odds <= 1:
            return 0

        roi = (estimated_prob * (odds - 1)) - (1 - estimated_prob)
        return roi * 100

    def filter_conflicting_bets(self, valuable_bets, home_player, away_player):
        home_bets = [
            b
            for b in valuable_bets
            if b["bet_type"] == "To Win" and b["selection"] == "Home"
        ]
        away_bets = [
            b
            for b in valuable_bets
            if b["bet_type"] == "To Win" and b["selection"] == "Away"
        ]
        other_bets = [b for b in valuable_bets if b["bet_type"] != "To Win"]

        if not home_bets or not away_bets:
            return valuable_bets

        h2h_stats = self.get_head_to_head_stats(home_player, away_player)

        if h2h_stats["total_matches"] > 0:
            if h2h_stats["player1_wins"] > h2h_stats["player2_wins"]:
                logger.info(
                    f"H2H: {home_player} tem mais vitórias ({h2h_stats['player1_wins']} vs {h2h_stats['player2_wins']})"
                )
                return home_bets + other_bets
            else:
                logger.info(
                    f"H2H: {away_player} tem mais vitórias ({h2h_stats['player2_wins']} vs {h2h_stats['player1_wins']})"
                )
                return away_bets + other_bets
        else:
            home_roi = max(b["estimated_roi"] for b in home_bets)
            away_roi = max(b["estimated_roi"] for b in away_bets)

            if home_roi > away_roi:
                logger.info(
                    f"ROI: Home tem maior ROI ({home_roi:.2f}% vs {away_roi:.2f}%)"
                )
                return home_bets + other_bets
            else:
                logger.info(
                    f"ROI: Away tem maior ROI ({away_roi:.2f}% vs {home_roi:.2f}%)"
                )
                return away_bets + other_bets

    def analyze_bet_value(self, match, odds_df):
        home_player = match["home_team"]
        away_player = match["away_team"]

        home_matches = self.get_player_last_10_matches(home_player)
        home_stats = self.calculate_player_stats(home_player, home_matches)

        away_matches = self.get_player_last_10_matches(away_player)
        away_stats = self.calculate_player_stats(away_player, away_matches)

        h2h_stats = self.get_head_to_head_stats(home_player, away_player)

        # FILTRO: Mínimo 15 partidas para ambos jogadores
        if home_stats["total_matches"] < 15 or away_stats["total_matches"] < 15:
            logger.warning(f"❌ DESCARTADO - Dados insuficientes: {home_player}({home_stats['total_matches']}) vs {away_player}({away_stats['total_matches']}) - Mínimo 15 partidas")
            return []

        valuable_bets = []

        for _, odd in odds_df.iterrows():
            market = odd["market_type"]
            selection = odd["selection"]
            odds_value = odd["odds"]
            handicap = odd["handicap_value"]

            handicap_value = None
            if market == "Total" and handicap:
                try:
                    handicap_value = float(handicap.replace("O ", "").replace("U ", ""))
                except ValueError:
                    handicap_value = None

            if market == "To Win":
                if selection == "Home":
                    base_prob = home_stats["win_rate"] / 100

                    if h2h_stats["total_matches"] > 0:
                        adjusted_prob = (0.7 * base_prob) + (0.3 * h2h_stats["win_rate_player1"])
                    else:
                        adjusted_prob = base_prob

                    estimated_roi = self.calculate_estimated_roi(adjusted_prob, odds_value)

                    log_message = (
                        f"To Win - Home: {home_player} vs {away_player} | "
                        f"Odds: {odds_value:.2f} | "
                        f"Base Prob: {base_prob:.3f} | "
                        f"Adj Prob: {adjusted_prob:.3f} | "
                        f"ROI: {estimated_roi:.2f}%"
                    )
                    
                    # ML: ROI >= 30%
                    if estimated_roi >= 30:
                        logger.info(Fore.GREEN + f"✅ APROVADA: {log_message}")
                        valuable_bets.append({
                            "event_id": match["event_id"],
                            "league_name": match["league_name"],
                            "home_team": home_player,
                            "away_team": away_player,
                            "event_time": match["event_time"],
                            "bet_type": market,
                            "selection": selection,
                            "handicap": None,
                            "odds": odds_value,
                            "fair_odds": 1 / adjusted_prob if adjusted_prob > 0 else 0,
                            "estimated_roi": estimated_roi,
                        })
                    else:
                        logger.info(Fore.RED + f"❌ REJEITADA: {log_message}")

                elif selection == "Away":
                    base_prob = away_stats["win_rate"] / 100

                    if h2h_stats["total_matches"] > 0:
                        h2h_prob_away = 1 - h2h_stats["win_rate_player1"]
                        adjusted_prob = (0.7 * base_prob) + (0.3 * h2h_prob_away)
                    else:
                        adjusted_prob = base_prob

                    estimated_roi = self.calculate_estimated_roi(adjusted_prob, odds_value)

                    log_message = (
                        f"To Win - Away: {home_player} vs {away_player} | "
                        f"Odds: {odds_value:.2f} | "
                        f"Base Prob: {base_prob:.3f} | "
                        f"Adj Prob: {adjusted_prob:.3f} | "
                        f"ROI: {estimated_roi:.2f}%"
                    )
                    
                    # ML: ROI >= 30%
                    if estimated_roi >= 30:
                        logger.info(Fore.GREEN + f"✅ APROVADA: {log_message}")
                        valuable_bets.append({
                            "event_id": match["event_id"],
                            "league_name": match["league_name"],
                            "home_team": home_player,
                            "away_team": away_player,
                            "event_time": match["event_time"],
                            "bet_type": market,
                            "selection": selection,
                            "handicap": None,
                            "odds": odds_value,
                            "fair_odds": 1 / adjusted_prob if adjusted_prob > 0 else 0,
                            "estimated_roi": estimated_roi,
                        })
                    else:
                        logger.info(Fore.RED + f"❌ REJEITADA: {log_message}")

            elif market == "Total" and handicap_value is not None:
                all_games = (
                    home_stats["games_per_match_list"]
                    + away_stats["games_per_match_list"]
                )

                if "Over" in selection:
                    over_count = sum(1 for games in all_games if games > handicap_value)
                    total_games = len(all_games)
                    est_prob = over_count / total_games if total_games > 0 else 0

                    estimated_roi = self.calculate_estimated_roi(est_prob, odds_value)

                    log_message = (
                        f"Total - {selection} {handicap_value}: {home_player} vs {away_player} | "
                        f"Odds: {odds_value:.2f} | "
                        f"Est Prob: {est_prob:.3f} | "
                        f"ROI: {estimated_roi:.2f}% | "
                        f"Sample: {len(all_games)} matches"
                    )
                    
                    # Over/Under: ROI >= 20%
                    if estimated_roi >= 20:
                        logger.info(Fore.GREEN + f"✅ APROVADA: {log_message}")
                        valuable_bets.append({
                            "event_id": match["event_id"],
                            "league_name": match["league_name"],
                            "home_team": home_player,
                            "away_team": away_player,
                            "event_time": match["event_time"],
                            "bet_type": market,
                            "selection": selection,
                            "handicap": handicap_value,
                            "odds": odds_value,
                            "fair_odds": 1 / est_prob if est_prob > 0 else 0,
                            "estimated_roi": estimated_roi,
                        })
                    else:
                        logger.info(Fore.RED + f"❌ REJEITADA: {log_message}")

                elif "Under" in selection:
                    under_count = sum(1 for games in all_games if games < handicap_value)
                    total_games = len(all_games)
                    est_prob = under_count / total_games if total_games > 0 else 0

                    estimated_roi = self.calculate_estimated_roi(est_prob, odds_value)

                    log_message = (
                        f"Total - {selection} {handicap_value}: {home_player} vs {away_player} | "
                        f"Odds: {odds_value:.2f} | "
                        f"Est Prob: {est_prob:.3f} | "
                        f"ROI: {estimated_roi:.2f}% | "
                        f"Sample: {len(all_games)} matches"
                    )
                    
                    # Over/Under: ROI >= 20%
                    if estimated_roi >= 20:
                        logger.info(Fore.GREEN + f"✅ APROVADA: {log_message}")
                        valuable_bets.append({
                            "event_id": match["event_id"],
                            "league_name": match["league_name"],
                            "home_team": home_player,
                            "away_team": away_player,
                            "event_time": match["event_time"],
                            "bet_type": market,
                            "selection": selection,
                            "handicap": handicap_value,
                            "odds": odds_value,
                            "fair_odds": 1 / est_prob if est_prob > 0 else 0,
                            "estimated_roi": estimated_roi,
                        })
                    else:
                        logger.info(Fore.RED + f"❌ REJEITADA: {log_message}")

        return valuable_bets

    def save_top_bets_by_league(self, all_bets):
        """Salva apenas as 20 melhores apostas de ML e 20 melhores de Under/Over por liga por dia"""
        if not all_bets:
            return 0

        # Agrupar apostas por liga e data do evento
        bets_by_league_date = {}
        for bet in all_bets:
            event_date = bet["event_time"].date()  # Extrai a data do evento
            league = bet["league_name"]
            key = (league, event_date)

            if key not in bets_by_league_date:
                bets_by_league_date[key] = []
            bets_by_league_date[key].append(bet)

        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        total_saved = 0

        for (league_name, event_date), league_bets in bets_by_league_date.items():
            # Separar por tipo
            ml_bets = [b for b in league_bets if b["bet_type"] == "To Win"]
            ou_bets = [b for b in league_bets if b["bet_type"] == "Total"]

            # Ordenar cada tipo por ROI decrescente
            ml_bets.sort(key=lambda x: x["estimated_roi"], reverse=True)
            ou_bets.sort(key=lambda x: x["estimated_roi"], reverse=True)

            # Pegar apenas top 20 de cada tipo
            top_ml = ml_bets[:20]
            top_ou = ou_bets[:20]

            top_bets = top_ml + top_ou

            logger.info(
                f"Liga {league_name} - {event_date}: {len(ml_bets)} ML candidatas → {len(top_ml)} salvando"
            )
            logger.info(
                f"Liga {league_name} - {event_date}: {len(ou_bets)} O/U candidatas → {len(top_ou)} salvando"
            )

            for bet in top_bets:
                try:
                    cursor.execute(
                        """
                    INSERT OR REPLACE INTO bets 
                    (event_id, league_name, home_team, away_team, event_time, 
                     bet_type, selection, handicap, odds, fair_odds, estimated_roi)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            bet["event_id"],
                            bet["league_name"],
                            bet["home_team"],
                            bet["away_team"],
                            bet["event_time"],
                            bet["bet_type"],
                            bet["selection"],
                            bet["handicap"],
                            bet["odds"],
                            bet["fair_odds"],
                            bet["estimated_roi"],
                        ),
                    )
                    if cursor.rowcount > 0:
                        total_saved += 1
                        logger.info(Fore.GREEN + f"💾 SALVA: {bet['bet_type']} {bet['selection']} @ {bet['odds']:.2f} (ROI: {bet['estimated_roi']:.2f}%)")
                except sqlite3.Error as e:
                    logger.error(f"Erro ao salvar aposta: {e}")

        conn.commit()
        conn.close()
        return total_saved

    def process_all_matches(self):
        logger.info("Iniciando processamento de jogos...")

        upcoming_matches = self.get_all_upcoming_matches()
        logger.info(f"Jogos não processados para analisar: {len(upcoming_matches)}")

        if not upcoming_matches:
            logger.info("Nenhum jogo novo para processar.")
            return

        all_valuable_bets = []
        processed_events = []

        for match in upcoming_matches:
            try:
                event_id = match["event_id"]
                logger.info(f"Analisando evento {event_id}: {match['home_team']} vs {match['away_team']}")
                
                odds_df = self.get_match_odds(event_id)
                logger.info(f"Encontradas {len(odds_df)} odds para este evento")

                if not odds_df.empty:
                    valuable_bets = self.analyze_bet_value(match, odds_df)
                    if valuable_bets:
                        logger.info(f"Encontradas {len(valuable_bets)} apostas valiosas neste evento")
                        all_valuable_bets.extend(valuable_bets)
                    else:
                        logger.info("Nenhuma aposta valiosa encontrada neste evento")
                else:
                    logger.warning("Nenhuma odd disponível para este evento")

                # Marcar evento como processado (mesmo que não tenha odds valiosas)
                self.mark_event_processed(event_id)
                processed_events.append(event_id)

            except Exception as e:
                logger.error(f"Erro ao processar evento {match['event_id']}: {e}")
                # Marcar como processado mesmo com erro para não reprocessar
                self.mark_event_processed(match["event_id"])

        # Salvar apenas as top 20 ML + top 20 O/U por liga por dia
        total_saved = self.save_top_bets_by_league(all_valuable_bets)

        logger.info(f"Processamento concluído.")
        logger.info(f"Eventos processados: {len(processed_events)}")
        logger.info(f"Total de apostas valiosas encontradas: {len(all_valuable_bets)}")
        logger.info(f"Total de apostas salvas: {total_saved}")

        if len(processed_events) > 0:
            logger.info("✅ Sistema pode rodar novamente para capturar novos eventos!")


def main():
    processor = BetProcessor()
    processor.process_all_matches()


if __name__ == "__main__":
    main()