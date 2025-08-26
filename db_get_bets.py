import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
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

    def get_all_upcoming_matches(self):
        conn = sqlite3.connect(self.tm_db_path)

        # Obter IDs já processados primeiro
        processed_ids = self.get_processed_event_ids()

        upcoming_matches = []

        for league_id, league_name in self.leagues.items():
            # Já filtrar eventos processados na query SQL
            if processed_ids:
                query = """
                SELECT id, league_name, home_team, away_team, time 
                FROM events 
                WHERE time_status = 0 AND league_id = ? 
                AND id NOT IN ({})
                """.format(",".join("?" * len(processed_ids)))
                params = (league_id,) + tuple(processed_ids)
            else:
                query = """
                SELECT id, league_name, home_team, away_team, time 
                FROM events 
                WHERE time_status = 0 AND league_id = ?
                """
                params = (league_id,)

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

    def get_processed_event_ids(self):
        conn = sqlite3.connect(self.bets_db_path)
        query = "SELECT event_id FROM processed_events"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return set(df["event_id"].tolist()) if not df.empty else set()

    def mark_event_as_processed(self, event_id):
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO processed_events (event_id) VALUES (?)", (event_id,)
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Evento já foi marcado como processado
            return False
        finally:
            conn.close()

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

    def analyze_bet_value(self, match, odds_df):
        home_player = match["home_team"]
        away_player = match["away_team"]

        logger.info(f"🔍 ANALISANDO JOGO: {home_player} vs {away_player}")
        logger.info(f"   Liga: {match['league_name']}")
        logger.info(f"   Event ID: {match['event_id']}")

        home_matches = self.get_player_last_10_matches(home_player)
        home_stats = self.calculate_player_stats(home_player, home_matches)

        away_matches = self.get_player_last_10_matches(away_player)
        away_stats = self.calculate_player_stats(away_player, away_matches)

        h2h_stats = self.get_head_to_head_stats(home_player, away_player)

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
                        adjusted_prob = (0.7 * base_prob) + (
                            0.3 * h2h_stats["win_rate_player1"]
                        )
                    else:
                        adjusted_prob = base_prob

                    impl_prob = self.calculate_implied_probability(odds_value)
                    edge = adjusted_prob - impl_prob
                    estimated_roi = self.calculate_estimated_roi(
                        adjusted_prob, odds_value
                    )

                    if estimated_roi >= 15:
                        valuable_bets.append(
                            {
                                "event_id": match["event_id"],
                                "league_name": match["league_name"],
                                "home_team": home_player,
                                "away_team": away_player,
                                "event_time": match["event_time"],
                                "bet_type": market,
                                "selection": selection,
                                "handicap": None,
                                "odds": odds_value,
                                "fair_odds": 1 / adjusted_prob
                                if adjusted_prob > 0
                                else 0,
                                "estimated_roi": estimated_roi,
                                "estimated_prob": adjusted_prob,
                            }
                        )

                elif selection == "Away":
                    base_prob = away_stats["win_rate"] / 100

                    if h2h_stats["total_matches"] > 0:
                        h2h_prob_away = 1 - h2h_stats["win_rate_player1"]
                        adjusted_prob = (0.7 * base_prob) + (0.3 * h2h_prob_away)
                    else:
                        adjusted_prob = base_prob

                    impl_prob = self.calculate_implied_probability(odds_value)
                    edge = adjusted_prob - impl_prob
                    estimated_roi = self.calculate_estimated_roi(
                        adjusted_prob, odds_value
                    )

                    if estimated_roi >= 15:
                        valuable_bets.append(
                            {
                                "event_id": match["event_id"],
                                "league_name": match["league_name"],
                                "home_team": home_player,
                                "away_team": away_player,
                                "event_time": match["event_time"],
                                "bet_type": market,
                                "selection": selection,
                                "handicap": None,
                                "odds": odds_value,
                                "fair_odds": 1 / adjusted_prob
                                if adjusted_prob > 0
                                else 0,
                                "estimated_roi": estimated_roi,
                                "estimated_prob": adjusted_prob,
                            }
                        )

            elif market == "Total" and handicap_value is not None:
                all_games = (
                    home_stats["games_per_match_list"]
                    + away_stats["games_per_match_list"]
                )

                if "Over" in selection:
                    over_count = sum(1 for games in all_games if games > handicap_value)
                    total_games = len(all_games)
                    est_prob = over_count / total_games if total_games > 0 else 0

                    impl_prob = self.calculate_implied_probability(odds_value)
                    edge = est_prob - impl_prob
                    estimated_roi = self.calculate_estimated_roi(est_prob, odds_value)

                    if estimated_roi >= 15:
                        valuable_bets.append(
                            {
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
                                "estimated_prob": est_prob,
                            }
                        )

                elif "Under" in selection:
                    under_count = sum(
                        1 for games in all_games if games < handicap_value
                    )
                    total_games = len(all_games)
                    est_prob = under_count / total_games if total_games > 0 else 0

                    impl_prob = self.calculate_implied_probability(odds_value)
                    edge = est_prob - impl_prob
                    estimated_roi = self.calculate_estimated_roi(est_prob, odds_value)

                    if estimated_roi >= 15:
                        valuable_bets.append(
                            {
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
                                "estimated_prob": est_prob,
                            }
                        )

        return valuable_bets

    def save_valuable_bets(self, valuable_bets):
        if not valuable_bets:
            return 0

        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        saved_count = 0
        for bet in valuable_bets:
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
                    saved_count += 1
            except sqlite3.Error as e:
                logger.error(f"Erro ao salvar aposta: {e}")

        conn.commit()
        conn.close()
        return saved_count

    def process_all_matches(self):
        logger.info("🔄 Iniciando processamento de jogos...")

        # Obter apenas jogos não processados
        unprocessed_matches = self.get_all_upcoming_matches()
        logger.info(f"📊 Jogos novos para processar: {len(unprocessed_matches)}")

        if not unprocessed_matches:
            logger.info("✅ Nenhum jogo novo para processar.")
            return

        total_valuable_bets = 0

        for match in unprocessed_matches:
            try:
                # Verificar novamente se o evento já foi processado (double-check)
                processed_ids = self.get_processed_event_ids()
                if match["event_id"] in processed_ids:
                    logger.info(
                        f"⏭️  Evento {match['event_id']} já processado, pulando..."
                    )
                    continue

                odds_df = self.get_match_odds(match["event_id"])

                if odds_df.empty:
                    logger.info(
                        f"⚠️  Sem odds para {match['home_team']} vs {match['away_team']}"
                    )
                else:
                    valuable_bets = self.analyze_bet_value(match, odds_df)

                    if valuable_bets:
                        saved_count = self.save_valuable_bets(valuable_bets)
                        total_valuable_bets += saved_count
                        logger.info(
                            f"💾 {saved_count} apostas salvas para evento {match['event_id']}"
                        )

                # Marcar como processado apenas se conseguir
                if self.mark_event_as_processed(match["event_id"]):
                    logger.info(f"✓ Evento {match['event_id']} marcado como processado")

            except Exception as e:
                logger.error(f"❌ Erro ao processar evento {match['event_id']}: {e}")
                # Ainda marca como processado para evitar loops
                self.mark_event_as_processed(match["event_id"])

        logger.info(
            f"✅ Processamento concluído. Total de apostas valiosas: {total_valuable_bets}"
        )


def main():
    processor = BetProcessor()
    processor.process_all_matches()


if __name__ == "__main__":
    main()
