import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, date
import logging
from colorama import Fore, Style, init
from collections import defaultdict

init(autoreset=True)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("bet_processor_elo")

# --- NOVOS PARÂMETROS DO MODELO ELO ---
K_FACTOR = 32
DEFAULT_ELO = 1500

# --- NOVOS PARÂMETROS PARA ESTRATÉGIA DE TOTAIS ---
ELO_DIFF_THRESHOLD_OU = (
    100  # Diferença de ELO para considerar um jogo equilibrado para O/U
)
MIN_ROI_BALANCED_OVER = 20  # ROI mínimo para Over em jogo equilibrado
MIN_ROI_UNBALANCED_OVER = 15  # ROI mínimo para Over em jogo desequilibrado
MIN_ROI_BALANCED_UNDER = 15  # ROI mínimo para Under em jogo equilibrado
MIN_ROI_UNBALANCED_UNDER = 25  # ROI mínimo para Under em jogo desequilibrado

# --- BLACKLIST DE LIGAS ---
OU_LEAGUE_BLACKLIST = ["TT Elite Series"]


class BetProcessor:
    def __init__(
        self,
        tm_db_path: str = "tm_data.db",
        bets_db_path: str = "bets.db",
        results_db_path: str = "table_tennis_results.db",
    ):
        self.tm_db_path = tm_db_path
        self.bets_db_path = bets_db_path
        self.results_db_path = results_db_path
        self.leagues = {
            10047071: "Setka Cup Women",
            10047098: "Setka Cup",
            10068516: "Challenger Series TT",
            10048210: "Czech Liga Pro",
            10073432: "TT Cup",
            10073465: "TT Elite Series",
        }
        self.init_bets_db()
        logger.info(
            "🧠 Calculando ratings ELO de todos os jogadores a partir do histórico..."
        )
        self.player_ratings = self._calculate_all_player_elos()
        logger.info(
            f"✅ Ratings ELO calculados para {len(self.player_ratings)} jogadores."
        )

    def _get_expected_score(self, rating1, rating2):
        return 1 / (1 + 10 ** ((rating2 - rating1) / 400))

    def _update_ratings(self, rating1, rating2, score1, score2):
        expected1 = self._get_expected_score(rating1, rating2)
        new_rating1 = rating1 + K_FACTOR * (score1 - expected1)
        new_rating2 = rating2 + K_FACTOR * ((1 - score1) - (1 - expected1))
        return new_rating1, new_rating2

    def _calculate_all_player_elos(self):
        conn = sqlite3.connect(self.results_db_path)
        query = """
        SELECT home_name, away_name, score
        FROM events 
        WHERE time_status = 3 AND score IS NOT NULL AND score != ''
        ORDER BY event_time ASC
        """
        df = pd.read_sql_query(query, conn)
        conn.close()

        ratings = defaultdict(lambda: DEFAULT_ELO)

        for _, row in df.iterrows():
            home_player, away_player = row["home_name"], row["away_name"]

            try:
                home_score, away_score = map(int, row["score"].split("-"))
            except ValueError:
                continue  # Ignora scores mal formatados

            s1 = 1 if home_score > away_score else 0
            r1, r2 = ratings[home_player], ratings[away_player]

            new_r1, new_r2 = self._update_ratings(r1, r2, s1, 1 - s1)
            ratings[home_player] = new_r1
            ratings[away_player] = new_r2

        return dict(ratings)

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
            home_elo_at_bet REAL,
            away_elo_at_bet REAL,
            elo_prob_home REAL,
            implied_prob REAL,
            bet_edge REAL,
            min_roi_required REAL,
            bet_decision_reason TEXT,
            player_form_home TEXT,
            player_form_away TEXT,
            h2h_summary TEXT,
            bet_timestamp TIMESTAMP,
            UNIQUE(event_id, bet_type, selection, handicap)
        )
        """)

        cursor.execute(
            "CREATE TABLE IF NOT EXISTS processed_events (event_id INTEGER PRIMARY KEY, processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.commit()
        conn.close()

    def get_processed_event_ids(self):
        conn = sqlite3.connect(self.bets_db_path)
        processed_ids = {
            row[0]
            for row in conn.cursor()
            .execute("SELECT event_id FROM processed_events")
            .fetchall()
        }
        conn.close()
        return processed_ids

    def mark_event_processed(self, event_id):
        conn = sqlite3.connect(self.bets_db_path)
        try:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR IGNORE INTO processed_events (event_id) VALUES (?)",
                (event_id,),
            )
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Erro ao marcar evento {event_id}: {e}")
        finally:
            conn.close()

    def get_all_upcoming_matches(self):
        conn = sqlite3.connect(self.tm_db_path)
        today = date.today()
        processed_events = self.get_processed_event_ids()
        upcoming_matches = []
        for league_id in self.leagues.keys():
            if processed_events:
                placeholders = ",".join("?" * len(processed_events))
                query = f"SELECT id, league_name, home_team, away_team, time FROM events WHERE time_status = 0 AND league_id = ? AND date(datetime(time, 'unixepoch')) >= date(?) AND id NOT IN ({placeholders})"
                params = [league_id, today] + list(processed_events)
            else:
                query = "SELECT id, league_name, home_team, away_team, time FROM events WHERE time_status = 0 AND league_id = ? AND date(datetime(time, 'unixepoch')) >= date(?)"
                params = [league_id, today]
            df = pd.read_sql_query(query, conn, params=params)
            for _, row in df.iterrows():
                upcoming_matches.append(
                    {
                        "event_id": row["id"],
                        "league_name": row["league_name"],
                        "home_team": row["home_team"],
                        "away_team": row["away_team"],
                        "event_time": datetime.fromtimestamp(row["time"]),
                    }
                )
        conn.close()
        return upcoming_matches

    def get_match_odds(self, event_id):
        conn = sqlite3.connect(self.tm_db_path)
        query = """SELECT market_type, selection, odds, handicap_value FROM match_odds WHERE event_id = ? AND market_type IN ('To Win', 'Total')"""
        df = pd.read_sql_query(query, conn, params=(event_id,))
        conn.close()
        return df

    def get_games_per_match_list(self, player_name, limit=20):
        conn = sqlite3.connect(self.results_db_path)
        matches_df = pd.read_sql_query(
            "SELECT event_id FROM events WHERE (home_name = ? OR away_name = ?) ORDER BY event_time DESC LIMIT ?",
            conn,
            params=(player_name, player_name, limit),
        )
        games_list = []
        for _, match in matches_df.iterrows():
            detailed_scores = pd.read_sql_query(
                "SELECT home_score, away_score FROM event_scores WHERE event_id = ?",
                conn,
                params=(match["event_id"],),
            )
            total_games = (
                detailed_scores["home_score"].sum()
                + detailed_scores["away_score"].sum()
            )
            if total_games > 0:
                games_list.append(total_games)
        conn.close()
        return games_list

    def analyze_over_under_bet_strategy(
        self,
        home_games,
        away_games,
        handicap_value,
        selection,
        odds_value,
        home_rating,
        away_rating,
        ml_odds_home,
        ml_odds_away,
    ):
        # 1. Avaliar o equilíbrio do confronto usando ELO e Odds ML
        elo_diff = abs(home_rating - away_rating)
        is_balanced_by_elo = elo_diff < ELO_DIFF_THRESHOLD_OU
        is_balanced_by_ml_odds = (
            ml_odds_home > 1.50 and ml_odds_away > 1.50
        )  # Exemplo: ambos com odds acima de 1.50

        is_confronto_parelho = is_balanced_by_elo and is_balanced_by_ml_odds

        decision_reason = ""
        min_roi_required = 0
        prob_source = ""

        # 2. Definir a estratégia e ROI mínimo com base no equilíbrio e tipo de aposta
        if "Over" in selection:
            if is_confronto_parelho:
                min_roi_required = MIN_ROI_BALANCED_OVER
                prob_source = "Over em Jogo Parelho"
            else:
                min_roi_required = MIN_ROI_UNBALANCED_OVER
                prob_source = "Over em Jogo Desequilibrado"
        elif "Under" in selection:
            if is_confronto_parelho:
                min_roi_required = MIN_ROI_BALANCED_UNDER
                prob_source = "Under em Jogo Parelho"
            else:
                min_roi_required = MIN_ROI_UNBALANCED_UNDER
                prob_source = "Under em Jogo Desequilibrado"
        else:
            decision_reason = "Rejeitada (Total): Tipo de aposta Total inválido"
            logger.info(Fore.RED + f"❌ FILTRO FINAL: {decision_reason}")
            return False, 0, 0, decision_reason

        # 3. Calcular probabilidade estimada e ROI
        if "Over" in selection:
            home_prob = (
                sum(1 for g in home_games if g > handicap_value) / len(home_games)
                if home_games
                else 0
            )
            away_prob = (
                sum(1 for g in away_games if g > handicap_value) / len(away_games)
                if away_games
                else 0
            )
        else:  # Under
            home_prob = (
                sum(1 for g in home_games if g < handicap_value) / len(home_games)
                if home_games
                else 0
            )
            away_prob = (
                sum(1 for g in away_games if g < handicap_value) / len(away_games)
                if away_games
                else 0
            )

        est_prob = (home_prob + away_prob) / 2
        roi = ((est_prob * (odds_value - 1)) - (1 - est_prob)) * 100

        # 4. Decisão final
        accept = roi >= min_roi_required

        if accept:
            decision_reason = (
                f"Aceita: {prob_source} com ROI {roi:.2f}% >= {min_roi_required}%"
            )
        else:
            decision_reason = (
                f"Rejeitada: {prob_source} com ROI {roi:.2f}% < {min_roi_required}%"
            )
            logger.info(Fore.RED + f"❌ FILTRO FINAL: {decision_reason}")

        return accept, est_prob, roi, decision_reason

    def analyze_bet_value(self, match, odds_df):
        valuable_bets = []
        home_player, away_player = match["home_team"], match["away_team"]
        home_rating = self.player_ratings.get(home_player, DEFAULT_ELO)
        away_rating = self.player_ratings.get(away_player, DEFAULT_ELO)

        # Buscar odds ML para determinar o equilíbrio do confronto
        ml_odds_home = (
            odds_df[
                (odds_df["market_type"] == "To Win") & (odds_df["selection"] == "Home")
            ]["odds"].iloc[0]
            if not odds_df[
                (odds_df["market_type"] == "To Win") & (odds_df["selection"] == "Home")
            ].empty
            else 0
        )
        ml_odds_away = (
            odds_df[
                (odds_df["market_type"] == "To Win") & (odds_df["selection"] == "Away")
            ]["odds"].iloc[0]
            if not odds_df[
                (odds_df["market_type"] == "To Win") & (odds_df["selection"] == "Away")
            ].empty
            else 0
        )

        for _, odds_row in odds_df.iterrows():
            market, selection, odds_value, handicap = (
                odds_row["market_type"],
                odds_row["selection"],
                odds_row["odds"],
                odds_row["handicap_value"],
            )

            if market != "Total" or not handicap:
                continue

            if match["league_name"] in OU_LEAGUE_BLACKLIST:
                logger.info(f"❌ Liga {match['league_name']} na blacklist. Ignorando.")
                continue

            try:
                handicap_value = float(
                    str(handicap).replace("O ", "").replace("U ", "")
                )
            except ValueError:
                logger.warning(f"Handicap inválido: {handicap}. Ignorando.")
                continue

            home_games_list = self.get_games_per_match_list(home_player)
            away_games_list = self.get_games_per_match_list(away_player)

            if not home_games_list or not away_games_list:
                logger.info(
                    f"❌ Dados históricos insuficientes para {home_player} ou {away_player}. Ignorando."
                )
                continue

            accept_bet, est_prob, estimated_roi, decision_reason = (
                self.analyze_over_under_bet_strategy(
                    home_games_list,
                    away_games_list,
                    handicap_value,
                    selection,
                    odds_value,
                    home_rating,
                    away_rating,
                    ml_odds_home,
                    ml_odds_away,
                )
            )

            if accept_bet:
                elo_diff = abs(home_rating - away_rating)
                is_balanced_by_elo = elo_diff < ELO_DIFF_THRESHOLD_OU
                is_balanced_by_ml_odds = ml_odds_home > 1.50 and ml_odds_away > 1.50
                is_confronto_parelho = is_balanced_by_elo and is_balanced_by_ml_odds

                if "Over" in selection:
                    min_roi_for_record = (
                        MIN_ROI_BALANCED_OVER
                        if is_confronto_parelho
                        else MIN_ROI_UNBALANCED_OVER
                    )
                else:  # Under
                    min_roi_for_record = (
                        MIN_ROI_BALANCED_UNDER
                        if is_confronto_parelho
                        else MIN_ROI_UNBALANCED_UNDER
                    )

                valuable_bets.append(
                    {
                        "event_id": match["event_id"],
                        "league_name": match["league_name"],
                        "home_team": home_player,
                        "away_team": away_player,
                        "event_time": match["event_time"].isoformat(),
                        "bet_type": market,
                        "selection": selection,
                        "handicap": handicap_value,
                        "odds": odds_value,
                        "fair_odds": 1 / est_prob if est_prob > 0 else 0,
                        "estimated_roi": estimated_roi,
                        "home_elo_at_bet": home_rating,
                        "away_elo_at_bet": away_rating,
                        "elo_prob_home": self._get_expected_score(
                            home_rating, away_rating
                        ),
                        "implied_prob": 1 / odds_value,
                        "bet_edge": est_prob - (1 / odds_value),
                        "min_roi_required": min_roi_for_record,
                        "bet_decision_reason": decision_reason,
                        "player_form_home": "N/A",
                        "player_form_away": "N/A",
                        "h2h_summary": "N/A",
                        "bet_timestamp": datetime.now().isoformat(),
                    }
                )
            else:
                logger.info(
                    f"🔍 Análise para {home_player} vs {away_player}: {decision_reason}"
                )

        return valuable_bets

    def save_top_bets_by_league(self, bets):
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()
        saved_count = 0
        for bet in bets:
            try:
                # Convert datetime objects to ISO format strings for SQLite TIMESTAMP compatibility
                bet["event_time"] = (
                    bet["event_time"]
                    if isinstance(bet["event_time"], str)
                    else bet["event_time"].isoformat()
                )
                bet["bet_timestamp"] = (
                    bet["bet_timestamp"]
                    if isinstance(bet["bet_timestamp"], str)
                    else bet["bet_timestamp"].isoformat()
                )

                cursor.execute(
                    "SELECT id FROM bets WHERE event_id = ? AND bet_type = ? AND selection = ? AND handicap = ?",
                    (
                        bet["event_id"],
                        bet["bet_type"],
                        bet["selection"],
                        bet["handicap"],
                    ),
                )
                exists = cursor.fetchone()
                if exists:
                    cursor.execute(
                        """
                        UPDATE bets SET 
                            odds = ?, estimated_roi = ?, bet_timestamp = ?, 
                            fair_odds = ?, home_elo_at_bet = ?, away_elo_at_bet = ?, 
                            elo_prob_home = ?, implied_prob = ?, bet_edge = ?, 
                            min_roi_required = ?, bet_decision_reason = ?, 
                            player_form_home = ?, player_form_away = ?, h2h_summary = ?
                        WHERE id = ?
                    """,
                        (
                            bet["odds"],
                            bet["estimated_roi"],
                            bet["bet_timestamp"],
                            bet["fair_odds"],
                            bet["home_elo_at_bet"],
                            bet["away_elo_at_bet"],
                            bet["elo_prob_home"],
                            bet["implied_prob"],
                            bet["bet_edge"],
                            bet["min_roi_required"],
                            bet["bet_decision_reason"],
                            bet["player_form_home"],
                            bet["player_form_away"],
                            bet["h2h_summary"],
                            exists[0],
                        ),
                    )
                else:
                    columns = ", ".join(bet.keys())
                    placeholders = ", ".join("?" * len(bet))
                    cursor.execute(
                        f"INSERT INTO bets ({columns}) VALUES ({placeholders})",
                        list(bet.values()),
                    )
                saved_count += 1
            except sqlite3.IntegrityError:
                logger.warning(
                    f"Aposta para evento {bet['event_id']} já existe e não pôde ser atualizada."
                )
            except Exception as e:
                logger.error(f"Erro ao salvar aposta {bet['event_id']}: {e}")
        conn.commit()
        conn.close()
        return saved_count

    def process_all_matches(self):
        logger.info(
            "🚀 Iniciando processamento com MODELO ELO e registro de dados expandido..."
        )
        logger.info(
            f"📊 Parâmetros: K_FACTOR={K_FACTOR}, DEFAULT_ELO={DEFAULT_ELO}, ELO_DIFF_THRESHOLD_OU={ELO_DIFF_THRESHOLD_OU}"
        )
        logger.info(
            f"📊 ROIs O/U: BALANCED_OVER={MIN_ROI_BALANCED_OVER}%, UNBALANCED_OVER={MIN_ROI_UNBALANCED_OVER}%, "
            f"BALANCED_UNDER={MIN_ROI_BALANCED_UNDER}%, UNBALANCED_UNDER={MIN_ROI_UNBALANCED_UNDER}%"
        )
        upcoming_matches = self.get_all_upcoming_matches()
        logger.info(f"Jogos não processados para analisar: {len(upcoming_matches)}")
        if not upcoming_matches:
            logger.info("Nenhum jogo novo para processar.")
            return

        all_valuable_bets = []
        for match in upcoming_matches:
            try:
                event_id = match["event_id"]
                logger.info(
                    f"Analisando evento {event_id}: {match['home_team']} vs {match['away_team']}"
                )
                odds_df = self.get_match_odds(event_id)
                if not odds_df.empty:
                    valuable_bets = self.analyze_bet_value(match, odds_df)
                    all_valuable_bets.extend(valuable_bets)
                self.mark_event_processed(event_id)
            except Exception as e:
                logger.error(
                    f"Erro fatal ao processar evento {match.get('event_id', 'N/A')}: {e}"
                )
                if "event_id" in match:
                    self.mark_event_processed(match["event_id"])

        total_saved = self.save_top_bets_by_league(all_valuable_bets)
        logger.info(f"✅ Processamento ELO concluído. {total_saved} apostas salvas.")


def main():
    processor = BetProcessor(
        tm_db_path="tm_data.db",
        bets_db_path="bets.db",
        results_db_path="table_tennis_results.db",
    )
    processor.process_all_matches()


if __name__ == "__main__":
    main()
