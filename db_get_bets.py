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

# Configurações globais de ROI mínimo
MIN_ROI_ML = 20
MIN_ROI_OVER_UNDER = 25

# Parâmetros da nova fórmula
STRENGTH_SCALE_FACTOR = 0.5  # Fator de escala para diferença de força
H2H_MAX_WEIGHT = 0.3  # Peso máximo dos confrontos diretos
MIN_H2H_MATCHES = 3  # Mínimo de confrontos para considerar H2H
MIN_EDGE = 0.05  # Edge mínimo (prob_estimada - prob_implícita)


class BetProcessor:
    def __init__(self, tm_db_path="tm_data.db", bets_db_path="bets.db"):
        self.tm_db_path = tm_db_path
        self.bets_db_path = bets_db_path
        # Ligas atualizadas
        self.leagues = {
            10047071: "Setka Cup Women",
            10047098: "Setka Cup",
            10048210: "Czech Liga Pro",
            10073432: "TT Cup",
            10073465: "TT Elite Series",
        }
        self.init_bets_db()

    def init_bets_db(self):
        """Mantém estrutura original do banco - SEM MODIFICAÇÕES"""
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
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT event_id FROM processed_events")
        processed_ids = {row[0] for row in cursor.fetchall()}

        conn.close()
        return processed_ids

    def mark_event_processed(self, event_id):
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

    def get_player_last_matches(self, player_name, limit=20):
        """Busca últimos jogos com peso decrescente por idade"""
        conn = sqlite3.connect("table_tennis_results.db")

        query = """
        SELECT 
            event_id, event_time, home_name, away_name, score
        FROM events 
        WHERE (home_name = ? OR away_name = ?) 
        ORDER BY event_time DESC 
        LIMIT ?
        """

        df = pd.read_sql_query(query, conn, params=(player_name, player_name, limit))
        conn.close()

        return df

    def get_head_to_head_stats(self, player1, player2):
        """Busca estatísticas de confrontos diretos"""
        conn = sqlite3.connect("table_tennis_results.db")

        query = """
        SELECT 
            event_id, home_name, away_name, score, event_time
        FROM events 
        WHERE ((home_name = ? AND away_name = ?) 
               OR (home_name = ? AND away_name = ?))
        AND time_status = 3
        ORDER BY event_time DESC
        LIMIT 10
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

    def calculate_player_stats_weighted(self, player_name, matches_df):
        """Calcula estatísticas com peso decrescente por idade"""
        if matches_df.empty:
            return {
                "total_matches": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0,
                "total_games_played": 0,
                "avg_games_per_match": 0,
                "games_per_match_list": [],
                "weighted_win_rate": 0,
            }

        stats = {
            "total_matches": 0,
            "wins": 0,
            "losses": 0,
            "total_games_played": 0,
            "games_per_match_list": [],
            "weighted_wins": 0,
            "weighted_total": 0,
        }

        for idx, match in matches_df.iterrows():
            # Peso decrescente: jogos mais recentes têm mais peso
            weight = 1.0 / (idx + 1)  # 1.0, 0.5, 0.33, 0.25, etc.

            is_home = match["home_name"] == player_name
            score = match["score"]

            if not score or "-" not in score:
                continue

            try:
                home_score, away_score = map(int, score.split("-"))
            except ValueError:
                continue

            stats["total_matches"] += 1
            stats["weighted_total"] += weight

            won = (is_home and home_score > away_score) or (
                not is_home and away_score > home_score
            )

            if won:
                stats["wins"] += 1
                stats["weighted_wins"] += weight
            else:
                stats["losses"] += 1

            # Calcular jogos totais
            detailed_scores = self.get_detailed_scores(match["event_id"])
            total_games = 0
            for _, set_score in detailed_scores.iterrows():
                total_games += set_score["home_score"] + set_score["away_score"]

            if total_games > 0:
                stats["games_per_match_list"].append(total_games)
                stats["total_games_played"] += total_games

        # Win rates
        stats["win_rate"] = (
            (stats["wins"] / stats["total_matches"])
            if stats["total_matches"] > 0
            else 0
        )
        stats["weighted_win_rate"] = (
            (stats["weighted_wins"] / stats["weighted_total"])
            if stats["weighted_total"] > 0
            else 0
        )

        stats["avg_games_per_match"] = (
            (stats["total_games_played"] / len(stats["games_per_match_list"]))
            if stats["games_per_match_list"]
            else 0
        )

        logger.info(
            f"📊 {player_name}: {stats['wins']}W-{stats['losses']}L em {stats['total_matches']} jogos = {stats['win_rate']:.1f}% WR (Ponderado: {stats['weighted_win_rate']:.1f}%)"
        )

        return stats

    def calculate_ml_probability_v2(
        self, home_stats, away_stats, home_player, away_player
    ):
        """
        Nova fórmula: Força Relativa + Confrontos Diretos
        """
        home_wr = home_stats["weighted_win_rate"]
        away_wr = away_stats["weighted_win_rate"]

        # Passo 1: Calcular força relativa (vs média da liga = 50%)
        home_strength = home_wr - 0.5
        away_strength = away_wr - 0.5
        strength_diff = home_strength - away_strength

        # Passo 2: Converter para probabilidade base
        prob_base = 0.5 + (strength_diff * STRENGTH_SCALE_FACTOR)
        prob_base = max(0.15, min(0.85, prob_base))  # Limita extremos

        # Passo 3: Ajustar com confrontos diretos (se existirem)
        h2h_stats = self.get_head_to_head_stats(home_player, away_player)
        h2h_matches = h2h_stats["total_matches"]
        h2h_weight = 0

        if h2h_matches >= MIN_H2H_MATCHES:
            h2h_weight = min(H2H_MAX_WEIGHT, h2h_matches * 0.1)
            h2h_prob = h2h_stats["win_rate_player1"]  # player1 = home

            # Combina probabilidade base com H2H
            prob_final = (prob_base * (1 - h2h_weight)) + (h2h_prob * h2h_weight)
        else:
            prob_final = prob_base

        # Garantir limites
        prob_final = max(0.1, min(0.9, prob_final))

        return prob_final, strength_diff, h2h_matches, h2h_weight

    def analyze_ml_bet_v2(
        self, home_stats, away_stats, home_player, away_player, selection, odds_value
    ):
        """
        Nova análise ML com Força Relativa + Confrontos Diretos
        """
        prob_home, strength_diff, h2h_matches, h2h_weight = (
            self.calculate_ml_probability_v2(
                home_stats, away_stats, home_player, away_player
            )
        )

        if selection == "Home":
            est_prob = prob_home
        else:
            est_prob = 1 - prob_home

        # Calcular métricas
        implied_prob = 1 / odds_value
        edge = est_prob - implied_prob
        roi = ((est_prob * (odds_value - 1)) - (1 - est_prob)) * 100

        # Critérios de aceitação mais rigorosos
        accept_bet = (
            roi >= MIN_ROI_ML
            and edge >= MIN_EDGE
            and home_stats["total_matches"] >= 5
            and away_stats["total_matches"] >= 5
        )

        return accept_bet, est_prob, roi

    def analyze_over_under_bet(
        self, home_games, away_games, handicap_value, selection, odds_value
    ):
        """
        Análise Over/Under (mantida igual)
        """
        if "Over" in selection:
            home_count = sum(1 for g in home_games if g > handicap_value)
            away_count = sum(1 for g in away_games if g > handicap_value)
        else:
            home_count = sum(1 for g in home_games if g < handicap_value)
            away_count = sum(1 for g in away_games if g < handicap_value)

        home_prob = home_count / len(home_games) if home_games else 0
        away_prob = away_count / len(away_games) if away_games else 0

        prob_diff = abs(home_prob - away_prob)

        if prob_diff < 0.20:
            if home_prob >= 0.60 and away_prob >= 0.60:
                est_prob = max(home_prob, away_prob)
                min_roi = 15
            elif home_prob <= 0.40 and away_prob <= 0.40:
                est_prob = (home_prob + away_prob) / 2
                min_roi = 40
            else:
                est_prob = (home_prob + away_prob) / 2
                min_roi = 25
        else:
            est_prob = (home_prob + away_prob) / 2
            min_roi = 30

        roi = ((est_prob * (odds_value - 1)) - (1 - est_prob)) * 100
        accept = roi >= min_roi

        return accept, est_prob, roi, home_prob, away_prob, min_roi

    def filter_conflicting_bets(self, valuable_bets, home_player, away_player):
        """Mantém lógica original de filtro de conflitos"""
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

        # Buscar mais jogos (20 ao invés de 10)
        home_matches = self.get_player_last_matches(home_player, 20)
        home_stats = self.calculate_player_stats_weighted(home_player, home_matches)

        away_matches = self.get_player_last_matches(away_player, 20)
        away_stats = self.calculate_player_stats_weighted(away_player, away_matches)

        if home_stats["total_matches"] < 5 or away_stats["total_matches"] < 5:
            logger.warning(
                f"❌ DESCARTADO - Dados insuficientes: {home_player}({home_stats['total_matches']}) vs {away_player}({away_stats['total_matches']})"
            )
            return []

        valuable_bets = []

        for _, row in odds_df.iterrows():
            market = row["market_type"]
            selection = row["selection"]
            odds_value = row["odds"]
            handicap = row["handicap_value"]

            if odds_value <= 1.01:
                continue

            handicap_value = None
            if market == "Total" and handicap:
                try:
                    handicap_value = float(handicap.replace("O ", "").replace("U ", ""))
                except ValueError:
                    handicap_value = None

            if market == "To Win":
                accept_bet, est_prob, estimated_roi = self.analyze_ml_bet_v2(
                    home_stats,
                    away_stats,
                    home_player,
                    away_player,
                    selection,
                    odds_value,
                )

                log_message = (
                    f"ML V2 - {selection}: {home_player}({home_stats['weighted_win_rate']:.1f}%) vs {away_player}({away_stats['weighted_win_rate']:.1f}%) | "
                    f"Odds: {odds_value:.2f} | "
                    f"Est Prob: {est_prob:.3f} | "
                    f"Edge: {est_prob - (1 / odds_value):.3f} | "
                    f"ROI: {estimated_roi:.2f}%"
                )

                if accept_bet:
                    logger.info(Fore.GREEN + f"✅ APROVADA: {log_message}")
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
                            "fair_odds": 1 / est_prob if est_prob > 0 else 0,
                            "estimated_roi": estimated_roi,
                        }
                    )
                else:
                    logger.info(Fore.RED + f"❌ REJEITADA: {log_message}")

            elif market == "Total" and handicap_value is not None:
                home_games = home_stats["games_per_match_list"]
                away_games = away_stats["games_per_match_list"]

                accept_bet, est_prob, estimated_roi, home_prob, away_prob, min_roi = (
                    self.analyze_over_under_bet(
                        home_games, away_games, handicap_value, selection, odds_value
                    )
                )

                prob_label = "Over%" if "Over" in selection else "Under%"
                log_message = (
                    f"Total - {selection} {handicap_value}: {home_player} vs {away_player} | "
                    f"Odds: {odds_value:.2f} | "
                    f"Home {prob_label}: {home_prob:.3f} | "
                    f"Away {prob_label}: {away_prob:.3f} | "
                    f"Est Prob: {est_prob:.3f} | "
                    f"ROI: {estimated_roi:.2f}% | "
                    f"Min ROI: {min_roi}%"
                )

                if accept_bet:
                    logger.info(Fore.GREEN + f"✅ APROVADA: {log_message}")
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
                        }
                    )
                else:
                    logger.info(Fore.RED + f"❌ REJEITADA: {log_message}")

        return self.filter_conflicting_bets(valuable_bets, home_player, away_player)

    def save_top_bets_by_league(self, all_bets):
        """Mantém estrutura original do banco - SEM NOVAS COLUNAS"""
        if not all_bets:
            return 0

        bets_by_league_date = {}
        for bet in all_bets:
            event_date = bet["event_time"].date()
            league = bet["league_name"]
            key = (league, event_date)

            if key not in bets_by_league_date:
                bets_by_league_date[key] = []
            bets_by_league_date[key].append(bet)

        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        total_saved = 0

        for (league_name, event_date), league_bets in bets_by_league_date.items():
            ml_bets = [b for b in league_bets if b["bet_type"] == "To Win"]
            ou_bets = [b for b in league_bets if b["bet_type"] == "Total"]

            ml_bets.sort(key=lambda x: x["estimated_roi"], reverse=True)
            ou_bets.sort(key=lambda x: x["estimated_roi"], reverse=True)

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
                    # ESTRUTURA ORIGINAL - SEM NOVAS COLUNAS
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
                        logger.info(
                            Fore.GREEN
                            + f"💾 SALVA: {bet['bet_type']} {bet['selection']} @ {bet['odds']:.2f} (ROI: {bet['estimated_roi']:.2f}%)"
                        )
                except sqlite3.Error as e:
                    logger.error(f"Erro ao salvar aposta: {e}")

        conn.commit()
        conn.close()
        return total_saved

    def process_all_matches(self):
        logger.info("🚀 Iniciando processamento com NOVA LÓGICA V2...")
        logger.info(
            f"📊 Parâmetros: MIN_ROI_ML={MIN_ROI_ML}%, MIN_EDGE={MIN_EDGE}, STRENGTH_SCALE={STRENGTH_SCALE_FACTOR}"
        )

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
                logger.info(
                    f"Analisando evento {event_id}: {match['home_team']} vs {match['away_team']} ({match['league_name']})"
                )

                odds_df = self.get_match_odds(event_id)
                logger.info(f"Encontradas {len(odds_df)} odds para este evento")

                if not odds_df.empty:
                    valuable_bets = self.analyze_bet_value(match, odds_df)
                    if valuable_bets:
                        logger.info(
                            f"Encontradas {len(valuable_bets)} apostas valiosas neste evento"
                        )
                        all_valuable_bets.extend(valuable_bets)
                    else:
                        logger.info("Nenhuma aposta valiosa encontrada neste evento")
                else:
                    logger.warning("Nenhuma odd disponível para este evento")

                self.mark_event_processed(event_id)
                processed_events.append(event_id)

            except Exception as e:
                logger.error(f"Erro ao processar evento {match['event_id']}: {e}")
                self.mark_event_processed(match["event_id"])

        total_saved = self.save_top_bets_by_league(all_valuable_bets)

        logger.info(f"✅ Processamento V2 concluído.")
        logger.info(f"Eventos processados: {len(processed_events)}")
        logger.info(f"Total de apostas valiosas encontradas: {len(all_valuable_bets)}")
        logger.info(f"Total de apostas salvas: {total_saved}")

        if len(processed_events) > 0:
            logger.info("🔄 Sistema pode rodar novamente para capturar novos eventos!")


def main():
    processor = BetProcessor()
    processor.process_all_matches()


if __name__ == "__main__":
    main()
