import sqlite3
import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("bet_results")


class BetResultsChecker:
    def __init__(
        self, bets_db_path="bets.db", results_db_path="table_tennis_results.db"
    ):
        self.bets_db_path = bets_db_path
        self.results_db_path = results_db_path

    def get_pending_bets(self):
        """Busca apostas que ainda não tem resultado"""
        conn = sqlite3.connect(self.bets_db_path)

        query = """
        SELECT 
            id,
            event_id,
            league_name,
            home_team,
            away_team,
            event_time,
            bet_type,
            selection,
            handicap,
            odds
        FROM bets
        WHERE result IS NULL
        AND event_time < datetime('now')
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        return df

    def get_match_result(self, home_team, away_team, event_time):
        """Busca o resultado da partida no banco de resultados"""
        conn = sqlite3.connect(self.results_db_path)

        # Converter event_time para timestamp
        if isinstance(event_time, str):
            event_dt = pd.to_datetime(event_time)
        else:
            event_dt = event_time

        # Buscar com margem de 2 horas antes e 4 horas depois do horário previsto
        time_min = int((event_dt - pd.Timedelta(hours=2)).timestamp())
        time_max = int((event_dt + pd.Timedelta(hours=4)).timestamp())

        query = """
        SELECT 
            event_id,
            home_name,
            away_name,
            score,
            time_status
        FROM events
        WHERE home_name = ? 
        AND away_name = ?
        AND event_time BETWEEN ? AND ?
        AND time_status = 3
        """

        df = pd.read_sql_query(
            query, conn, params=(home_team, away_team, time_min, time_max)
        )
        conn.close()

        if df.empty:
            return None

        return df.iloc[0]

    def get_match_total_games(self, event_id):
        """Calcula o total de games em uma partida"""
        conn = sqlite3.connect(self.results_db_path)

        query = """
        SELECT 
            set_number,
            home_score,
            away_score
        FROM event_scores
        WHERE event_id = ?
        ORDER BY set_number
        """

        df = pd.read_sql_query(query, conn, params=(event_id,))
        conn.close()

        if df.empty:
            return None

        total_games = 0
        for _, row in df.iterrows():
            total_games += row["home_score"] + row["away_score"]

        return total_games

    def check_bet_result(self, bet, match_result):
        """Verifica se a aposta foi ganha ou perdida e retorna o resultado real"""
        if match_result is None:
            return None, None, None

        score = match_result["score"]
        if not score or "-" not in score:
            return None, None, None

        try:
            home_sets, away_sets = map(int, score.split("-"))
        except ValueError:
            return None, None, None

        bet_type = bet["bet_type"]
        selection = bet["selection"]
        odds = bet["odds"]
        home_team = bet["home_team"]
        away_team = bet["away_team"]

        # Para apostas To Win
        if bet_type == "To Win":
            # Determinar o vencedor real
            if home_sets > away_sets:
                actual_winner = home_team
            else:
                actual_winner = away_team

            if selection == "Home":
                won = home_sets > away_sets
            elif selection == "Away":
                won = away_sets > home_sets
            else:
                return None, None, None

            # Calcular lucro/prejuízo (considerando stake de 1 unidade)
            if won:
                profit = odds - 1  # Lucro = (odds - 1) * stake
                result = 1
            else:
                profit = -1  # Perda = -stake
                result = 0

            return result, profit, actual_winner

        # Para apostas Total
        elif bet_type == "Total":
            # Buscar total de games
            total_games = self.get_match_total_games(match_result["event_id"])

            if total_games is None:
                return None, None, None

            handicap = bet["handicap"]

            if "Over" in selection:
                won = total_games > handicap
            elif "Under" in selection:
                won = total_games < handicap
            else:
                return None, None, None

            # Calcular lucro/prejuízo
            if won:
                profit = odds - 1
                result = 1
            else:
                profit = -1
                result = 0

            # O resultado real é o total de games
            actual_result_value = str(total_games)

            return result, profit, actual_result_value

        return None, None, None

    def update_bet_result(self, bet_id, result, profit, actual_result=None):
        """Atualiza o resultado da aposta no banco"""
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        if actual_result is not None:
            cursor.execute(
                """
            UPDATE bets
            SET result = ?, profit = ?, actual_result = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
                (result, profit, actual_result, bet_id),
            )
        else:
            cursor.execute(
                """
            UPDATE bets
            SET result = ?, profit = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
                (result, profit, bet_id),
            )

        conn.commit()
        conn.close()

    def process_results(self):
        """Processa todos os resultados pendentes"""
        logger.info("🔍 Buscando apostas pendentes de resultado...")

        pending_bets = self.get_pending_bets()

        if pending_bets.empty:
            logger.info("✅ Nenhuma aposta pendente de resultado")
            return

        logger.info(f"📊 Total de apostas pendentes: {len(pending_bets)}")

        processed = 0
        wins = 0
        losses = 0
        total_profit = 0

        for _, bet in pending_bets.iterrows():
            # Buscar resultado da partida
            match_result = self.get_match_result(
                bet["home_team"], bet["away_team"], bet["event_time"]
            )

            if match_result is not None:
                # Verificar resultado da aposta
                result, profit, actual_result = self.check_bet_result(bet, match_result)

                if result is not None:
                    # Atualizar no banco
                    self.update_bet_result(bet["id"], result, profit, actual_result)
                    processed += 1

                    if result == 1:
                        wins += 1
                        logger.info(
                            f"✅ GANHOU: {bet['home_team']} vs {bet['away_team']} - {bet['selection']} - Lucro: +{profit:.2f} - Resultado: {actual_result}"
                        )
                    else:
                        losses += 1
                        logger.info(
                            f"❌ PERDEU: {bet['home_team']} vs {bet['away_team']} - {bet['selection']} - Perda: {profit:.2f} - Resultado: {actual_result}"
                        )

                    total_profit += profit

        # Resumo
        logger.info(f"\n{'=' * 50}")
        logger.info(f"📊 RESUMO DOS RESULTADOS")
        logger.info(f"{'=' * 50}")
        logger.info(f"Total processadas: {processed}")
        logger.info(
            f"Vitórias: {wins} ({wins / processed * 100:.1f}%)"
            if processed > 0
            else "Vitórias: 0"
        )
        logger.info(
            f"Derrotas: {losses} ({losses / processed * 100:.1f}%)"
            if processed > 0
            else "Derrotas: 0"
        )
        logger.info(f"Lucro/Prejuízo Total: {total_profit:+.2f} unidades")
        logger.info(
            f"ROI Real: {total_profit / processed * 100:+.1f}%"
            if processed > 0
            else "ROI Real: 0%"
        )
        logger.info(f"{'=' * 50}")

        # Estatísticas por liga
        self.show_league_stats()

    def show_league_stats(self):
        """Mostra estatísticas por liga"""
        conn = sqlite3.connect(self.bets_db_path)

        query = """
        SELECT 
            league_name,
            COUNT(*) as total_bets,
            SUM(CASE WHEN result = 1 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN result = 0 THEN 1 ELSE 0 END) as losses,
            SUM(profit) as total_profit,
            AVG(estimated_roi) as avg_estimated_roi
        FROM bets
        WHERE result IS NOT NULL
        GROUP BY league_name
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        if not df.empty:
            logger.info(f"\n📊 ESTATÍSTICAS POR LIGA:")
            logger.info(f"{'=' * 50}")

            for _, row in df.iterrows():
                win_rate = (
                    (row["wins"] / row["total_bets"] * 100)
                    if row["total_bets"] > 0
                    else 0
                )
                real_roi = (
                    (row["total_profit"] / row["total_bets"] * 100)
                    if row["total_bets"] > 0
                    else 0
                )

                logger.info(f"\n{row['league_name']}")
                logger.info(f"  • Total: {int(row['total_bets'])} apostas")
                logger.info(f"  • Vitórias: {int(row['wins'])} ({win_rate:.1f}%)")
                logger.info(f"  • Lucro: {row['total_profit']:+.2f} unidades")
                logger.info(f"  • ROI Real: {real_roi:+.1f}%")
                logger.info(f"  • ROI Estimado Médio: {row['avg_estimated_roi']:.1f}%")


def main():
    checker = BetResultsChecker()
    checker.process_results()


if __name__ == "__main__":
    main()
