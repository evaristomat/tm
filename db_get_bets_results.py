import sqlite3
import pandas as pd
import requests
import os
from datetime import datetime
import logging
from dotenv import load_dotenv

load_dotenv()

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
        self.api_key = os.getenv("BETSAPI_API_KEY")

    def get_pending_bets(self):
        """Busca apostas que ainda não tem resultado"""
        conn = sqlite3.connect(self.bets_db_path)
        query = """
        SELECT id, event_id, league_name, home_team, away_team,
               event_time, bet_type, selection, handicap, odds
        FROM bets
        WHERE result IS NULL
        ORDER BY event_time
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def get_result_from_api(self, event_id):
        """Busca resultado diretamente da API"""
        if not self.api_key:
            logger.warning("API_KEY não encontrada")
            return None

        url = "https://api.betsapi.com/v1/bet365/result"
        params = {"token": self.api_key, "event_id": event_id}

        try:
            response = requests.get(url, params=params)
            data = response.json()

            logger.info(
                f"API Response - Success: {data.get('success')}, Results: {len(data.get('results', []))}"
            )

            if data.get("success") == 1 and data.get("results"):
                result = data["results"][0]
                time_status = result.get("time_status")

                if str(time_status) == "3":  # Finalizado (comparar como string)
                    logger.info(
                        f"✅ Resultado encontrado na API para event_id {event_id}"
                    )
                    return result
                else:
                    logger.info(
                        f"🕐 Evento {event_id} ainda não finalizado (status: {time_status})"
                    )
                    return None
            else:
                logger.warning(f"❌ Evento {event_id} não encontrado na API: {data}")
                return None

        except Exception as e:
            logger.error(f"Erro ao buscar na API: {e}")
            return None

    def calculate_total_games_from_api(self, api_result):
        """Calcula total de games a partir do resultado da API"""
        scores = api_result.get("scores", {})
        if not scores:
            return None

        total_games = 0
        for set_num, set_score in scores.items():
            home_score = int(set_score.get("home", 0))
            away_score = int(set_score.get("away", 0))
            total_games += home_score + away_score

        logger.info(f"Total de games calculado: {total_games}")
        return total_games

    def check_bet_result_from_api(self, bet, api_result):
        """Verifica resultado da aposta usando dados da API"""
        ss_score = api_result.get("ss")
        if not ss_score or "-" not in ss_score:
            logger.warning(f"Score inválido na API: {ss_score}")
            return None, None, None

        try:
            home_sets, away_sets = map(int, ss_score.split("-"))
        except ValueError:
            logger.warning(f"Erro ao parsear score da API: {ss_score}")
            return None, None, None

        bet_type = bet["bet_type"]
        selection = bet["selection"]
        odds = bet["odds"]

        if bet_type == "To Win":
            if selection == "Home":
                won = home_sets > away_sets
                actual_result = bet["home_team"] if won else bet["away_team"]
            elif selection == "Away":
                won = away_sets > home_sets
                actual_result = bet["away_team"] if won else bet["home_team"]
            else:
                logger.warning(f"Selection inválida: {selection}")
                return None, None, None

            result = 1 if won else 0
            profit = (odds - 1) if won else -1

            logger.info(
                f"To Win: {selection} | Real: {home_sets}-{away_sets} | Won: {won}"
            )
            return result, profit, actual_result

        elif bet_type == "Total":
            total_games = self.calculate_total_games_from_api(api_result)
            if total_games is None:
                return None, None, None

            handicap = bet["handicap"]

            if "Over" in selection:
                won = total_games > handicap
            elif "Under" in selection:
                won = total_games < handicap
            else:
                logger.warning(f"Selection inválida para Total: {selection}")
                return None, None, None

            result = 1 if won else 0
            profit = (odds - 1) if won else -1
            actual_result = f"{total_games} games"

            logger.info(
                f"Total: {selection} {handicap} | Real: {total_games} | Won: {won}"
            )
            return result, profit, actual_result

        return None, None, None

    def update_bet_result(self, bet_id, result, profit, actual_result=None):
        """Atualiza resultado da aposta no banco"""
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
        UPDATE bets
        SET result = ?, profit = ?, actual_result = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
            (result, profit, actual_result, bet_id),
        )

        conn.commit()
        conn.close()

    def process_results(self):
        """Processa resultados usando a API"""
        logger.info("🔍 Processando resultados via API...")

        pending_bets = self.get_pending_bets()

        if pending_bets.empty:
            logger.info("✅ Nenhuma aposta pendente")
            return

        logger.info(f"📊 Total pendentes: {len(pending_bets)}")

        processed = 0
        wins = 0
        losses = 0
        total_profit = 0
        not_found = 0

        for i, bet in pending_bets.iterrows():
            logger.info(f"\n--- {i + 1}/{len(pending_bets)} ---")
            logger.info(f"ID: {bet['id']} | Event: {bet['event_id']}")
            logger.info(f"{bet['home_team']} vs {bet['away_team']}")
            logger.info(f"{bet['bet_type']} | {bet['selection']}")

            # Buscar resultado na API
            api_result = self.get_result_from_api(bet["event_id"])

            if api_result:
                result, profit, actual_result = self.check_bet_result_from_api(
                    bet, api_result
                )

                if result is not None:
                    self.update_bet_result(bet["id"], result, profit, actual_result)
                    processed += 1
                    total_profit += profit

                    if result == 1:
                        wins += 1
                        logger.info(f"🟢 GANHOU | +{profit:.2f}u | {actual_result}")
                    else:
                        losses += 1
                        logger.info(f"🔴 PERDEU | {profit:.2f}u | {actual_result}")
                else:
                    logger.warning("❓ Erro ao processar resultado")
            else:
                not_found += 1
                logger.warning("❌ Resultado não disponível")

        self.show_summary(processed, wins, losses, total_profit, not_found)

    def show_summary(self, processed, wins, losses, total_profit, not_found):
        """Mostra resumo dos resultados"""
        logger.info(f"\n{'=' * 50}")
        logger.info(f"📊 RESUMO FINAL")
        logger.info(f"{'=' * 50}")
        logger.info(f"Processadas: {processed}")
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
        logger.info(f"Não encontradas: {not_found}")
        logger.info(f"Lucro total: {total_profit:+.2f}u")
        logger.info(
            f"ROI: {total_profit / processed * 100:+.1f}%"
            if processed > 0
            else "ROI: 0%"
        )


def main():
    checker = BetResultsChecker()
    checker.process_results()


if __name__ == "__main__":
    main()
