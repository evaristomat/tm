import pandas as pd
import requests
import sqlite3
import os
import time
from datetime import datetime
from dotenv import load_dotenv
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("bulk_results")


class BulkResultsProcessor:
    def __init__(self, csv_path, db_path="bets.db"):
        self.csv_path = csv_path
        self.db_path = db_path
        self.api_key = os.getenv("BETSAPI_API_KEY")
        self.request_count = 0
        self.start_time = time.time()

        if not self.api_key:
            raise ValueError("BETSAPI_API_KEY não encontrada!")

    def load_pending_bets(self):
        """Carrega apostas sem resultado do CSV"""
        df = pd.read_csv(self.csv_path)

        # Filtrar apenas apostas sem resultado
        pending = df[df["result"].isna() | (df["result"] == "")]

        logger.info(f"Total de apostas no CSV: {len(df)}")
        logger.info(f"Apostas pendentes: {len(pending)}")

        return pending

    def get_results_batch(self, event_ids):
        """Busca resultados para um lote de event_ids (máximo 10)"""
        if len(event_ids) > 10:
            event_ids = event_ids[:10]

        event_ids_str = ",".join([str(eid) for eid in event_ids])

        url = "https://api.betsapi.com/v1/bet365/result"
        params = {"token": self.api_key, "event_id": event_ids_str}

        try:
            # Rate limiting: máximo 50 req/min
            time.sleep(1.2)  # 50 req/min = 1 req por 1.2s

            response = requests.get(url, params=params, timeout=10)
            self.request_count += 1

            if self.request_count % 10 == 0:
                elapsed = time.time() - self.start_time
                rate = self.request_count / elapsed * 60
                logger.info(
                    f"Progresso: {self.request_count} requests - {rate:.1f} req/min"
                )

            if response.status_code == 200:
                data = response.json()

                if data.get("success") == 1 and data.get("results"):
                    return data["results"]
                else:
                    logger.warning(f"Sem resultados para lote: {event_ids_str}")
                    return []
            else:
                logger.error(f"HTTP {response.status_code} para lote: {event_ids_str}")
                return []

        except Exception as e:
            logger.error(f"Erro ao buscar lote {event_ids_str}: {e}")
            return []

    def calculate_total_games(self, scores):
        """Calcula total de games a partir dos scores"""
        if not scores:
            return None

        total_games = 0
        for set_num, set_score in scores.items():
            try:
                home_score = int(set_score.get("home", 0))
                away_score = int(set_score.get("away", 0))
                total_games += home_score + away_score
            except (ValueError, TypeError):
                continue

        return total_games

    def check_bet_result(self, bet, api_result):
        """Verifica resultado da aposta usando dados da API"""
        ss_score = api_result.get("ss")
        if not ss_score or "-" not in ss_score:
            return None, None, None

        try:
            home_sets, away_sets = map(int, ss_score.split("-"))
        except ValueError:
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
                return None, None, None

            result = 1 if won else 0
            profit = (odds - 1) if won else -1
            return result, profit, actual_result

        elif bet_type == "Total":
            total_games = self.calculate_total_games(api_result.get("scores", {}))
            if total_games is None:
                return None, None, None

            handicap = bet["handicap"]

            if "Over" in selection:
                won = total_games > handicap
            elif "Under" in selection:
                won = total_games < handicap
            else:
                return None, None, None

            result = 1 if won else 0
            profit = (odds - 1) if won else -1
            actual_result = f"{total_games} games"
            return result, profit, actual_result

        return None, None, None

    def process_batch(self, batch_df):
        """Processa um lote de apostas"""
        event_ids = batch_df["event_id"].unique().tolist()
        api_results = self.get_results_batch(event_ids)

        # Criar dicionário de resultados por event_id
        results_dict = {}
        for result in api_results:
            if str(result.get("time_status")) == "3":  # Finalizado
                results_dict[str(result.get("id"))] = result

        processed_rows = []

        for _, bet in batch_df.iterrows():
            event_id = str(bet["event_id"])

            if event_id in results_dict:
                api_result = results_dict[event_id]
                result, profit, actual_result = self.check_bet_result(bet, api_result)

                if result is not None:
                    bet_updated = bet.copy()
                    bet_updated["result"] = result
                    bet_updated["profit"] = profit
                    bet_updated["actual_result"] = actual_result
                    bet_updated["updated_at"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    processed_rows.append(bet_updated)

                    status = "GANHOU" if result == 1 else "PERDEU"
                    logger.info(
                        f"{status}: {bet['home_team']} vs {bet['away_team']} - {profit:+.2f}u"
                    )

        return processed_rows

    def process_all_bets(self, batch_size=10, max_workers=3):
        """Processa todas as apostas pendentes"""
        pending_bets = self.load_pending_bets()

        if pending_bets.empty:
            logger.info("Nenhuma aposta pendente para processar")
            return

        # Dividir em lotes
        batches = []
        for i in range(0, len(pending_bets), batch_size):
            batch = pending_bets.iloc[i : i + batch_size]
            batches.append(batch)

        logger.info(f"Processando {len(pending_bets)} apostas em {len(batches)} lotes")

        all_processed = []
        processed_count = 0

        # Processar sequencialmente para respeitar rate limits
        for i, batch in enumerate(batches):
            logger.info(f"Processando lote {i + 1}/{len(batches)}")

            processed_batch = self.process_batch(batch)
            all_processed.extend(processed_batch)
            processed_count += len(processed_batch)

            # Progress update
            if (i + 1) % 10 == 0:
                logger.info(
                    f"Progresso: {i + 1}/{len(batches)} lotes - {processed_count} apostas processadas"
                )

        return all_processed

    def update_csv(self, processed_bets):
        """Atualiza o CSV com os resultados"""
        if not processed_bets:
            logger.info("Nenhuma aposta para atualizar")
            return

        # Carregar CSV original
        original_df = pd.read_csv(self.csv_path)

        # Criar dicionário de updates por ID
        updates = {}
        for bet in processed_bets:
            bet_id = bet["id"]
            updates[bet_id] = {
                "result": bet["result"],
                "profit": bet["profit"],
                "actual_result": bet["actual_result"],
                "updated_at": bet["updated_at"],
            }

        # Aplicar updates
        updated_count = 0
        for idx, row in original_df.iterrows():
            bet_id = row["id"]
            if bet_id in updates:
                for col, value in updates[bet_id].items():
                    original_df.at[idx, col] = value
                updated_count += 1

        # Salvar CSV atualizado
        backup_path = self.csv_path.replace(".csv", "_backup.csv")
        original_df.to_csv(backup_path, index=False)
        original_df.to_csv(self.csv_path, index=False)

        logger.info(f"CSV atualizado: {updated_count} apostas")
        logger.info(f"Backup salvo em: {backup_path}")

    def update_database(self, processed_bets):
        """Atualiza o banco SQLite se existir"""
        if not processed_bets or not os.path.exists(self.db_path):
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        updated_count = 0
        for bet in processed_bets:
            cursor.execute(
                """
            UPDATE bets 
            SET result = ?, profit = ?, actual_result = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
                (bet["result"], bet["profit"], bet["actual_result"], bet["id"]),
            )

            if cursor.rowcount > 0:
                updated_count += 1

        conn.commit()
        conn.close()

        logger.info(f"Banco atualizado: {updated_count} apostas")

    def show_summary(self, processed_bets):
        """Mostra resumo dos resultados"""
        if not processed_bets:
            return

        df = pd.DataFrame(processed_bets)

        total_processed = len(df)
        wins = len(df[df["result"] == 1])
        losses = len(df[df["result"] == 0])
        total_profit = df["profit"].sum()
        win_rate = wins / total_processed * 100 if total_processed > 0 else 0
        roi = total_profit / total_processed * 100 if total_processed > 0 else 0

        logger.info(f"\n{'=' * 50}")
        logger.info(f"RESUMO FINAL")
        logger.info(f"{'=' * 50}")
        logger.info(f"Apostas processadas: {total_processed}")
        logger.info(f"Vitórias: {wins} ({win_rate:.1f}%)")
        logger.info(f"Derrotas: {losses}")
        logger.info(f"Lucro total: {total_profit:+.2f}u")
        logger.info(f"ROI: {roi:+.1f}%")
        logger.info(f"Total de requests: {self.request_count}")

    def run(self):
        """Executa o processamento completo"""
        logger.info("Iniciando processamento em lote...")

        processed_bets = self.process_all_bets(batch_size=10, max_workers=1)

        if processed_bets:
            self.update_csv(processed_bets)
            self.update_database(processed_bets)
            self.show_summary(processed_bets)
        else:
            logger.info("Nenhuma aposta foi processada")


def main():
    csv_path = input("Digite o caminho do arquivo CSV: ").strip()

    if not os.path.exists(csv_path):
        print(f"Arquivo não encontrado: {csv_path}")
        return

    processor = BulkResultsProcessor(csv_path)
    processor.run()


if __name__ == "__main__":
    main()
