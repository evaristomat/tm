import requests
import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Carregar variáveis de ambiente
load_dotenv()


class TableTennisUpdater:
    def __init__(self, db_path="table_tennis_results.db"):
        self.db_path = db_path
        self.api_key = os.getenv("BETSAPI_API_KEY")
        self.leagues = {
            10048210: "Czech Liga Pro",
            10068516: "Challenger Series TT",
            10073432: "TT Cup",
            10073465: "TT Elite Series",
        }
        self.init_database()

    def init_database(self):
        """Inicializa o banco de dados para resultados de tênis de mesa"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Tabela principal de eventos
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT UNIQUE,
            event_time INTEGER,
            time_status INTEGER,
            league_id TEXT,
            league_name TEXT,
            home_id TEXT,
            home_name TEXT,
            home_image_id INTEGER,
            home_cc TEXT,
            away_id TEXT,
            away_name TEXT,
            away_image_id INTEGER,
            away_cc TEXT,
            score TEXT,
            bestofsets TEXT,
            stadium_id TEXT,
            stadium_name TEXT,
            stadium_city TEXT,
            stadium_country TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Tabela de scores por set
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS event_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT,
            set_number INTEGER,
            home_score INTEGER,
            away_score INTEGER,
            FOREIGN KEY (event_id) REFERENCES events (event_id)
        )
        """)

        conn.commit()
        conn.close()
        print("✅ Banco de dados inicializado")

    def get_existing_event_ids(self):
        """Retorna todos os event_ids já existentes no banco de dados"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT event_id FROM events")
            existing_ids = {str(row[0]) for row in cursor.fetchall()}
            print(f"📋 Encontrados {len(existing_ids)} eventos no banco de dados")
        except sqlite3.OperationalError:
            # Se a tabela não existir, criar e retornar conjunto vazio
            print("📭 Tabela 'events' não encontrada, criando nova...")
            self.init_database()
            existing_ids = set()

        conn.close()
        return existing_ids

    def get_events_from_leagues(self, days=2):
        """Coleta eventos das ligas de tênis de mesa dos últimos N dias"""
        if not self.api_key:
            print("❌ BETSAPI_API_KEY não encontrada nas variáveis de ambiente")
            return []

        # Obter eventos já existentes no banco
        existing_ids = self.get_existing_event_ids()
        all_events = []
        total_found = 0
        total_new = 0

        print("=" * 70)
        print(f"COLETANDO EVENTOS DE TÊNIS DE MESA - ÚLTIMOS {days} DIAS")
        print("=" * 70)

        # Coletar eventos para cada dia
        for i in range(days):
            target_date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            print(f"\n📅 Buscando eventos para: {target_date}")

            for league_id, league_name in self.leagues.items():
                print(f"   🔍 Liga: {league_name}")

                url = "https://api.betsapi.com/v1/bet365/upcoming"
                params = {
                    "token": self.api_key,
                    "sport_id": 92,
                    "league_id": league_id,
                    "day": target_date,
                }

                try:
                    response = requests.get(url, params=params)
                    data = response.json()

                    if data.get("success") == 1 and "results" in data:
                        events = data["results"]
                        new_events = []

                        for event in events:
                            event_id = str(event.get("id"))
                            total_found += 1

                            # Verificar se o evento já existe no banco
                            if event_id not in existing_ids:
                                event["league_name"] = league_name
                                event["event_date"] = target_date
                                new_events.append(event)
                                total_new += 1
                            else:
                                print(f"      ⏭️  Evento {event_id} já existe no banco")

                        print(
                            f"      ✅ {len(events)} eventos encontrados, {len(new_events)} novos"
                        )
                        all_events.extend(new_events)
                    else:
                        print(f"      ⚠️  Nenhum evento encontrado")

                    # Delay para não sobrecarregar a API
                    time.sleep(0.5)

                except Exception as e:
                    print(f"      ❌ Erro: {e}")

        print(f"\n📊 Total: {total_found} eventos encontrados, {total_new} novos")
        return all_events

    def filter_existing_events(self, event_ids):
        """Filtra event_ids que já existem no banco de dados"""
        existing_ids = self.get_existing_event_ids()
        new_event_ids = []

        for eid in event_ids:
            if str(eid) not in existing_ids:
                new_event_ids.append(eid)
            else:
                print(f"⏭️  Evento {eid} já existe no banco (filtragem)")

        print(f"📊 Após filtragem: {len(new_event_ids)} eventos novos para processar")
        return new_event_ids

    def get_event_results_batch(self, event_ids):
        """Busca resultados para um lote de event_ids (máximo 10 por requisição)"""
        if not self.api_key:
            print("❌ BETSAPI_API_KEY não encontrada")
            return []

        # Limitar a 10 event_ids por requisição
        if len(event_ids) > 10:
            event_ids = event_ids[:10]

        event_ids_str = ",".join([str(eid) for eid in event_ids])

        url = "https://api.betsapi.com/v1/bet365/result"
        params = {"token": self.api_key, "event_id": event_ids_str}

        try:
            response = requests.get(url, params=params)
            data = response.json()

            if data.get("success") == 1 and "results" in data:
                return data["results"]
            else:
                print(f"❌ Nenhum resultado encontrado para lote")
                return []

        except Exception as e:
            print(f"❌ Erro ao buscar resultados: {e}")
            return []

    def get_all_event_results(self, event_ids, max_workers=3):
        """Busca resultados para todos os event_ids usando multi-threading"""
        if not event_ids:
            return []

        all_results = []

        # Dividir event_ids em lotes de 10
        batches = [event_ids[i : i + 10] for i in range(0, len(event_ids), 10)]

        print(
            f"\n📊 Buscando resultados para {len(event_ids)} eventos em {len(batches)} lotes..."
        )

        # Usar multi-threading para processar os lotes em paralelo
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Enviar todos os lotes para processamento
            future_to_batch = {
                executor.submit(self.get_event_results_batch, batch): batch
                for batch in batches
            }

            # Coletar resultados conforme ficam prontos
            for future in as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    results = future.result()
                    all_results.extend(results)
                    print(f"✅ Lote processado: {len(results)} resultados")
                except Exception as e:
                    print(f"❌ Erro ao processar lote: {e}")

                # Delay entre requisições para não sobrecarregar a API
                time.sleep(1)

        return all_results

    def save_results_to_db(self, results):
        """Salva os resultados no banco de dados"""
        if not results:
            print("📭 Nenhum resultado para salvar")
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        saved_count = 0
        skipped_count = 0

        for result in results:
            event_id = str(result.get("id"))

            # Verificar se o evento já existe (verificação adicional)
            cursor.execute("SELECT id FROM events WHERE event_id = ?", (event_id,))
            if cursor.fetchone():
                skipped_count += 1
                print(f"⏭️  Evento {event_id} já existe, pulando...")
                continue

            try:
                # Extrair dados do estádio
                stadium_data = result.get("extra", {}).get("stadium_data", {})

                # Inserir evento principal
                cursor.execute(
                    """
                INSERT INTO events (
                    event_id, event_time, time_status, league_id, league_name,
                    home_id, home_name, home_image_id, home_cc,
                    away_id, away_name, away_image_id, away_cc,
                    score, bestofsets, stadium_id, stadium_name, stadium_city, stadium_country
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        event_id,
                        result.get("time"),
                        result.get("time_status"),
                        result.get("league", {}).get("id"),
                        result.get("league", {}).get("name"),
                        result.get("home", {}).get("id"),
                        result.get("home", {}).get("name"),
                        result.get("home", {}).get("image_id"),
                        result.get("home", {}).get("cc"),
                        result.get("away", {}).get("id"),
                        result.get("away", {}).get("name"),
                        result.get("away", {}).get("image_id"),
                        result.get("away", {}).get("cc"),
                        result.get("ss"),
                        result.get("extra", {}).get("bestofsets"),
                        stadium_data.get("id"),
                        stadium_data.get("name"),
                        stadium_data.get("city"),
                        stadium_data.get("country"),
                    ),
                )

                # Inserir scores por set
                scores = result.get("scores", {})
                for set_num, score_data in scores.items():
                    try:
                        set_number = int(set_num)
                        cursor.execute(
                            """
                        INSERT INTO event_scores (event_id, set_number, home_score, away_score)
                        VALUES (?, ?, ?, ?)
                        """,
                            (
                                event_id,
                                set_number,
                                int(score_data.get("home", 0)),
                                int(score_data.get("away", 0)),
                            ),
                        )
                    except (ValueError, TypeError):
                        # Ignorar sets com valores inválidos
                        continue

                saved_count += 1
                print(
                    f"💾 Salvo: {result.get('home', {}).get('name')} {result.get('ss')} {result.get('away', {}).get('name')}"
                )

            except Exception as e:
                print(f"❌ Erro ao salvar evento {event_id}: {e}")

        conn.commit()
        conn.close()
        print(
            f"✅ {saved_count} resultados salvos, {skipped_count} pulados (já existiam)"
        )

    def update_database(self):
        """Atualiza o banco de dados com os eventos mais recentes"""
        # Coletar eventos dos últimos 2 dias
        events = self.get_events_from_leagues(days=2)

        if not events:
            print("✅ Nenhum novo evento encontrado para processar")
            return

        # Extrair IDs dos eventos
        event_ids = [str(event["id"]) for event in events]

        # Filtragem adicional para garantir que não processamos eventos existentes
        event_ids = self.filter_existing_events(event_ids)

        if not event_ids:
            print("✅ Nenhum novo evento para processar após filtragem")
            return

        # Buscar resultados usando multi-threading
        results = self.get_all_event_results(event_ids, max_workers=3)

        # Salvar resultados no banco de dados
        self.save_results_to_db(results)

        print("✅ Atualização concluída!")


def main():
    # Inicializar atualizador
    updater = TableTennisUpdater()

    # Executar atualização
    updater.update_database()


if __name__ == "__main__":
    main()
