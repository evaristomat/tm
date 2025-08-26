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


class TableTennisResults:
    def __init__(self, db_path="table_tennis_results.db"):
        self.db_path = db_path
        self.api_key = os.getenv("BETSAPI_API_KEY")
        self.leagues = {
            10048210: "Czech Liga Pro",
            10068516: "Challenger Series TT",
            10073432: "TT Cup",
            10073465: "TT Elite Series",
        }
        self.request_count = 0
        self.last_request_time = time.time()
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

    def rate_limited_request(self, url, params):
        """Faz uma requisição com controle de rate limiting"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time

        # Limite de 3600 requisições por minuto = 60 por segundo
        # Vamos ser conservadores e usar 50 por segundo para dar margem de segurança
        min_interval = (
            0.02  # 50 requisições por segundo (0.02 segundos entre requisições)
        )

        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

        try:
            response = requests.get(url, params=params)
            self.last_request_time = time.time()
            self.request_count += 1

            # Log a cada 100 requisições
            if self.request_count % 100 == 0:
                print(f"📊 Total de requisições: {self.request_count}")

            return response
        except Exception as e:
            print(f"❌ Erro na requisição: {e}")
            return None

    def get_events_from_leagues(self, days=30):
        """Coleta eventos das ligas de tênis de mesa dos últimos N dias"""
        if not self.api_key:
            print("❌ BETSAPI_API_KEY não encontrada nas variáveis de ambiente")
            return []

        all_events = []

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
                    response = self.rate_limited_request(url, params)
                    if not response:
                        continue

                    data = response.json()

                    if data.get("success") == 1 and "results" in data:
                        events = data["results"]
                        print(f"      ✅ {len(events)} eventos encontrados")

                        for event in events:
                            event["league_name"] = league_name
                            event["event_date"] = target_date
                            all_events.append(event)
                    else:
                        print(f"      ⚠️  Nenhum evento encontrado")

                except Exception as e:
                    print(f"      ❌ Erro: {e}")

        return all_events

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
            response = self.rate_limited_request(url, params)
            if not response:
                return []

            data = response.json()

            if data.get("success") == 1 and "results" in data:
                return data["results"]
            else:
                print(f"❌ Nenhum resultado encontrado para lote")
                return []

        except Exception as e:
            print(f"❌ Erro ao buscar resultados: {e}")
            return []

    def get_all_event_results(self, event_ids, max_workers=5):
        """Busca resultados para todos os event_ids usando multi-threading"""
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

        return all_results

    def save_results_to_db(self, results):
        """Salva os resultados no banco de dados"""
        if not results:
            print("📭 Nenhum resultado para salvar")
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        saved_count = 0

        for result in results:
            event_id = result.get("id")

            # Verificar se o evento já existe
            cursor.execute("SELECT id FROM events WHERE event_id = ?", (event_id,))
            if cursor.fetchone():
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
        print(f"✅ {saved_count} resultados salvos no banco de dados")

    def analyze_results(self):
        """Analisa os resultados armazenados no banco de dados"""
        conn = sqlite3.connect(self.db_path)

        # Verificar se há dados
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM events")
        event_count = cursor.fetchone()[0]

        if event_count == 0:
            print("📭 Nenhum resultado encontrado no banco de dados")
            conn.close()
            return

        print("\n📊 ANÁLISE DOS RESULTADOS ARMAZENADOS")
        print("=" * 60)

        # Carregar dados
        events_df = pd.read_sql_query("SELECT * FROM events", conn)
        scores_df = pd.read_sql_query("SELECT * FROM event_scores", conn)

        # Estatísticas básicas
        print(f"📈 Total de eventos: {len(events_df)}")
        print(f"🏆 Ligas representadas: {events_df['league_name'].nunique()}")
        print(
            f"👥 Times únicos: {events_df['home_name'].nunique() + events_df['away_name'].nunique()}"
        )

        # Distribuição por liga
        print("\n📋 DISTRIBUIÇÃO POR LIGA:")
        league_stats = events_df["league_name"].value_counts().reset_index()
        league_stats.columns = ["Liga", "Eventos"]
        league_stats["Percentual"] = (
            league_stats["Eventos"] / len(events_df) * 100
        ).round(1)
        print(league_stats.to_string(index=False))

        # Estatísticas de sets
        print("\n🎯 ESTATÍSTICAS DE SETS:")
        if not scores_df.empty:
            set_stats = scores_df.groupby("set_number").size().reset_index(name="Jogos")
            print("Sets com pontuação registrada:")
            print(set_stats.to_string(index=False))

        # Exemplo de resultados recentes
        print("\n👀 EXEMPLOS DE RESULTADOS RECENTES:")
        recent_results = events_df[
            ["home_name", "score", "away_name", "league_name"]
        ].head(5)
        for _, row in recent_results.iterrows():
            print(
                f"   {row['home_name']} {row['score']} {row['away_name']} ({row['league_name']})"
            )

        conn.close()


def main():
    # Inicializar coletor
    collector = TableTennisResults()

    # Coletar eventos dos últimos 30 dias
    events = collector.get_events_from_leagues(days=2)

    if not events:
        print("❌ Nenhum evento encontrado")
        return

    # Extrair IDs dos eventos
    event_ids = [event["id"] for event in events]
    print(f"\n📋 Total de {len(event_ids)} eventos encontrados")

    # Buscar resultados usando multi-threading
    results = collector.get_all_event_results(event_ids, max_workers=5)

    # Salvar resultados no banco de dados
    collector.save_results_to_db(results)

    # Analisar resultados
    collector.analyze_results()

    print(f"\n📊 Total de requisições realizadas: {collector.request_count}")


if __name__ == "__main__":
    main()
