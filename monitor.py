import httpx
import asyncio
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os
from dotenv import load_dotenv

# Suprimir logs do httpx
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# Carregar variáveis de ambiente
load_dotenv()


# Configuração de logging enriquecido
class EnhancedFormatter(logging.Formatter):
    """Formata logs de forma enriquecida com cores"""

    # Cores ANSI
    GREEN = "\x1b[32m"
    YELLOW = "\x1b[33m"
    RED = "\x1b[31m"
    BLUE = "\x1b[34m"
    MAGENTA = "\x1b[35m"
    CYAN = "\x1b[36m"
    RESET = "\x1b[0m"
    BOLD = "\x1b[1m"

    def format(self, record):
        timestamp = datetime.now().strftime("%H:%M:%S")

        if record.levelno == logging.INFO:
            return f"{self.GREEN}{timestamp} ✓ {record.getMessage()}{self.RESET}"
        elif record.levelno == logging.WARNING:
            return f"{self.YELLOW}{timestamp} ⚠ {record.getMessage()}{self.RESET}"
        elif record.levelno == logging.ERROR:
            return f"{self.RED}{timestamp} ✗ {record.getMessage()}{self.RESET}"
        elif record.levelno == logging.DEBUG:
            return f"{self.BLUE}{timestamp} • {record.getMessage()}{self.RESET}"
        else:
            return f"{timestamp} - {record.getMessage()}"


# Configurar logger
logger = logging.getLogger("TableTennisMonitor")
logger.setLevel(logging.INFO)

# Handler para console com formatação melhorada
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(EnhancedFormatter())

# Adicionar handler ao logger
logger.addHandler(console_handler)


class BetsAPIError(Exception):
    pass


class RateLimitError(Exception):
    pass


class Bet365Client:
    def __init__(self):
        self.base_url = os.getenv("BASE_URL", "https://api.betsapi.com/v1")
        self.base_url_v3 = os.getenv("BASE_URL_V3", "https://api.b365api.com/v3")
        # Tentar múltiplas variáveis de ambiente
        self.api_key = (
            os.getenv("API_KEY")
            or os.getenv("BETSAPI_API_KEY")
            or os.getenv("BETS_API_KEY")
        )
        self.request_timeout = int(os.getenv("REQUEST_TIMEOUT", "30"))
        self.client = httpx.AsyncClient(timeout=self.request_timeout)
        self.semaphore = asyncio.Semaphore(10)
        self.requests_count = 0

        # Debug da API key
        if not self.api_key:
            logger.error("API Key não encontrada! Variáveis disponíveis:")
            for key in os.environ.keys():
                if "API" in key.upper() or "KEY" in key.upper():
                    logger.error(f"  - {key}")
        else:
            logger.info(f"API Key carregada: {self.api_key[:10]}...")

    async def _make_request(
        self, endpoint: str, params: dict = None, version: str = "v1"
    ) -> dict:
        if params is None:
            params = {}

        if not self.api_key:
            raise BetsAPIError("API Key não configurada")

        params["token"] = self.api_key

        try:
            base_url = self.base_url if version == "v1" else self.base_url_v3
            url = f"{base_url}/{endpoint}"

            async with self.semaphore:
                self.requests_count += 1
                response = await self.client.get(url, params=params)
                response.raise_for_status()

                data = response.json()

                if data.get("success") == 0:
                    error_msg = data.get("error", "Unknown error")
                    if "rate limit" in error_msg.lower():
                        raise RateLimitError(error_msg)
                    raise BetsAPIError(error_msg)

                return data

        except httpx.HTTPError as e:
            raise BetsAPIError(f"HTTP error: {str(e)}")

    async def upcoming(
        self,
        sport_id: int,
        league_id: int = None,
        day: str = None,
        page: int = None,
    ) -> dict:
        params = {"sport_id": sport_id}
        if league_id:
            params["league_id"] = league_id
        if day:
            params["day"] = day
        if page:
            params["page"] = page
        return await self._make_request("bet365/upcoming", params, "v1")

    async def prematch(self, FI: str, raw: bool = False) -> dict:
        params = {"FI": FI}
        if raw:
            params["raw"] = 1
        return await self._make_request("bet365/prematch", params, "v3")

    async def close(self):
        await self.client.aclose()


class SimplifiedDatabase:
    def __init__(self, db_name: str = "tm_data.db"):
        self.db_name = db_name
        self.new_events_count = 0
        self.existing_events_count = 0
        self.new_odds_count = 0
        self.existing_odds_count = 0
        self.duplicate_events_count = 0
        self.init_database()

    def init_database(self):
        """Inicializa o banco de dados com tabelas simplificadas"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()

            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='events'"
            )
            events_table_exists = cursor.fetchone()

            if events_table_exists:
                cursor.execute("SELECT COUNT(*) FROM events")
                total_events = cursor.fetchone()[0]
                logger.info(
                    f"Banco de dados conectado. Total de eventos: {total_events}"
                )
            else:
                logger.info("Criando novo banco de dados...")

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    time INTEGER,
                    time_status INTEGER,
                    league_id INTEGER,
                    league_name TEXT,
                    home_team TEXT,
                    away_team TEXT,
                    odds_processed BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS match_odds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT,
                    market_type TEXT,
                    selection TEXT,
                    odds REAL,
                    handicap_value TEXT,
                    updated_at TIMESTAMP,
                    UNIQUE(event_id, market_type, selection, handicap_value),
                    FOREIGN KEY (event_id) REFERENCES events (id)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS first_game_odds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT,
                    market_type TEXT,
                    selection TEXT,
                    odds REAL,
                    handicap_value TEXT,
                    updated_at TIMESTAMP,
                    UNIQUE(event_id, market_type, selection, handicap_value),
                    FOREIGN KEY (event_id) REFERENCES events (id)
                )
            """)

            conn.commit()
            conn.close()

        except Exception as e:
            logger.error(f"Erro ao inicializar banco de dados: {e}")

    def get_stats(self):
        """Retorna estatísticas da sessão atual"""
        return {
            "new_events": self.new_events_count,
            "existing_events": self.existing_events_count,
            "new_odds": self.new_odds_count,
            "existing_odds": self.existing_odds_count,
            "duplicate_events": self.duplicate_events_count,
        }

    def find_similar_event(
        self, event: dict, time_threshold_hours: int = 6
    ) -> Optional[str]:
        """Procura por evento similar (mesmo confronto em período próximo)"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()

            home_team = event.get("home", {}).get("name", "")
            away_team = event.get("away", {}).get("name", "")
            league_id = event.get("league_id")

            # Converter event_time para inteiro se necessário
            event_time = event.get("time", 0)
            if isinstance(event_time, str):
                try:
                    event_time = int(event_time)
                except (ValueError, TypeError):
                    event_time = 0

            time_start = event_time - (time_threshold_hours * 3600)
            time_end = event_time + (time_threshold_hours * 3600)

            cursor.execute(
                """
                SELECT id FROM events 
                WHERE league_id = ? 
                AND home_team = ? 
                AND away_team = ? 
                AND time BETWEEN ? AND ?
                AND id != ?
                LIMIT 1
            """,
                (
                    league_id,
                    home_team,
                    away_team,
                    time_start,
                    time_end,
                    event.get("id"),
                ),
            )

            result = cursor.fetchone()
            conn.close()

            return result[0] if result else None

        except Exception as e:
            logger.error(f"Erro ao buscar evento similar: {e}")
            return None

    def event_exists(self, event_id: str) -> bool:
        """Verifica se um evento já existe no banco de dados"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()

            cursor.execute("SELECT id FROM events WHERE id = ?", (event_id,))
            result = cursor.fetchone()

            conn.close()
            return result is not None

        except Exception as e:
            logger.error(f"Erro ao verificar evento: {e}")
            return False

    def event_has_odds(self, event_id: str) -> bool:
        """Verifica se um evento já tem odds processadas"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()

            cursor.execute(
                "SELECT odds_processed FROM events WHERE id = ?", (event_id,)
            )
            result = cursor.fetchone()

            conn.close()
            return result and result[0] == 1

        except Exception as e:
            logger.error(f"Erro ao verificar odds: {e}")
            return False

    def save_event(self, event: dict) -> bool:
        """Salva um evento no banco de dados, retorna True se foi inserido novo"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()

            event_id = event.get("id")

            # Converter event_time para inteiro se necessário
            event_time = event.get("time", 0)
            if isinstance(event_time, str):
                try:
                    event_time = int(event_time)
                except (ValueError, TypeError):
                    event_time = 0

            # Verificar se já existe por ID
            cursor.execute("SELECT id FROM events WHERE id = ?", (event_id,))
            existing_event = cursor.fetchone()

            if existing_event:
                cursor.execute(
                    """
                    UPDATE events 
                    SET time = ?, time_status = ?, league_id = ?, league_name = ?, 
                        home_team = ?, away_team = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """,
                    (
                        event_time,
                        event.get("time_status", 0),
                        event.get("league_id"),
                        event.get("league_name", ""),
                        event.get("home", {}).get("name", ""),
                        event.get("away", {}).get("name", ""),
                        event_id,
                    ),
                )
                conn.commit()
                self.existing_events_count += 1
                logger.debug(f"Evento atualizado: {event_id}")
                return False

            # Verificar se é duplicata por confronto e horário
            similar_event_id = self.find_similar_event(event)
            if similar_event_id:
                self.duplicate_events_count += 1
                logger.warning(
                    f"Evento duplicado ignorado: {event.get('home', {}).get('name', '')} vs {event.get('away', {}).get('name', '')} (similar to {similar_event_id})"
                )
                return False

            # Inserir novo evento
            cursor.execute(
                """
                INSERT INTO events 
                (id, time, time_status, league_id, league_name, home_team, away_team)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    event_id,
                    event_time,
                    event.get("time_status", 0),
                    event.get("league_id"),
                    event.get("league_name", ""),
                    event.get("home", {}).get("name", ""),
                    event.get("away", {}).get("name", ""),
                ),
            )
            conn.commit()
            self.new_events_count += 1
            logger.info(
                f"Novo evento: {event.get('home', {}).get('name', '')} vs {event.get('away', {}).get('name', '')}"
            )
            return True

        except Exception as e:
            logger.error(f"Erro ao salvar evento: {e}")
            return False
        finally:
            conn.close()

    def mark_event_processed(self, event_id: str):
        """Marca um evento como tendo odds processadas"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()

            cursor.execute(
                """
                UPDATE events 
                SET odds_processed = 1, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """,
                (event_id,),
            )
            conn.commit()
        except Exception as e:
            logger.error(f"Erro ao marcar evento: {e}")
        finally:
            conn.close()

    def extract_important_odds(self, odds_data: dict) -> dict:
        """Extrai apenas as odds importantes da resposta da API"""
        important_odds = {"match_lines": {"odds": []}, "1st_game": {"odds": []}}

        sections = {
            "game": odds_data.get("game", {}),
            "main": odds_data.get("main", {}),
            "match": odds_data.get("match", {}),
            "schedule": odds_data.get("schedule", {}),
        }

        others = odds_data.get("others", [])
        for other in others:
            if "sp" in other:
                sections.update(other["sp"])

        for section_name, section_data in sections.items():
            if not section_data or "sp" not in section_data:
                continue

            sp_data = section_data["sp"]

            for market_id, market_data in sp_data.items():
                if market_id == "match_lines" and "odds" in market_data:
                    important_odds["match_lines"]["odds"].extend(market_data["odds"])

                if market_id == "1st_game" and "odds" in market_data:
                    important_odds["1st_game"]["odds"].extend(market_data["odds"])

        return important_odds

    def save_important_odds(self, event_id: str, odds_data: dict) -> bool:
        """Salva apenas as odds importantes no banco de dados, retorna True se salvou novas odds"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        try:
            important_odds = self.extract_important_odds(odds_data)
            odds_saved = False

            # Processar odds da partida (match_lines)
            for outcome in important_odds["match_lines"].get("odds", []):
                if outcome.get("name") == "To Win":
                    selection = "Home" if outcome.get("header") == "1" else "Away"
                    try:
                        cursor.execute(
                            """
                            INSERT OR IGNORE INTO match_odds 
                            (event_id, market_type, selection, odds, handicap_value, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """,
                            (
                                event_id,
                                "To Win",
                                selection,
                                float(outcome.get("odds", 0)),
                                "",
                                datetime.now().timestamp(),
                            ),
                        )
                        if cursor.rowcount > 0:
                            odds_saved = True
                            self.new_odds_count += 1
                        else:
                            self.existing_odds_count += 1
                    except sqlite3.IntegrityError:
                        self.existing_odds_count += 1
                        pass

                elif outcome.get("name") == "Total":
                    selection = "Over" if outcome.get("header") == "1" else "Under"
                    try:
                        cursor.execute(
                            """
                            INSERT OR IGNORE INTO match_odds 
                            (event_id, market_type, selection, odds, handicap_value, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """,
                            (
                                event_id,
                                "Total",
                                f"{selection} {outcome.get('handicap', '')}",
                                float(outcome.get("odds", 0)),
                                outcome.get("handicap", ""),
                                datetime.now().timestamp(),
                            ),
                        )
                        if cursor.rowcount > 0:
                            odds_saved = True
                            self.new_odds_count += 1
                        else:
                            self.existing_odds_count += 1
                    except sqlite3.IntegrityError:
                        self.existing_odds_count += 1
                        pass

                elif outcome.get("name") == "Handicap":
                    selection = "Home" if outcome.get("header") == "1" else "Away"
                    try:
                        cursor.execute(
                            """
                            INSERT OR IGNORE INTO match_odds 
                            (event_id, market_type, selection, odds, handicap_value, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """,
                            (
                                event_id,
                                "Handicap",
                                selection,
                                float(outcome.get("odds", 0)),
                                outcome.get("handicap", ""),
                                datetime.now().timestamp(),
                            ),
                        )
                        if cursor.rowcount > 0:
                            odds_saved = True
                            self.new_odds_count += 1
                        else:
                            self.existing_odds_count += 1
                    except sqlite3.IntegrityError:
                        self.existing_odds_count += 1
                        pass

            # Processar odds do primeiro game (1st_game)
            for outcome in important_odds["1st_game"].get("odds", []):
                if outcome.get("name") == "To Win":
                    selection = "Home" if outcome.get("header") == "1" else "Away"
                    try:
                        cursor.execute(
                            """
                            INSERT OR IGNORE INTO first_game_odds 
                            (event_id, market_type, selection, odds, handicap_value, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """,
                            (
                                event_id,
                                "To Win",
                                selection,
                                float(outcome.get("odds", 0)),
                                "",
                                datetime.now().timestamp(),
                            ),
                        )
                        if cursor.rowcount > 0:
                            odds_saved = True
                            self.new_odds_count += 1
                        else:
                            self.existing_odds_count += 1
                    except sqlite3.IntegrityError:
                        self.existing_odds_count += 1
                        pass

                elif outcome.get("name") == "Total":
                    selection = "Over" if outcome.get("header") == "1" else "Under"
                    try:
                        cursor.execute(
                            """
                            INSERT OR IGNORE INTO first_game_odds 
                            (event_id, market_type, selection, odds, handicap_value, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """,
                            (
                                event_id,
                                "Total",
                                f"{selection} {outcome.get('handicap', '')}",
                                float(outcome.get("odds", 0)),
                                outcome.get("handicap", ""),
                                datetime.now().timestamp(),
                            ),
                        )
                        if cursor.rowcount > 0:
                            odds_saved = True
                            self.new_odds_count += 1
                        else:
                            self.existing_odds_count += 1
                    except sqlite3.IntegrityError:
                        self.existing_odds_count += 1
                        pass

                elif outcome.get("name") == "Handicap":
                    selection = "Home" if outcome.get("header") == "1" else "Away"
                    try:
                        cursor.execute(
                            """
                            INSERT OR IGNORE INTO first_game_odds 
                            (event_id, market_type, selection, odds, handicap_value, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """,
                            (
                                event_id,
                                "Handicap",
                                selection,
                                float(outcome.get("odds", 0)),
                                outcome.get("handicap", ""),
                                datetime.now().timestamp(),
                            ),
                        )
                        if cursor.rowcount > 0:
                            odds_saved = True
                            self.new_odds_count += 1
                        else:
                            self.existing_odds_count += 1
                    except sqlite3.IntegrityError:
                        self.existing_odds_count += 1
                        pass

            conn.commit()

            if odds_saved:
                self.mark_event_processed(event_id)
                logger.info(f"Odds salvas para: {event_id}")
            else:
                logger.debug(f"Odds já existiam para: {event_id}")

            return odds_saved

        except Exception as e:
            logger.error(f"Erro ao salvar odds: {e}")
            return False
        finally:
            conn.close()


class TableTennisMonitor:
    def __init__(self):
        self.client = Bet365Client()
        self.db = SimplifiedDatabase()
        self.sport_id = 92
        self.leagues = {
            10048210: "Czech Liga Pro",
            10068516: "Challenger Series TT",
            10073432: "TT Cup",
            10073465: "TT Elite Series",
        }
        self.processed_events = set()

    async def get_upcoming_matches(self, days_ahead: int = 7) -> List[Dict]:
        """Busca partidas futuras para todas as ligas especificadas, evitando duplicatas"""
        all_matches = []
        seen_event_ids = set()

        for league_id, league_name in self.leagues.items():
            logger.info(f"Verificando liga: {league_name}")
            league_events = 0
            league_pages = 0
            league_days = 0

            try:
                for i in range(days_ahead):
                    day = (datetime.now() + timedelta(days=i)).strftime("%Y%m%d")
                    page = 1
                    has_more_pages = True
                    day_events = 0

                    while has_more_pages:
                        response = await self.client.upcoming(
                            sport_id=self.sport_id,
                            league_id=league_id,
                            day=day,
                            page=page,
                        )

                        if not response.get("success", 1) or "results" not in response:
                            logger.debug(f"  → Sem resultados para {day} página {page}")
                            break

                        results = response["results"]
                        if not results:
                            logger.debug(
                                f"  → Resultados vazios para {day} página {page}"
                            )
                            break

                        new_events = []
                        for event in results:
                            event_id = event.get("id")

                            if event_id in seen_event_ids:
                                continue

                            if self.db.event_exists(
                                event_id
                            ) and self.db.event_has_odds(event_id):
                                continue

                            event["league_name"] = league_name
                            event["league_id"] = league_id
                            new_events.append(event)
                            seen_event_ids.add(event_id)
                            league_events += 1
                            day_events += 1

                        all_matches.extend(new_events)
                        league_pages += 1

                        pager = response.get("pager", {})
                        if page >= pager.get("total", 1):
                            has_more_pages = False
                        else:
                            page += 1
                            await asyncio.sleep(0.3)

                    if day_events > 0:
                        logger.info(f"  → Dia {day}: {day_events} eventos")
                        league_days += 1

            except Exception as e:
                logger.error(f"Erro na liga {league_name}: {e}")
                continue

            if league_events > 0:
                logger.info(
                    f"  → {league_events} eventos encontrados em {league_name} ({league_days} dias, {league_pages} páginas)"
                )
            else:
                logger.info(f"  → Nenhum evento novo em {league_name}")

        return all_matches

    async def get_prematch_odds(self, event_id: str) -> Optional[Dict]:
        """Busca as odds prematch para um evento específico"""
        try:
            response = await self.client.prematch(FI=event_id)

            if response.get("success", 1) and "results" in response:
                return response["results"][0] if response["results"] else None
            else:
                logger.warning(f"Resposta vazia para odds do evento {event_id}")
                return None

        except Exception as e:
            logger.error(f"Erro ao buscar odds: {e}")
            return None

    async def process_event(self, match: Dict):
        """Processa um único evento (salva evento e busca odds)"""
        event_id = match["id"]

        if event_id in self.processed_events:
            return

        try:
            is_new_event = self.db.save_event(match)

            if not is_new_event and self.db.event_has_odds(event_id):
                logger.debug(f"Evento já processado: {event_id}")
                self.processed_events.add(event_id)
                return

            odds_data = await self.get_prematch_odds(event_id)
            if odds_data:
                odds_saved = self.db.save_important_odds(event_id, odds_data)
                if not odds_saved:
                    logger.debug(f"Odds já existiam para: {event_id}")
            else:
                logger.warning(f"Sem odds disponíveis para: {event_id}")

            self.processed_events.add(event_id)

        except Exception as e:
            logger.error(f"Erro no evento {event_id}: {e}")

    async def monitor_and_save_odds(self, days_ahead: int = 7):
        """Monitora jogos upcoming e salva suas odds no banco de dados"""
        logger.info("🚀 Iniciando monitoramento de tênis de mesa")
        logger.info(f"📊 Ligas monitoradas: {len(self.leagues)}")
        logger.info(f"📅 Dias analisados: {days_ahead}")

        matches = await self.get_upcoming_matches(days_ahead)

        if not matches:
            logger.info("✅ Nenhuma partida nova encontrada")
            return

        logger.info(f"🎯 {len(matches)} partidas novas para processar")

        # Processar eventos
        tasks = [self.process_event(match) for match in matches]
        await asyncio.gather(*tasks)

        # Exibir estatísticas
        stats = self.db.get_stats()
        logger.info("📈 Estatísticas da sessão:")
        logger.info(f"   → Novos eventos: {stats['new_events']}")
        logger.info(f"   → Eventos existentes: {stats['existing_events']}")
        logger.info(f"   → Eventos duplicados: {stats['duplicate_events']}")
        logger.info(f"   → Novas odds: {stats['new_odds']}")
        logger.info(f"   → Odds existentes: {stats['existing_odds']}")
        logger.info(f"   → Requisições API: {self.client.requests_count}")

        logger.info("✅ Monitoramento concluído com sucesso")

    async def close(self):
        await self.client.close()


async def main():
    monitor = TableTennisMonitor()

    try:
        await monitor.monitor_and_save_odds(days_ahead=3)
    except Exception as e:
        logger.error(f"Erro no monitoramento: {e}")
    finally:
        await monitor.close()


if __name__ == "__main__":
    asyncio.run(main())
