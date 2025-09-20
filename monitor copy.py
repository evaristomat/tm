import httpx
import asyncio
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
import os
from dotenv import load_dotenv
import json
from contextlib import asynccontextmanager

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


class DatabaseError(Exception):
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
        self.max_concurrent_requests = int(os.getenv("MAX_CONCURRENT_REQUESTS", "10"))
        self.retry_attempts = int(os.getenv("RETRY_ATTEMPTS", "3"))
        self.retry_delay = float(os.getenv("RETRY_DELAY", "1.0"))
        self.client = httpx.AsyncClient(timeout=self.request_timeout)
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        self.requests_count = 0

        if not self.api_key:
            logger.error("API Key não encontrada! Variáveis disponíveis:")
            for key in os.environ.keys():
                if "API" in key.upper() or "KEY" in key.upper():
                    logger.error(f"  - {key}")
            raise BetsAPIError("API Key não configurada")
        else:
            logger.info(f"API Key carregada: {self.api_key[:10]}...")

    async def _make_request(
        self, endpoint: str, params: dict = None, version: str = "v1"
    ) -> dict:
        if params is None:
            params = {}

        params["token"] = self.api_key

        base_url = self.base_url if version == "v1" else self.base_url_v3
        url = f"{base_url}/{endpoint}"

        for attempt in range(self.retry_attempts):
            try:
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

            except (httpx.HTTPError, RateLimitError) as e:
                if attempt == self.retry_attempts - 1:
                    raise BetsAPIError(
                        f"Request failed after {self.retry_attempts} attempts: {str(e)}"
                    )

                wait_time = self.retry_delay * (2**attempt)  # Exponential backoff
                logger.warning(
                    f"Attempt {attempt + 1} failed. Retrying in {wait_time}s: {str(e)}"
                )
                await asyncio.sleep(wait_time)

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


class DatabaseManager:
    def __init__(self, db_name: str = "tm_data.db"):
        self.db_name = db_name
        self.conn = None
        self.cache_existing_events: Set[str] = set()
        self.cache_events_with_odds: Set[str] = set()
        self.init_database()
        self.load_event_cache()

    def init_database(self):
        """Inicializa o banco de dados com tabelas otimizadas"""
        try:
            self.conn = sqlite3.connect(self.db_name)
            cursor = self.conn.cursor()

            # Criar tabelas se não existirem
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

            # Criar índices para melhorar performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_time ON events(time)")
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_league ON events(league_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_odds_processed ON events(odds_processed)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_match_odds_event ON match_odds(event_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_first_game_odds_event ON first_game_odds(event_id)"
            )

            self.conn.commit()
            logger.info("Banco de dados inicializado com sucesso")

        except Exception as e:
            logger.error(f"Erro ao inicializar banco de dados: {e}")
            raise DatabaseError(f"Falha na inicialização do banco: {e}")

    def load_event_cache(self):
        """Carrega cache de eventos existentes para evitar consultas repetidas ao banco"""
        try:
            cursor = self.conn.cursor()

            # Carregar todos os IDs de eventos
            cursor.execute("SELECT id FROM events")
            self.cache_existing_events = {row[0] for row in cursor.fetchall()}

            # Carregar IDs de eventos com odds processadas
            cursor.execute("SELECT id FROM events WHERE odds_processed = 1")
            self.cache_events_with_odds = {row[0] for row in cursor.fetchall()}

            logger.info(
                f"Cache carregado: {len(self.cache_existing_events)} eventos, {len(self.cache_events_with_odds)} com odds"
            )

        except Exception as e:
            logger.error(f"Erro ao carregar cache: {e}")

    def event_exists(self, event_id: str) -> bool:
        """Verifica se um evento já existe usando cache"""
        return event_id in self.cache_existing_events

    def event_has_odds(self, event_id: str) -> bool:
        """Verifica se um evento já tem odds processadas usando cache"""
        return event_id in self.cache_events_with_odds

    def find_similar_event(
        self, event: dict, time_threshold_hours: int = 6
    ) -> Optional[str]:
        """Procura por evento similar (mesmo confronto em período próximo)"""
        try:
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

            cursor = self.conn.cursor()
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
            return result[0] if result else None

        except Exception as e:
            logger.error(f"Erro ao buscar evento similar: {e}")
            return None

    def save_events_batch(self, events: List[dict]) -> Tuple[int, int]:
        """Salva múltiplos eventos em lote, retorna (novos, atualizados)"""
        if not events:
            return 0, 0

        new_count = 0
        updated_count = 0

        try:
            cursor = self.conn.cursor()

            for event in events:
                event_id = event.get("id")
                if not event_id:
                    continue

                # Converter event_time para inteiro se necessário
                event_time = event.get("time", 0)
                if isinstance(event_time, str):
                    try:
                        event_time = int(event_time)
                    except (ValueError, TypeError):
                        event_time = 0

                # Verificar se já existe
                if event_id in self.cache_existing_events:
                    # Atualizar evento existente
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
                    updated_count += 1
                    logger.debug(f"Evento atualizado: {event_id}")
                else:
                    # Verificar se é duplicata por confronto e horário
                    similar_event_id = self.find_similar_event(event)
                    if similar_event_id:
                        logger.warning(
                            f"Evento duplicado ignorado: {event.get('home', {}).get('name', '')} vs {event.get('away', {}).get('name', '')} (similar to {similar_event_id})"
                        )
                        continue

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
                    self.cache_existing_events.add(event_id)
                    new_count += 1
                    logger.info(
                        f"Novo evento: {event.get('home', {}).get('name', '')} vs {event.get('away', {}).get('name', '')}"
                    )

            self.conn.commit()
            return new_count, updated_count

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erro ao salvar eventos em lote: {e}")
            return new_count, updated_count

    def mark_events_processed(self, event_ids: List[str]):
        """Marca múltiplos eventos como tendo odds processadas"""
        if not event_ids:
            return

        try:
            cursor = self.conn.cursor()
            placeholders = ",".join(["?"] * len(event_ids))

            cursor.execute(
                f"""
                UPDATE events 
                SET odds_processed = 1, updated_at = CURRENT_TIMESTAMP
                WHERE id IN ({placeholders})
            """,
                event_ids,
            )

            # Atualizar cache
            for event_id in event_ids:
                self.cache_events_with_odds.add(event_id)

            self.conn.commit()
        except Exception as e:
            logger.error(f"Erro ao marcar eventos como processados: {e}")

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

    def save_odds_batch(self, event_odds: List[Tuple[str, dict]]) -> int:
        """Salva múltiplas odds em lote, retorna o número de novas odds salvas"""
        if not event_odds:
            return 0

        new_odds_count = 0
        processed_events = []

        try:
            cursor = self.conn.cursor()
            current_timestamp = datetime.now().timestamp()

            for event_id, odds_data in event_odds:
                important_odds = self.extract_important_odds(odds_data)
                odds_to_insert = []

                # Processar odds da partida (match_lines)
                for outcome in important_odds["match_lines"].get("odds", []):
                    if outcome.get("name") == "To Win":
                        selection = "Home" if outcome.get("header") == "1" else "Away"
                        odds_to_insert.append(
                            (
                                event_id,
                                "To Win",
                                selection,
                                float(outcome.get("odds", 0)),
                                "",
                                current_timestamp,
                                "match_odds",
                            )
                        )

                    elif outcome.get("name") == "Total":
                        selection = "Over" if outcome.get("header") == "1" else "Under"
                        odds_to_insert.append(
                            (
                                event_id,
                                "Total",
                                f"{selection} {outcome.get('handicap', '')}",
                                float(outcome.get("odds", 0)),
                                outcome.get("handicap", ""),
                                current_timestamp,
                                "match_odds",
                            )
                        )

                    elif outcome.get("name") == "Handicap":
                        selection = "Home" if outcome.get("header") == "1" else "Away"
                        odds_to_insert.append(
                            (
                                event_id,
                                "Handicap",
                                selection,
                                float(outcome.get("odds", 0)),
                                outcome.get("handicap", ""),
                                current_timestamp,
                                "match_odds",
                            )
                        )

                # Processar odds do primeiro game (1st_game)
                for outcome in important_odds["1st_game"].get("odds", []):
                    if outcome.get("name") == "To Win":
                        selection = "Home" if outcome.get("header") == "1" else "Away"
                        odds_to_insert.append(
                            (
                                event_id,
                                "To Win",
                                selection,
                                float(outcome.get("odds", 0)),
                                "",
                                current_timestamp,
                                "first_game_odds",
                            )
                        )

                    elif outcome.get("name") == "Total":
                        selection = "Over" if outcome.get("header") == "1" else "Under"
                        odds_to_insert.append(
                            (
                                event_id,
                                "Total",
                                f"{selection} {outcome.get('handicap', '')}",
                                float(outcome.get("odds", 0)),
                                outcome.get("handicap", ""),
                                current_timestamp,
                                "first_game_odds",
                            )
                        )

                    elif outcome.get("name") == "Handicap":
                        selection = "Home" if outcome.get("header") == "1" else "Away"
                        odds_to_insert.append(
                            (
                                event_id,
                                "Handicap",
                                selection,
                                float(outcome.get("odds", 0)),
                                outcome.get("handicap", ""),
                                current_timestamp,
                                "first_game_odds",
                            )
                        )

                # Inserir odds em lote por tipo de tabela
                match_odds = [o for o in odds_to_insert if o[6] == "match_odds"]
                first_game_odds = [
                    o for o in odds_to_insert if o[6] == "first_game_odds"
                ]

                if match_odds:
                    cursor.executemany(
                        """
                        INSERT OR IGNORE INTO match_odds 
                        (event_id, market_type, selection, odds, handicap_value, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        [(o[0], o[1], o[2], o[3], o[4], o[5]) for o in match_odds],
                    )
                    new_odds_count += cursor.rowcount

                if first_game_odds:
                    cursor.executemany(
                        """
                        INSERT OR IGNORE INTO first_game_odds 
                        (event_id, market_type, selection, odds, handicap_value, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        [(o[0], o[1], o[2], o[3], o[4], o[5]) for o in first_game_odds],
                    )
                    new_odds_count += cursor.rowcount

                if odds_to_insert:
                    processed_events.append(event_id)

            # Marcar eventos como processados
            if processed_events:
                self.mark_events_processed(processed_events)
                logger.info(f"Odds salvas para {len(processed_events)} eventos")

            self.conn.commit()
            return new_odds_count

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erro ao salvar odds em lote: {e}")
            return new_odds_count

    def close(self):
        """Fecha a conexão com o banco de dados"""
        if self.conn:
            self.conn.close()


class TableTennisMonitor:
    def __init__(self):
        self.client = Bet365Client()
        self.db = DatabaseManager()
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

                            # Pular eventos já vistos ou processados
                            if (
                                event_id in seen_event_ids
                                or event_id in self.processed_events
                                or (
                                    self.db.event_exists(event_id)
                                    and self.db.event_has_odds(event_id)
                                )
                            ):
                                continue

                            event["league_name"] = league_name
                            event["league_id"] = league_id
                            new_events.append(event)
                            seen_event_ids.add(event_id)
                            league_events += 1
                            day_events += 1

                        all_matches.extend(new_events)

                        pager = response.get("pager", {})
                        if page >= pager.get("total", 1) or not results:
                            has_more_pages = False
                        else:
                            page += 1
                            await asyncio.sleep(0.1)  # Reduzido para melhor performance

                    if day_events > 0:
                        logger.info(f"  → Dia {day}: {day_events} eventos")

            except Exception as e:
                logger.error(f"Erro na liga {league_name}: {e}")
                continue

            if league_events > 0:
                logger.info(f"  → {league_events} eventos encontrados em {league_name}")
            else:
                logger.info(f"  → Nenhum evento novo em {league_name}")

        return all_matches

    async def get_prematch_odds_batch(
        self, event_ids: List[str]
    ) -> List[Tuple[str, Optional[Dict]]]:
        """Busca as odds prematch para múltiplos eventos de forma concorrente"""
        if not event_ids:
            return []

        results = []
        tasks = []

        for event_id in event_ids:
            tasks.append(self._get_single_prematch_odds(event_id))

        # Executar tarefas em lote com semáforo controlado pelo cliente
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Processar resultados
        processed_results = []
        for event_id, result in zip(event_ids, results):
            if isinstance(result, Exception):
                logger.error(f"Erro ao buscar odds para {event_id}: {result}")
                processed_results.append((event_id, None))
            else:
                processed_results.append((event_id, result))

        return processed_results

    async def _get_single_prematch_odds(self, event_id: str) -> Optional[Dict]:
        """Busca as odds prematch para um evento específico"""
        try:
            response = await self.client.prematch(FI=event_id)

            if response.get("success", 1) and "results" in response:
                return response["results"][0] if response["results"] else None
            else:
                logger.warning(f"Resposta vazia para odds do evento {event_id}")
                return None

        except Exception as e:
            logger.error(f"Erro ao buscar odds para {event_id}: {e}")
            return None

    async def process_events_batch(self, matches: List[Dict]):
        """Processa múltiplos eventos em lote (salva eventos e busca odds)"""
        if not matches:
            return

        # Filtrar eventos já processados
        new_matches = [
            match
            for match in matches
            if match["id"] not in self.processed_events
            and not (
                self.db.event_exists(match["id"])
                and self.db.event_has_odds(match["id"])
            )
        ]

        if not new_matches:
            logger.info("Nenhum evento novo para processar")
            return

        # Salvar eventos em lote
        new_count, updated_count = self.db.save_events_batch(new_matches)
        logger.info(f"Eventos salvos: {new_count} novos, {updated_count} atualizados")

        # Buscar odds apenas para eventos novos (não existentes ou sem odds)
        events_needing_odds = [
            match["id"]
            for match in new_matches
            if not self.db.event_has_odds(match["id"])
        ]

        if events_needing_odds:
            logger.info(f"Buscando odds para {len(events_needing_odds)} eventos")
            odds_results = await self.get_prematch_odds_batch(events_needing_odds)

            # Filtrar resultados válidos
            valid_odds = [
                (event_id, odds) for event_id, odds in odds_results if odds is not None
            ]

            if valid_odds:
                # Salvar odds em lote
                new_odds_count = self.db.save_odds_batch(valid_odds)
                logger.info(f"Salvas {new_odds_count} novas odds")
            else:
                logger.warning("Nenhuma odds válida encontrada")

        # Adicionar eventos processados ao conjunto
        for match in new_matches:
            self.processed_events.add(match["id"])

    async def monitor_and_save_odds(self, days_ahead: int = 7):
        """Monitora jogos upcoming e salva suas odds no banco de dados"""
        logger.info("🚀 Iniciando monitoramento de tênis de mesa")
        logger.info(f"📊 Ligas monitoradas: {len(self.leagues)}")
        logger.info(f"📅 Dias analisados: {days_ahead}")

        matches = await self.get_upcoming_matches(days_ahead)

        if not matches:
            logger.info("✅ Nenhuma partida nova encontrada")
            return

        logger.info(f"🎯 {len(matches)} partidas para processar")

        # Processar eventos em lote
        await self.process_events_batch(matches)

        logger.info(
            f"✅ Monitoramento concluído. Requisições API: {self.client.requests_count}"
        )

    async def close(self):
        await self.client.close()
        self.db.close()


@asynccontextmanager
async def monitor_context():
    """Context manager para gerenciar recursos do monitor"""
    monitor = TableTennisMonitor()
    try:
        yield monitor
    finally:
        await monitor.close()


async def main():
    async with monitor_context() as monitor:
        try:
            await monitor.monitor_and_save_odds(days_ahead=3)
        except Exception as e:
            logger.error(f"Erro no monitoramento: {e}")


if __name__ == "__main__":
    asyncio.run(main())
