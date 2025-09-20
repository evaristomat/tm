import httpx
import asyncio
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
import os
from dotenv import load_dotenv
import time

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
        self.api_key = (
            os.getenv("API_KEY")
            or os.getenv("BETSAPI_API_KEY")
            or os.getenv("BETS_API_KEY")
        )
        self.request_timeout = int(os.getenv("REQUEST_TIMEOUT", "30"))
        self.max_concurrent_requests = int(os.getenv("MAX_CONCURRENT_REQUESTS", "5"))
        self.retry_attempts = int(os.getenv("RETRY_ATTEMPTS", "3"))
        self.retry_delay = float(os.getenv("RETRY_DELAY", "2.0"))
        self.client = httpx.AsyncClient(timeout=self.request_timeout)
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        self.requests_count = 0
        self.failed_requests = 0

        if not self.api_key:
            logger.error("API Key não encontrada!")
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

            except (httpx.HTTPError, RateLimitError, BetsAPIError) as e:
                if attempt == self.retry_attempts - 1:
                    self.failed_requests += 1
                    raise BetsAPIError(
                        f"Request failed after {self.retry_attempts} attempts: {str(e)}"
                    )

                wait_time = self.retry_delay * (2**attempt)
                logger.warning(
                    f"Attempt {attempt + 1} failed. Retrying in {wait_time}s: {str(e)}"
                )
                await asyncio.sleep(wait_time)
            except Exception as e:
                self.failed_requests += 1
                raise BetsAPIError(f"Unexpected error: {str(e)}")

    async def upcoming(
        self,
        sport_id: int,
        league_id: int = None,
        day: str = None,
        page: int = 1,
    ) -> dict:
        params = {"sport_id": sport_id, "page": page}
        if league_id:
            params["league_id"] = league_id
        if day:
            params["day"] = day
        return await self._make_request("bet365/upcoming", params, "v1")

    async def prematch(self, FI: str) -> dict:
        params = {"FI": FI}
        return await self._make_request("bet365/prematch", params, "v3")

    async def close(self):
        await self.client.aclose()


class DatabaseManager:
    def __init__(self, db_name: str = "tm_data.db"):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        self.cache_existing_events: Set[str] = set()
        self.cache_events_with_odds: Set[str] = set()
        self.cache_event_teams: Dict[str, Tuple[str, str]] = {}
        self.init_database()
        self.load_event_cache()

    def init_database(self):
        """Inicializa o banco de dados com tabelas otimizadas"""
        try:
            cursor = self.conn.cursor()

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
                "CREATE INDEX IF NOT EXISTS idx_events_teams ON events(home_team, away_team)"
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
            cursor.execute("SELECT id, home_team, away_team FROM events")
            for row in cursor.fetchall():
                self.cache_existing_events.add(row[0])
                self.cache_event_teams[row[0]] = (row[1], row[2])

            # Carregar IDs de eventos com odds processadas
            cursor.execute("SELECT id FROM events WHERE odds_processed = 1")
            self.cache_events_with_odds = {row[0] for row in cursor.fetchall()}

            logger.info(
                f"Cache carregado: {len(self.cache_existing_events)} eventos, {len(self.cache_events_with_odds)} com odds"
            )

        except Exception as e:
            logger.error(f"Erro ao carregar cache: {e}")

    def is_duplicate_event(self, event: dict, time_threshold_hours: int = 6) -> bool:
        """
        Verifica se um evento é duplicado baseado em times and liga em um período de tempo próximo
        Um evento é considerado duplicado se tiver o mesmo confronto (times) na mesma liga
        em um período de tempo próximo, mas com ID diferente
        """
        try:
            event_id = event.get("id")
            home_team = event.get("home", {}).get("name", "").strip()
            away_team = event.get("away", {}).get("name", "").strip()
            league_id = event.get("league_id")
            event_time = event.get("time", 0)

            if isinstance(event_time, str):
                try:
                    event_time = int(event_time)
                except (ValueError, TypeError):
                    event_time = 0

            # Se não temos informações suficientes, não consideramos duplicado
            if not home_team or not away_team or not league_id or event_time == 0:
                return False

            # Verificar se já existe um evento com o mesmo ID
            if event_id in self.cache_existing_events:
                return False  # Não é duplicado, é o mesmo evento

            # Verificar se existe evento com mesmo confronto na mesma liga em período próximo
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
                (league_id, home_team, away_team, time_start, time_end, event_id),
            )

            result = cursor.fetchone()
            return result is not None

        except Exception as e:
            logger.error(f"Erro ao verificar evento duplicado: {e}")
            return False

    def save_events_batch(self, events: List[dict]) -> Tuple[int, int, int]:
        """Salva múltiplos eventos em lote, retorna (novos, atualizados, duplicados)"""
        if not events:
            return 0, 0, 0

        new_count = 0
        updated_count = 0
        duplicate_count = 0

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

                home_team = event.get("home", {}).get("name", "").strip()
                away_team = event.get("away", {}).get("name", "").strip()
                league_id = event.get("league_id")
                league_name = event.get("league_name", "")

                # Verificar se já existe por ID
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
                            league_id,
                            league_name,
                            home_team,
                            away_team,
                            event_id,
                        ),
                    )
                    updated_count += 1
                else:
                    # Verificar se é duplicado por confronto e horário
                    if self.is_duplicate_event(event):
                        duplicate_count += 1
                        logger.debug(
                            f"Evento duplicado ignorado: {home_team} vs {away_team}"
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
                            league_id,
                            league_name,
                            home_team,
                            away_team,
                        ),
                    )
                    self.cache_existing_events.add(event_id)
                    self.cache_event_teams[event_id] = (home_team, away_team)
                    new_count += 1
                    logger.info(f"Novo evento: {home_team} vs {away_team}")

            self.conn.commit()
            return new_count, updated_count, duplicate_count

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erro ao salvar eventos em lote: {e}")
            return new_count, updated_count, duplicate_count

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

        # Verificar se há dados válidos
        if not odds_data or not isinstance(odds_data, dict):
            return important_odds

        # Verificar seções principais
        sections = {}
        for section_key in ["game", "main", "match", "schedule"]:
            if section_key in odds_data:
                sections[section_key] = odds_data[section_key]

        # Verificar outras seções
        others = odds_data.get("others", [])
        for other in others:
            if isinstance(other, dict) and "sp" in other:
                sections.update(other["sp"])

        # Processar seções
        for section_name, section_data in sections.items():
            if (
                not section_data
                or not isinstance(section_data, dict)
                or "sp" not in section_data
            ):
                continue

            sp_data = section_data["sp"]
            if not isinstance(sp_data, dict):
                continue

            for market_id, market_data in sp_data.items():
                if not isinstance(market_data, dict):
                    continue

                if (
                    market_id == "match_lines"
                    and "odds" in market_data
                    and isinstance(market_data["odds"], list)
                ):
                    important_odds["match_lines"]["odds"].extend(market_data["odds"])

                if (
                    market_id == "1st_game"
                    and "odds" in market_data
                    and isinstance(market_data["odds"], list)
                ):
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
                if not odds_data:
                    continue

                important_odds = self.extract_important_odds(odds_data)
                odds_to_insert = []

                # Processar odds da partida (match_lines)
                for outcome in important_odds["match_lines"].get("odds", []):
                    if not isinstance(outcome, dict):
                        continue

                    market_name = outcome.get("name", "")
                    header = outcome.get("header", "")
                    odds_value = outcome.get("odds", 0)
                    handicap = outcome.get("handicap", "")

                    try:
                        odds_float = float(odds_value) if odds_value else 0
                    except (ValueError, TypeError):
                        odds_float = 0

                    if market_name == "To Win" and header in ["1", "2"]:
                        selection = "Home" if header == "1" else "Away"
                        odds_to_insert.append(
                            (
                                event_id,
                                "To Win",
                                selection,
                                odds_float,
                                "",
                                current_timestamp,
                                "match_odds",
                            )
                        )

                    elif market_name == "Total" and header in ["1", "2"] and handicap:
                        selection = "Over" if header == "1" else "Under"
                        odds_to_insert.append(
                            (
                                event_id,
                                "Total",
                                f"{selection} {handicap}",
                                odds_float,
                                handicap,
                                current_timestamp,
                                "match_odds",
                            )
                        )

                    elif (
                        market_name == "Handicap" and header in ["1", "2"] and handicap
                    ):
                        selection = "Home" if header == "1" else "Away"
                        odds_to_insert.append(
                            (
                                event_id,
                                "Handicap",
                                selection,
                                odds_float,
                                handicap,
                                current_timestamp,
                                "match_odds",
                            )
                        )

                # Processar odds do primeiro game (1st_game)
                for outcome in important_odds["1st_game"].get("odds", []):
                    if not isinstance(outcome, dict):
                        continue

                    market_name = outcome.get("name", "")
                    header = outcome.get("header", "")
                    odds_value = outcome.get("odds", 0)
                    handicap = outcome.get("handicap", "")

                    try:
                        odds_float = float(odds_value) if odds_value else 0
                    except (ValueError, TypeError):
                        odds_float = 0

                    if market_name == "To Win" and header in ["1", "2"]:
                        selection = "Home" if header == "1" else "Away"
                        odds_to_insert.append(
                            (
                                event_id,
                                "To Win",
                                selection,
                                odds_float,
                                "",
                                current_timestamp,
                                "first_game_odds",
                            )
                        )

                    elif market_name == "Total" and header in ["1", "2"] and handicap:
                        selection = "Over" if header == "1" else "Under"
                        odds_to_insert.append(
                            (
                                event_id,
                                "Total",
                                f"{selection} {handicap}",
                                odds_float,
                                handicap,
                                current_timestamp,
                                "first_game_odds",
                            )
                        )

                    elif (
                        market_name == "Handicap" and header in ["1", "2"] and handicap
                    ):
                        selection = "Home" if header == "1" else "Away"
                        odds_to_insert.append(
                            (
                                event_id,
                                "Handicap",
                                selection,
                                odds_float,
                                handicap,
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
        self.failed_events = set()

    async def get_league_events(
        self, league_id: int, league_name: str, day: str
    ) -> List[Dict]:
        """Busca eventos para uma liga específica em um dia específico"""
        events = []
        page = 1
        max_pages = 10  # Limite de páginas para evitar loop infinito

        try:
            while page <= max_pages:
                response = await self.client.upcoming(
                    sport_id=self.sport_id, league_id=league_id, day=day, page=page
                )

                if not response.get("success", 1) or "results" not in response:
                    break

                results = response["results"]
                if not results:
                    break

                for event in results:
                    event_id = event.get("id")
                    if not event_id:
                        continue

                    # Pular eventos já processados ou que falharam
                    if (
                        event_id in self.processed_events
                        or event_id in self.failed_events
                    ):
                        continue

                    event["league_name"] = league_name
                    event["league_id"] = league_id
                    events.append(event)

                # Verificar se há mais páginas
                pager = response.get("pager", {})
                if page >= pager.get("page", page) or len(results) < pager.get(
                    "per_page", 100
                ):
                    break

                page += 1
                await asyncio.sleep(0.1)  # Pequena pausa entre páginas

        except Exception as e:
            logger.error(f"Erro ao buscar eventos para {league_name} no dia {day}: {e}")

        return events

    async def get_upcoming_matches(self, days_ahead: int = 3) -> List[Dict]:
        """Busca partidas futuras para todas as ligas especificadas"""
        all_matches = []
        total_days = min(
            days_ahead, 7
        )  # Limitar a 7 dias para evitar muitas requisições

        for league_id, league_name in self.leagues.items():
            logger.info(f"Verificando liga: {league_name}")
            league_events = 0

            for i in range(total_days):
                day = (datetime.now() + timedelta(days=i)).strftime("%Y%m%d")
                day_events = await self.get_league_events(league_id, league_name, day)

                if day_events:
                    league_events += len(day_events)
                    all_matches.extend(day_events)
                    logger.info(f"  → Dia {day}: {len(day_events)} eventos")

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
        batch_size = 10  # Tamanho menor do lote para evitar rate limiting
        delay_between_batches = 1.0  # 1 segundo entre lotes

        for i in range(0, len(event_ids), batch_size):
            batch = event_ids[i : i + batch_size]
            tasks = [self._get_single_prematch_odds(event_id) for event_id in batch]

            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Processar resultados do lote
            for event_id, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Erro ao buscar odds para {event_id}: {result}")
                    self.failed_events.add(event_id)
                    results.append((event_id, None))
                else:
                    results.append((event_id, result))

            # Aguardar entre lotes
            if i + batch_size < len(event_ids):
                await asyncio.sleep(delay_between_batches)

        return results

    async def _get_single_prematch_odds(self, event_id: str) -> Optional[Dict]:
        """Busca as odds prematch para um evento específico"""
        try:
            response = await self.client.prematch(FI=event_id)

            if (
                response.get("success", 1)
                and "results" in response
                and response["results"]
            ):
                return response["results"][0]
            else:
                logger.warning(f"Resposta vazia para odds do evento {event_id}")
                return None

        except Exception as e:
            logger.error(f"Erro ao buscar odds para {event_id}: {e}")
            raise

    async def process_events(self, matches: List[Dict]):
        """Processa múltiplos eventos (salva eventos e busca odds)"""
        if not matches:
            logger.info("Nenhum evento novo para processar")
            return

        # Salvar eventos em lote
        new_count, updated_count, duplicate_count = self.db.save_events_batch(matches)
        logger.info(
            f"Eventos salvos: {new_count} novos, {updated_count} atualizados, {duplicate_count} duplicados"
        )

        # Identificar eventos que precisam de odds (apenas os NOVOS)
        events_needing_odds = [
            match["id"]
            for match in matches
            if (
                match["id"]
                not in self.db.cache_events_with_odds  # Não tem odds processadas
                and match["id"] not in self.failed_events  # Não falhou anteriormente
                and not self.db.is_duplicate_event(match)  # Não é duplicado
                and match["id"] not in self.db.cache_existing_events
            )  # É um evento NOVO (não existia antes)
        ]

        if not events_needing_odds:
            logger.info("Nenhum evento precisa de odds")
            return

        logger.info(f"Buscando odds para {len(events_needing_odds)} eventos")

        # Buscar odds em lotes
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

        # Adicionar eventos processados
        for match in matches:
            self.processed_events.add(match["id"])

    async def monitor_and_save_odds(self, days_ahead: int = 3):
        """Monitora jogos upcoming e salva suas odds no banco de dados"""
        logger.info("🚀 Iniciando monitoramento de tênis de mesa")
        logger.info(f"📊 Ligas monitoradas: {len(self.leagues)}")
        logger.info(f"📅 Dias analisados: {days_ahead}")

        matches = await self.get_upcoming_matches(days_ahead)

        if not matches:
            logger.info("✅ Nenhuma partida nova encontrada")
            return

        logger.info(f"🎯 {len(matches)} partidas para processar")

        # Processar eventos
        await self.process_events(matches)

        logger.info(
            f"✅ Monitoramento concluído. Requisições: {self.client.requests_count}, Falhas: {self.client.failed_requests}"
        )

    async def close(self):
        await self.client.close()
        self.db.close()


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
