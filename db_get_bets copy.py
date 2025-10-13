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
K_FACTOR = 32  # Fator de ajuste do ELO. Comum em xadrez.
DEFAULT_ELO = 1500  # Rating inicial para novos jogadores.

# --- PARÂMETROS DE FILTRO DE APOSTAS (AJUSTADOS) ---
MIN_ROI_ML = 5  # ALTERADO: de 25 para 5%
MIN_EDGE = 0.01  # ALTERADO: de 0.05 para 0.01

# --- NOVOS PARÂMETROS PARA REFINAMENTO ML ---
# Threshold de diferença de ELO para considerar um jogador favorito forte
ELO_DIFFERENCE_STRONG_FAVORITE = (
    150  # Ex: Se a diferença for maior que 150, o favorito é forte
)
# Multiplicador para o Edge em apostas de favoritos fortes (para exigir um Edge maior)
EDGE_MULTIPLIER_STRONG_FAVORITE = 1.5


class BetProcessor:
    def __init__(
        self,
        tm_db_path="tm_data.db",
        bets_db_path="bets.db",
        results_db_path="table_tennis_results.db",
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
        # Calcula o ELO de todos os jogadores com base no histórico ao inicializar
        logger.info(
            "🧠 Calculando ratings ELO de todos os jogadores a partir do histórico..."
        )
        self.player_ratings = self._calculate_all_player_elos()
        logger.info(
            f"✅ Ratings ELO calculados para {len(self.player_ratings)} jogadores."
        )

    # ====================================================================
    # NOVA LÓGICA DE RATING ELO
    # ====================================================================

    def _get_expected_score(self, rating1, rating2):
        """Calcula a probabilidade de vitória do jogador 1 contra o jogador 2."""
        return 1 / (1 + 10 ** ((rating2 - rating1) / 400))

    def _update_ratings(self, rating1, rating2, score1, score2):
        """Atualiza os ratings ELO com base no resultado da partida."""
        expected1 = self._get_expected_score(rating1, rating2)
        expected2 = self._get_expected_score(rating2, rating1)

        new_rating1 = rating1 + K_FACTOR * (score1 - expected1)
        new_rating2 = rating2 + K_FACTOR * (score2 - expected2)

        return new_rating1, new_rating2

    def _calculate_all_player_elos(self):
        """Processa todos os jogos históricos para gerar os ratings ELO atuais."""
        conn = sqlite3.connect(self.results_db_path)
        # Busca todos os jogos finalizados em ordem cronológica
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

            # Define o resultado (1 para vitória, 0 para derrota)
            s1 = 1 if home_score > away_score else 0
            s2 = 1 - s1

            # Pega os ratings atuais (ou o padrão se for um novo jogador)
            r1, r2 = ratings[home_player], ratings[away_player]

            # Calcula e atualiza os novos ratings
            new_r1, new_r2 = self._update_ratings(r1, r2, s1, s2)
            ratings[home_player] = new_r1
            ratings[away_player] = new_r2

        return dict(ratings)

    def analyze_ml_bet_elo(self, home_rating, away_rating, selection, odds_value):
        """
        Analisa uma aposta de Moneyline (ML) usando o modelo ELO.
        Retorna (accept_bet, est_prob, roi, edge, decision_reason)
        """
        # Calcula a probabilidade de vitória do mandante com base no ELO
        prob_home = self._get_expected_score(home_rating, away_rating)

        if selection == "Home":
            est_prob = prob_home
        else:  # Away
            est_prob = 1 - prob_home

        # Métricas de valor
        implied_prob = 1 / odds_value
        edge = est_prob - implied_prob
        roi = ((est_prob * (odds_value - 1)) - (1 - est_prob)) * 100

        decision_reason = "Aceita: ROI e Edge ok"

        # Lógica de refinamento para apostas ML (To Win)
        required_edge = MIN_EDGE
        # Se for um favorito forte (grande diferença de ELO), podemos exigir um Edge maior
        if abs(home_rating - away_rating) > ELO_DIFFERENCE_STRONG_FAVORITE:
            required_edge *= EDGE_MULTIPLIER_STRONG_FAVORITE
            decision_reason = f"Rejeitada (ML): Favorito forte, Edge {edge:.4f} (min {required_edge}) não atendido"

        # Critério final de aceitação
        accept_bet = roi >= MIN_ROI_ML and edge >= required_edge
        if not accept_bet:
            if (
                "Favorito forte" not in decision_reason
            ):  # Evita sobrescrever se já foi definido acima
                decision_reason = f"Rejeitada (ML): ROI {roi:.2f}% (min {MIN_ROI_ML}%) ou EDGE {edge:.4f} (min {required_edge}) não atendidos"
            logger.info(Fore.RED + f"❌ FILTRO FINAL: {decision_reason}")

        return accept_bet, est_prob, roi, edge, decision_reason

    # ====================================================================
    # FUNÇÕES MANTIDAS E ADAPTADAS DA ESTRUTURA ORIGINAL
    # ====================================================================

    def init_bets_db(self):
        """Atualiza e mantém a estrutura do banco de dados de apostas com novos campos."""
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        # Tabela bets - Adicionando novas colunas se não existirem
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
            home_elo_at_bet REAL, -- NOVO
            away_elo_at_bet REAL, -- NOVO
            elo_prob_home REAL,   -- NOVO
            implied_prob REAL,    -- NOVO
            bet_edge REAL,        -- NOVO
            min_roi_required REAL, -- NOVO
            bet_decision_reason TEXT, -- NOVO
            player_form_home TEXT, -- NOVO (Placeholder por enquanto)
            player_form_away TEXT, -- NOVO (Placeholder por enquanto)
            h2h_summary TEXT,      -- NOVO (Placeholder por enquanto)
            bet_timestamp TIMESTAMP, -- NOVO
            UNIQUE(event_id, bet_type, selection, handicap)
        )
        """)

        # Adicionar colunas individualmente se não existirem (para compatibilidade com DBs existentes)
        new_columns = {
            "home_elo_at_bet": "REAL",
            "away_elo_at_bet": "REAL",
            "elo_prob_home": "REAL",
            "implied_prob": "REAL",
            "bet_edge": "REAL",
            "min_roi_required": "REAL",
            "bet_decision_reason": "TEXT",
            "player_form_home": "TEXT",
            "player_form_away": "TEXT",
            "h2h_summary": "TEXT",
            "bet_timestamp": "TIMESTAMP",
        }
        for col_name, col_type in new_columns.items():
            try:
                cursor.execute(f"ALTER TABLE bets ADD COLUMN {col_name} {col_type}")
                logger.info(f"Coluna '{col_name}' adicionada à tabela 'bets'.")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e):
                    logger.info(f"Coluna '{col_name}' já existe na tabela 'bets'.")
                else:
                    logger.error(f"Erro ao adicionar coluna '{col_name}': {e}")

        cursor.execute(
            "CREATE TABLE IF NOT EXISTS processed_events (event_id INTEGER PRIMARY KEY, processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS update_bets_timestamp
        AFTER UPDATE ON bets FOR EACH ROW
        BEGIN
            UPDATE bets SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
        END;""")
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
            conn.cursor().execute(
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

    # --- LÓGICA DE OVER/UNDER ATUALIZADA ---
    def get_player_last_matches_for_total(self, player_name, limit=20):
        conn = sqlite3.connect(self.results_db_path)
        query = "SELECT event_id, score FROM events WHERE (home_name = ? OR away_name = ?) ORDER BY event_time DESC LIMIT ?"
        df = pd.read_sql_query(query, conn, params=(player_name, player_name, limit))
        conn.close()
        return df

    def get_detailed_scores(self, event_id):
        conn = sqlite3.connect(self.results_db_path)
        df = pd.read_sql_query(
            "SELECT set_number, home_score, away_score FROM event_scores WHERE event_id = ? ORDER BY set_number",
            conn,
            params=(event_id,),
        )
        conn.close()
        return df

    def get_games_per_match_list(self, player_name):
        matches_df = self.get_player_last_matches_for_total(player_name)
        games_list = []
        for _, match in matches_df.iterrows():
            detailed_scores = self.get_detailed_scores(match["event_id"])
            total_games = (
                detailed_scores["home_score"].sum()
                + detailed_scores["away_score"].sum()
            )
            if total_games > 0:
                games_list.append(total_games)
        return games_list

    def analyze_over_under_bet_filtered(
        self, home_games, away_games, handicap_value, selection, odds_value
    ):
        decision_reason = "Aceita: ROI e Edge ok"
        min_roi_ou = 0  # Variável para o ROI mínimo específico de O/U

        # Permitir análise de Over e Under
        if "Under" in selection:
            home_count = sum(1 for g in home_games if g < handicap_value)
            away_count = sum(1 for g in away_games if g < handicap_value)
            # Lógica para Under
            home_prob = home_count / len(home_games) if home_games else 0
            away_prob = away_count / len(away_games) if away_games else 0
            prob_diff = abs(home_prob - away_prob)
            if prob_diff < 0.20:
                if home_prob >= 0.60 and away_prob >= 0.60:
                    est_prob = max(home_prob, away_prob)
                    min_roi_ou = 15
                elif home_prob <= 0.40 and away_prob <= 0.40:
                    est_prob = (home_prob + away_prob) / 2
                    min_roi_ou = 40
                else:
                    est_prob = (home_prob + away_prob) / 2
                    min_roi_ou = 25
            else:
                est_prob = (home_prob + away_prob) / 2
                min_roi_ou = 30
        elif "Over" in selection:
            home_count = sum(1 for g in home_games if g > handicap_value)
            away_count = sum(1 for g in away_games if g > handicap_value)
            # Lógica para Over (pode ser a mesma ou diferente da Under)
            home_prob = home_count / len(home_games) if home_games else 0
            away_prob = away_count / len(away_games) if away_games else 0
            prob_diff = abs(home_prob - away_prob)
            if prob_diff < 0.20:
                if home_prob >= 0.60 and away_prob >= 0.60:
                    est_prob = max(home_prob, away_prob)
                    min_roi_ou = 15
                elif home_prob <= 0.40 and away_prob <= 0.40:
                    est_prob = (home_prob + away_prob) / 2
                    min_roi_ou = 40
                else:
                    est_prob = (home_prob + away_prob) / 2
                    min_roi_ou = 25
            else:
                est_prob = (home_prob + away_prob) / 2
                min_roi_ou = 30
        else:
            decision_reason = "Rejeitada (Total): Tipo de aposta Total inválido"
            logger.info(Fore.RED + f"❌ FILTRO FINAL: {decision_reason}")
            return False, 0, 0, 0, 0, 0, decision_reason

        roi = ((est_prob * (odds_value - 1)) - (1 - est_prob)) * 100
        accept = roi >= min_roi_ou

        if not accept:
            decision_reason = f"Rejeitada (Total): ROI {roi:.2f}% abaixo do mínimo ({min_roi_ou}%) para O/U"
            logger.info(Fore.RED + f"❌ FILTRO FINAL: {decision_reason}")

        return accept, est_prob, roi, home_prob, away_prob, min_roi_ou, decision_reason

    # --- FIM DA LÓGICA DE OVER/UNDER ---

    def get_player_form_summary(self, player_name, limit=5):
        """Gera um resumo simples da forma recente do jogador (W-L)."""
        conn = sqlite3.connect(self.results_db_path)
        query = """
        SELECT home_name, away_name, score
        FROM events 
        WHERE (home_name = ? OR away_name = ?) AND time_status = 3
        ORDER BY event_time DESC 
        LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(player_name, player_name, limit))
        conn.close()

        wins = 0
        losses = 0
        for _, row in df.iterrows():
            if not row["score"] or "-" not in row["score"]:
                continue
            try:
                home_score, away_score = map(int, row["score"].split("-"))
                if row["home_name"] == player_name:
                    if home_score > away_score:
                        wins += 1
                    else:
                        losses += 1
                else:
                    if away_score > home_score:
                        wins += 1
                    else:
                        losses += 1
            except ValueError:
                continue
        return f"{wins}W-{losses}L nos últimos {len(df)} jogos"

    def get_h2h_summary(self, player1, player2, limit=5):
        """Gera um resumo simples do H2H (Player1 W-L vs Player2)."""
        conn = sqlite3.connect(self.results_db_path)
        query = """
        SELECT home_name, away_name, score
        FROM events 
        WHERE ((home_name = ? AND away_name = ?) OR (home_name = ? AND away_name = ?)) AND time_status = 3
        ORDER BY event_time DESC 
        LIMIT ?
        """
        df = pd.read_sql_query(
            query, conn, params=(player1, player2, player2, player1, limit)
        )
        conn.close()

        player1_wins = 0
        player2_wins = 0
        for _, row in df.iterrows():
            if not row["score"] or "-" not in row["score"]:
                continue
            try:
                home_score, away_score = map(int, row["score"].split("-"))
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
        return (
            f"{player1_wins}W-{player2_wins}L vs {player2}"
            if (player1_wins + player2_wins) > 0
            else "N/A"
        )

    def analyze_bet_value(self, match, odds_df):
        home_player = match["home_team"]
        away_player = match["away_team"]
        valuable_bets = []

        # Pega os ratings ELO pré-calculados (ou o padrão se for jogador novo)
        home_rating = self.player_ratings.get(home_player, DEFAULT_ELO)
        away_rating = self.player_ratings.get(away_player, DEFAULT_ELO)

        # Obter resumos de forma e H2H para registro
        player_form_home = self.get_player_form_summary(home_player)
        player_form_away = self.get_player_form_summary(away_player)
        h2h_summary = self.get_h2h_summary(home_player, away_player)

        # Lógica para Over/Under ainda precisa dos jogos recentes
        home_games_list = self.get_games_per_match_list(home_player)
        away_games_list = self.get_games_per_match_list(away_player)

        for _, row in odds_df.iterrows():
            market, selection, odds_value, handicap = (
                row["market_type"],
                row["selection"],
                row["odds"],
                row["handicap_value"],
            )
            if odds_value <= 1.01:
                continue

            bet_data = {
                "event_id": match["event_id"],
                "league_name": match["league_name"],
                "home_team": home_player,
                "away_team": away_player,
                "event_time": match["event_time"],
                "bet_type": market,
                "selection": selection,
                "handicap": None,
                "odds": odds_value,
                "fair_odds": 0,  # Será preenchido
                "estimated_roi": 0,  # Será preenchido
                "home_elo_at_bet": home_rating,
                "away_elo_at_bet": away_rating,
                "elo_prob_home": None,  # Será preenchido para ML
                "implied_prob": 1 / odds_value,
                "bet_edge": None,  # Será preenchido
                "min_roi_required": None,  # Será preenchido
                "bet_decision_reason": "",  # Será preenchido
                "player_form_home": player_form_home,
                "player_form_away": player_form_away,
                "h2h_summary": h2h_summary,
                "bet_timestamp": datetime.now(),
            }

            if market == "To Win":
                accept_bet, est_prob, estimated_roi, edge, decision_reason = (
                    self.analyze_ml_bet_elo(
                        home_rating, away_rating, selection, odds_value
                    )
                )
                bet_data.update(
                    {
                        "fair_odds": 1 / est_prob if est_prob > 0 else 0,
                        "estimated_roi": estimated_roi,
                        "elo_prob_home": self._get_expected_score(
                            home_rating, away_rating
                        ),
                        "bet_edge": edge,
                        "min_roi_required": MIN_ROI_ML,
                        "bet_decision_reason": decision_reason,
                    }
                )
                if accept_bet:
                    valuable_bets.append(bet_data)

            elif market == "Total" and handicap:
                try:
                    handicap_value = float(handicap.replace("O ", "").replace("U ", ""))
                except ValueError:
                    continue

                (
                    accept_bet,
                    est_prob,
                    estimated_roi,
                    home_prob,
                    away_prob,
                    min_roi_ou,
                    decision_reason,
                ) = self.analyze_over_under_bet_filtered(
                    home_games_list,
                    away_games_list,
                    handicap_value,
                    selection,
                    odds_value,
                )
                bet_data.update(
                    {
                        "handicap": handicap_value,
                        "fair_odds": 1 / est_prob if est_prob > 0 else 0,
                        "estimated_roi": estimated_roi,
                        "min_roi_required": min_roi_ou,
                        "bet_decision_reason": decision_reason,
                    }
                )
                if accept_bet:
                    valuable_bets.append(bet_data)
        return valuable_bets

    def save_top_bets_by_league(self, all_bets):
        """Salva as apostas no banco de dados, incluindo os novos campos."""
        if not all_bets:
            return 0
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()
        total_saved = 0
        for bet in all_bets:
            try:
                cursor.execute(
                    """
                INSERT OR REPLACE INTO bets 
                (event_id, league_name, home_team, away_team, event_time, 
                 bet_type, selection, handicap, odds, fair_odds, estimated_roi,
                 home_elo_at_bet, away_elo_at_bet, elo_prob_home, implied_prob, bet_edge,
                 min_roi_required, bet_decision_reason, player_form_home, player_form_away,
                 h2h_summary, bet_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        bet["bet_timestamp"],
                    ),
                )
                if cursor.rowcount > 0:
                    total_saved += 1
                    logger.info(
                        Fore.GREEN
                        + f"💾 SALVA: {bet['bet_type']} {bet['selection']} @ {bet['odds']:.2f} (ROI: {bet['estimated_roi']:.2f}%) - Razão: {bet['bet_decision_reason']}"
                    )
            except sqlite3.Error as e:
                logger.error(f"Erro ao salvar aposta: {e}")
        conn.commit()
        conn.close()
        return total_saved

    def process_all_matches(self):
        logger.info(
            "🚀 Iniciando processamento com MODELO ELO e registro de dados expandido..."
        )
        logger.info(
            f"📊 Parâmetros: K_FACTOR={K_FACTOR}, DEFAULT_ELO={DEFAULT_ELO}, MIN_ROI_ML={MIN_ROI_ML}%, MIN_EDGE={MIN_EDGE}"
        )
        logger.info(
            f"📊 Refinamento ML: ELO_DIFFERENCE_STRONG_FAVORITE={ELO_DIFFERENCE_STRONG_FAVORITE}, EDGE_MULTIPLIER_STRONG_FAVORITE={EDGE_MULTIPLIER_STRONG_FAVORITE}"
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
    processor = BetProcessor()
    processor.process_all_matches()


if __name__ == "__main__":
    main()
