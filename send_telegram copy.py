import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import asyncio
from telegram import Bot
from telegram.constants import ParseMode
import logging
import os

# Carregar variáveis de ambiente
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("telegram_notifier")


class TelegramBetNotifier:
    def __init__(self, bot_token=None, chat_id=None, bets_db_path="bets.db"):
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")

        if not self.bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN não encontrado!")

        if not self.chat_id:
            raise ValueError("TELEGRAM_CHAT_ID não encontrado!")

        self.bets_db_path = bets_db_path
        self.bot = Bot(token=self.bot_token)
        self.init_telegram_db()

        logger.info("✅ TelegramBetNotifier inicializado com sucesso")

    def init_telegram_db(self):
        """Inicializa tabelas de controle do telegram"""
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS telegram_sent_bets (
            bet_id INTEGER PRIMARY KEY,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (bet_id) REFERENCES bets (id)
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS telegram_daily_limit (
            league_name TEXT,
            date DATE,
            count INTEGER DEFAULT 0,
            PRIMARY KEY (league_name, date)
        )
        """)

        conn.commit()
        conn.close()

    def get_unsent_bets(self):
        """Busca apostas não enviadas ainda"""
        conn = sqlite3.connect(self.bets_db_path)

        query = """
        SELECT 
            b.id,
            b.event_id,
            b.league_name,
            b.home_team,
            b.away_team,
            b.event_time,
            b.bet_type,
            b.selection,
            b.handicap,
            b.odds,
            b.estimated_roi
        FROM bets b
        LEFT JOIN telegram_sent_bets t ON b.id = t.bet_id
        WHERE t.bet_id IS NULL
            AND b.event_time > datetime('now')
        ORDER BY b.league_name, b.estimated_roi DESC
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        return df

    def get_daily_count(self, league_name, date):
        """Obtém contagem diária de apostas enviadas por liga"""
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
        SELECT count FROM telegram_daily_limit
        WHERE league_name = ? AND date = ?
        """,
            (league_name, date),
        )

        result = cursor.fetchone()
        conn.close()

        return result[0] if result else 0

    def update_daily_count(self, league_name, date):
        """Incrementa contagem diária"""
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
        INSERT INTO telegram_daily_limit (league_name, date, count)
        VALUES (?, ?, 1)
        ON CONFLICT(league_name, date) 
        DO UPDATE SET count = count + 1
        """,
            (league_name, date),
        )

        conn.commit()
        conn.close()

    def mark_as_sent(self, bet_id):
        """Marca aposta como enviada"""
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
        INSERT INTO telegram_sent_bets (bet_id) VALUES (?)
        """,
            (bet_id,),
        )

        conn.commit()
        conn.close()

    def get_profit_data(self):
        """Busca dados de lucro por liga da tabela bets"""
        conn = sqlite3.connect(self.bets_db_path)

        query = """
        SELECT 
            league_name,
            SUM(CASE WHEN result = 1 THEN profit 
                    WHEN result = 0 THEN -1 
                    ELSE 0 END) as total_profit,
            COUNT(CASE WHEN result = 1 THEN 1 END) as wins,
            COUNT(CASE WHEN result = 0 THEN 1 END) as losses,
            SUM(CASE WHEN bet_type = 'To Win' AND result = 1 THEN profit 
                    WHEN bet_type = 'To Win' AND result = 0 THEN -1 
                    ELSE 0 END) as ml_profit,
            COUNT(CASE WHEN bet_type = 'To Win' AND result = 1 THEN 1 END) as ml_wins,
            COUNT(CASE WHEN bet_type = 'To Win' AND result = 0 THEN 1 END) as ml_losses,
            SUM(CASE WHEN bet_type = 'Total' AND result = 1 THEN profit 
                    WHEN bet_type = 'Total' AND result = 0 THEN -1 
                    ELSE 0 END) as ou_profit,
            COUNT(CASE WHEN bet_type = 'Total' AND result = 1 THEN 1 END) as ou_wins,
            COUNT(CASE WHEN bet_type = 'Total' AND result = 0 THEN 1 END) as ou_losses,
            SUM(CASE WHEN DATE(event_time) = DATE('now', '-1 day') AND result = 1 THEN profit 
                    WHEN DATE(event_time) = DATE('now', '-1 day') AND result = 0 THEN -1 
                    ELSE 0 END) as yesterday_profit,
            SUM(CASE WHEN DATE(event_time) = DATE('now') AND result = 1 THEN profit 
                    WHEN DATE(event_time) = DATE('now') AND result = 0 THEN -1 
                    ELSE 0 END) as today_profit,
            COUNT(*) as total_stake  -- Como stake é sempre 1, usamos a contagem de apostas
        FROM bets
        WHERE result IS NOT NULL
        GROUP BY league_name
        """

        try:
            df = pd.read_sql_query(query, conn)
        except Exception as e:
            logger.error(f"Erro ao buscar dados de lucro: {e}")
            df = pd.DataFrame()
        finally:
            conn.close()

        return df

    def format_profit_message(self, profit_data):
        """Formata mensagem de resumo de lucros"""
        if profit_data.empty:
            return "📊 Nenhum dado de lucro disponível ainda."

        message = "💰 *RESUMO DE LUCROS* 💰\n"
        message += "══════════════════════════════════\n\n"

        league_flags = {
            "Czech Liga Pro": "🇨🇿 República Tcheca - Liga Pro",
            "TT Elite Series": "🇵🇱 TT Elite Series",
            "Challenger Series TT": "🏓 Challenger Series TT",
            "TT Cup": "🏆 TT Cup",
        }

        total_profit = 0
        total_wins = 0
        total_losses = 0
        total_stake = 0
        total_ml_profit = 0
        total_ml_wins = 0
        total_ml_losses = 0
        total_ou_profit = 0
        total_ou_wins = 0
        total_ou_losses = 0

        for _, row in profit_data.iterrows():
            league_display = league_flags.get(
                row["league_name"], f"🏓 {row['league_name']}"
            )

            # Calcular ROI corretamente
            roi = (
                (row["total_profit"] / row["total_stake"]) * 100
                if row["total_stake"] > 0
                else 0
            )

            # Escolher emoji baseado no resultado
            result_emoji = "✅" if row["total_profit"] > 0 else "❌"

            message += f"{league_display}\n"
            message += f"├ Ontem: {row['yesterday_profit']:.2f}u\n"
            message += f"├ Hoje: {row['today_profit']:.2f}u\n"
            message += f"├ {result_emoji} Total: {row['total_profit']:+.2f}u (ROI: {roi:+.1f}%) | {row['wins']}W-{row['losses']}L\n"
            message += (
                f"├ ML: {row['ml_profit']:+.2f}u | {row['ml_wins']}W-{row['ml_losses']}L\n"
            )
            message += f"└ O/U: {row['ou_profit']:+.2f}u | {row['ou_wins']}W-{row['ou_losses']}L\n\n"

            # Acumular totais
            total_profit += row["total_profit"]
            total_wins += row["wins"]
            total_losses += row["losses"]
            total_stake += row["total_stake"]
            total_ml_profit += row["ml_profit"]
            total_ml_wins += row["ml_wins"]
            total_ml_losses += row["ml_losses"]
            total_ou_profit += row["ou_profit"]
            total_ou_wins += row["ou_wins"]
            total_ou_losses += row["ou_losses"]

        # Calcular ROI total corretamente
        total_roi = (total_profit / total_stake) * 100 if total_stake > 0 else 0
        total_emoji = "✅" if total_profit > 0 else "❌"

        message += "══════════════════════════════════\n"
        message += "📊 *TOTAL GERAL*\n"
        message += f"{total_emoji} {total_profit:+.2f}u (ROI: {total_roi:+.1f}%)\n"
        message += f"✅ {total_wins}W | ❌ {total_losses}L\n"
        message += f"├ ML: {total_ml_profit:+.2f}u | {total_ml_wins}W-{total_ml_losses}L\n"
        message += f"└ O/U: {total_ou_profit:+.2f}u | {total_ou_wins}W-{total_ou_losses}L\n"

        return message

    def format_bet_message(self, league_bets):
        """Formata mensagem para uma liga com separação por tipo de aposta"""
        if league_bets.empty:
            return None

        league_name = league_bets.iloc[0]["league_name"]

        league_flags = {
            "Czech Liga Pro": "🇨🇿 República Tcheca - Liga Pro",
            "Challenger Series TT": "🏓 Challenger Series TT",
            "TT Cup": "🏆 TT Cup",
            "TT Elite Series": "⭐ TT Elite Series",
        }

        league_display = league_flags.get(league_name, f"🏓 {league_name}")

        # Dividir por tipo de aposta
        to_win_bets = league_bets[league_bets["bet_type"] == "To Win"]
        total_bets = league_bets[league_bets["bet_type"] == "Total"]

        message = f"⭐ *{league_display}* ⭐\n"
        message += "┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n\n"

        # Seção Money Line
        if not to_win_bets.empty:
            message += "💰 *MONEY LINE* 💰\n"
            for _, bet in to_win_bets.iterrows():
                event_time = pd.to_datetime(bet["event_time"]).strftime("%d/%m %H:%M")

                if bet["selection"] == "Home":
                    tip = bet["home_team"]
                else:
                    tip = bet["away_team"]

                message += f"🏓 {bet['home_team']} vs {bet['away_team']}\n"
                message += f"🎯 TIP: {tip}\n"
                message += f"🎲 Odds: {bet['odds']:.2f} | 📅 {event_time}\n"
                message += f"📊 ROI: {bet['estimated_roi']:.1f}%\n"
                message += "┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n"

        # Seção Over/Under
        if not total_bets.empty:
            message += "🔢 *OVER/UNDER* 🔢\n"
            for _, bet in total_bets.iterrows():
                event_time = pd.to_datetime(bet["event_time"]).strftime("%d/%m %H:%M")
                tip = f"{bet['selection']} {bet['handicap']:.1f}"

                message += f"🏓 {bet['home_team']} vs {bet['away_team']}\n"
                message += f"🎯 TIP: {tip}\n"
                message += f"🎲 Odds: {bet['odds']:.2f} | 📅 {event_time}\n"
                message += f"📊 ROI: {bet['estimated_roi']:.1f}%\n"
                message += "┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n"

        return message

    async def send_message(self, message, parse_mode=ParseMode.MARKDOWN):
        """Envia mensagem para o Telegram"""
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=parse_mode,
            )
            return True
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem: {e}")
            return False

    async def process_and_send_bets(self):
        """Processa e envia apostas não enviadas"""
        today = datetime.now().date()

        unsent_bets = self.get_unsent_bets()

        if unsent_bets.empty:
            logger.info("Nenhuma aposta nova para enviar")
            return

        logger.info(f"Total de apostas não enviadas: {len(unsent_bets)}")

        leagues = unsent_bets["league_name"].unique()
        total_sent = 0

        for league in leagues:
            daily_count = self.get_daily_count(league, today)

            if daily_count >= 20:
                logger.info(f"Liga {league} já atingiu limite diário (20 apostas)")
                continue

            league_bets = unsent_bets[unsent_bets["league_name"] == league]
            remaining = 20 - daily_count
            league_bets = league_bets.head(remaining)

            if league_bets.empty:
                continue

            message = self.format_bet_message(league_bets)

            if message:
                success = await self.send_message(message)

                if success:
                    for bet_id in league_bets["id"]:
                        self.mark_as_sent(bet_id)
                        self.update_daily_count(league, today)
                        total_sent += 1

                    logger.info(
                        f"✅ Enviadas {len(league_bets)} apostas da liga {league}"
                    )
                    await asyncio.sleep(2)
                else:
                    logger.error(f"Falha ao enviar apostas da liga {league}")

        logger.info(f"Total de apostas enviadas: {total_sent}")

    async def send_profit_summary(self):
        """Envia resumo de lucros"""
        profit_data = self.get_profit_data()
        message = self.format_profit_message(profit_data)

        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=ParseMode.MARKDOWN,
            )
            logger.info("✅ Resumo de lucros enviado com sucesso!")
        except Exception as e:
            logger.error(f"❌ Erro ao enviar resumo: {e}")


def validate_env_vars():
    """Valida se as variáveis de ambiente estão configuradas"""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token:
        logger.error("❌ TELEGRAM_BOT_TOKEN não encontrado no .env!")
        return False

    if not chat_id:
        logger.error("❌ TELEGRAM_CHAT_ID não encontrado no .env!")
        return False

    logger.info("✅ Variáveis de ambiente carregadas com sucesso")
    return True


async def main():
    """Função principal"""
    if not validate_env_vars():
        logger.error("❌ Erro: Variáveis de ambiente não configuradas!")
        return

    try:
        notifier = TelegramBetNotifier()
        await notifier.process_and_send_bets()
        await notifier.send_profit_summary()
        logger.info("✅ Execução concluída com sucesso!")

    except Exception as e:
        logger.error(f"❌ Erro durante a execução: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
