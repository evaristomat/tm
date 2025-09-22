import sqlite3
import pandas as pd
from datetime import datetime
import asyncio
from telegram import Bot
from telegram.constants import ParseMode
import logging
import os

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
        self.bets_db_path = bets_db_path

        if not self.bot_token or not self.chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID são obrigatórios!")

        self.bot = Bot(token=self.bot_token)
        self.init_tracking_tables()

    def init_tracking_tables(self):
        """Inicializa tabelas para controle de envios"""
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS telegram_sent_bets (
            bet_id INTEGER PRIMARY KEY,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (bet_id) REFERENCES bets (id)
        )
        """)

        conn.commit()
        conn.close()

    def get_new_bets(self):
        """Busca todas as apostas não enviadas"""
        conn = sqlite3.connect(self.bets_db_path)

        # Debug: verificar totais
        debug_query = "SELECT COUNT(*) as total_bets FROM bets"
        debug_df = pd.read_sql_query(debug_query, conn)

        sent_query = "SELECT COUNT(*) as sent_count FROM telegram_sent_bets"
        sent_df = pd.read_sql_query(sent_query, conn)

        logger.info(
            f"Total apostas: {debug_df.iloc[0]['total_bets']}, Já enviadas: {sent_df.iloc[0]['sent_count']}"
        )

        # Buscar apostas não enviadas (todas, sem filtro de horário)
        query = """
        SELECT 
            b.id, b.league_name, b.home_team, b.away_team, 
            b.event_time, b.bet_type, b.selection, b.handicap, 
            b.odds, b.estimated_roi
        FROM bets b
        LEFT JOIN telegram_sent_bets t ON b.id = t.bet_id
        WHERE t.bet_id IS NULL
        ORDER BY b.league_name, b.estimated_roi DESC
        """

        df = pd.read_sql_query(query, conn)

        if not df.empty:
            logger.info(f"Apostas não enviadas encontradas: {len(df)}")

        conn.close()
        return df

    def mark_bets_as_sent(self, bet_ids):
        """Marca apostas como enviadas"""
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        for bet_id in bet_ids:
            cursor.execute(
                "INSERT INTO telegram_sent_bets (bet_id) VALUES (?)", (bet_id,)
            )

        conn.commit()
        conn.close()

    def get_profit_summary(self):
        """Busca resumo de lucros por liga"""
        conn = sqlite3.connect(self.bets_db_path)

        query = """
        SELECT 
            league_name,
            SUM(CASE WHEN result = 1 THEN profit WHEN result = 0 THEN -1 ELSE 0 END) as total_profit,
            COUNT(CASE WHEN result IS NOT NULL THEN 1 END) as total_bets,
            COUNT(CASE WHEN result = 1 THEN 1 END) as wins,
            COUNT(CASE WHEN result = 0 THEN 1 END) as losses,
            SUM(CASE WHEN bet_type = 'To Win' AND result = 1 THEN profit 
                     WHEN bet_type = 'To Win' AND result = 0 THEN -1 ELSE 0 END) as ml_profit,
            COUNT(CASE WHEN bet_type = 'To Win' AND result IS NOT NULL THEN 1 END) as ml_total,
            SUM(CASE WHEN bet_type = 'Total' AND result = 1 THEN profit 
                     WHEN bet_type = 'Total' AND result = 0 THEN -1 ELSE 0 END) as ou_profit,
            COUNT(CASE WHEN bet_type = 'Total' AND result IS NOT NULL THEN 1 END) as ou_total
        FROM bets
        WHERE result IS NOT NULL
        GROUP BY league_name
        ORDER BY total_profit DESC
        """

        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def format_bet_message(self, league_bets):
        """Formata mensagem de apostas por liga"""
        league_name = league_bets.iloc[0]["league_name"]

        league_icons = {
            "Czech Liga Pro": "🇨🇿",
            "TT Elite Series": "⭐",
            "Challenger Series TT": "🏓",
            "TT Cup": "🏆",
        }

        icon = league_icons.get(league_name, "🏓")
        message = f"{icon} *{league_name}*\n"
        message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        # Agrupar por tipo de aposta
        ml_bets = league_bets[league_bets["bet_type"] == "To Win"]
        ou_bets = league_bets[league_bets["bet_type"] == "Total"]

        if not ml_bets.empty:
            message += "💰 *MONEY LINE*\n"
            for _, bet in ml_bets.iterrows():
                time_str = pd.to_datetime(bet["event_time"]).strftime("%d/%m %H:%M")
                tip = (
                    bet["home_team"] if bet["selection"] == "Home" else bet["away_team"]
                )

                message += f"🆚 {bet['home_team']} vs {bet['away_team']}\n"
                message += f"🎯 {tip} | 📊 {bet['odds']:.2f} | ⏰ {time_str}\n"
                message += f"📈 ROI: {bet['estimated_roi']:.1f}%\n\n"

        if not ou_bets.empty:
            message += "🔢 *OVER/UNDER*\n"
            for _, bet in ou_bets.iterrows():
                time_str = pd.to_datetime(bet["event_time"]).strftime("%d/%m %H:%M")
                tip = f"{bet['selection']} {bet['handicap']:.1f}"

                message += f"🆚 {bet['home_team']} vs {bet['away_team']}\n"
                message += f"🎯 {tip} | 📊 {bet['odds']:.2f} | ⏰ {time_str}\n"
                message += f"📈 ROI: {bet['estimated_roi']:.1f}%\n\n"

        return message

    def format_profit_message(self, profit_data):
        """Formata mensagem de resumo de lucros"""
        if profit_data.empty:
            return "📊 *RESUMO DE LUCROS*\n\nNenhum dado disponível ainda."

        message = "💰 *RESUMO DE LUCROS*\n"
        message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        total_profit = 0
        total_bets = 0

        for _, row in profit_data.iterrows():
            roi = (
                (row["total_profit"] / row["total_bets"] * 100)
                if row["total_bets"] > 0
                else 0
            )
            status = "✅" if row["total_profit"] > 0 else "❌"

            message += f"🏓 *{row['league_name']}*\n"
            message += f"{status} {row['total_profit']:+.2f}u | ROI: {roi:+.1f}% | {row['wins']}W-{row['losses']}L\n"
            message += f"├ ML: {row['ml_profit']:+.2f}u ({row['ml_total']} apostas)\n"
            message += (
                f"└ O/U: {row['ou_profit']:+.2f}u ({row['ou_total']} apostas)\n\n"
            )

            total_profit += row["total_profit"]
            total_bets += row["total_bets"]

        total_roi = (total_profit / total_bets * 100) if total_bets > 0 else 0
        total_status = "✅" if total_profit > 0 else "❌"

        message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        message += f"📊 *TOTAL GERAL*\n"
        message += f"{total_status} {total_profit:+.2f}u | ROI: {total_roi:+.1f}%"

        return message

    async def send_message(self, text):
        """Envia mensagem para o Telegram"""
        try:
            await self.bot.send_message(
                chat_id=self.chat_id, text=text, parse_mode=ParseMode.MARKDOWN
            )
            return True
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem: {e}")
            return False

    async def send_new_bets(self):
        """Envia novas apostas para o grupo"""
        new_bets = self.get_new_bets()

        if new_bets.empty:
            logger.info("Nenhuma aposta nova para enviar")
            return 0

        logger.info(f"Encontradas {len(new_bets)} apostas novas")

        sent_count = 0
        sent_bet_ids = []

        for league in new_bets["league_name"].unique():
            league_bets = new_bets[new_bets["league_name"] == league]
            message = self.format_bet_message(league_bets)

            if await self.send_message(message):
                sent_bet_ids.extend(league_bets["id"].tolist())
                sent_count += len(league_bets)
                logger.info(f"✅ Enviadas {len(league_bets)} apostas da liga {league}")
                await asyncio.sleep(1)
            else:
                logger.error(f"❌ Falha ao enviar apostas da liga {league}")

        if sent_bet_ids:
            self.mark_bets_as_sent(sent_bet_ids)
            logger.info(f"Total de apostas enviadas: {sent_count}")

        return sent_count

    async def send_profit_summary(self):
        """Envia resumo de lucros"""
        profit_data = self.get_profit_summary()
        message = self.format_profit_message(profit_data)

        if await self.send_message(message):
            logger.info("✅ Resumo de lucros enviado")
        else:
            logger.error("❌ Falha ao enviar resumo de lucros")

    async def run(self):
        """Executa o processo completo"""
        try:
            sent_count = await self.send_new_bets()

            if sent_count > 0:
                await asyncio.sleep(2)
                await self.send_profit_summary()

            logger.info("✅ Execução concluída")

        except Exception as e:
            logger.error(f"❌ Erro durante execução: {e}")
            raise


async def main():
    """Função principal"""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        logger.error(
            "❌ Variáveis TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID são obrigatórias!"
        )
        return

    notifier = TelegramBetNotifier()
    await notifier.run()


if __name__ == "__main__":
    asyncio.run(main())
