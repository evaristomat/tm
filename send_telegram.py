import asyncio
import logging
import os
import sqlite3
from datetime import datetime

import pandas as pd
from telegram import Bot
from telegram.constants import ParseMode

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("telegram_notifier")

filtro = True  # variável global para ativar o filtro da estratégia


class TelegramBetNotifier:
    def __init__(self, bot_token=None, chat_id=None, bets_db_path="bets.db"):
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")
        self.bets_db_path = bets_db_path
        self.MAX_MESSAGE_LENGTH = 4096

        if not self.bot_token or not self.chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID são obrigatórios!")

        self.bot = Bot(token=self.bot_token)
        self.init_tracking_tables()

    def init_tracking_tables(self):
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_sent_bets (
                bet_id INTEGER PRIMARY KEY,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (bet_id) REFERENCES bets (id)
            )
            """
        )

        conn.commit()
        conn.close()

    def get_new_bets(self):
        conn = sqlite3.connect(self.bets_db_path)

        debug_query = "SELECT COUNT(*) as total_bets FROM bets"
        debug_df = pd.read_sql_query(debug_query, conn)

        sent_query = "SELECT COUNT(*) as sent_count FROM telegram_sent_bets"
        sent_df = pd.read_sql_query(sent_query, conn)

        logger.info(
            f"Total apostas: {debug_df.iloc[0]['total_bets']}, Já enviadas: {sent_df.iloc[0]['sent_count']}"
        )

        base_query = """
            SELECT 
                b.id, b.league_name, b.home_team, b.away_team, 
                b.event_time, b.bet_type, b.selection, b.handicap, 
                b.odds, b.estimated_roi
            FROM bets b
            LEFT JOIN telegram_sent_bets t ON b.id = t.bet_id
            WHERE t.bet_id IS NULL 
              AND b.bet_type = 'Total' 
              AND b.selection LIKE 'Under%'
        """

        if filtro:
            filtro_cond = """
              AND (
                (b.handicap = 76.5 AND b.estimated_roi >= 20)
                OR (b.handicap = 77.5 AND b.estimated_roi >= 40)
                OR (b.handicap = 78.5)
              )
            """
            query = (
                base_query + filtro_cond + " ORDER BY b.league_name, b.event_time ASC"
            )
        else:
            query = base_query + " ORDER BY b.league_name, b.event_time ASC"

        df = pd.read_sql_query(query, conn)

        if not df.empty:
            logger.info(f"Apostas Under não enviadas encontradas: {len(df)}")

        conn.close()
        return df

    def mark_bets_as_sent(self, bet_ids):
        conn = sqlite3.connect(self.bets_db_path)
        cursor = conn.cursor()

        for bet_id in bet_ids:
            cursor.execute(
                "INSERT INTO telegram_sent_bets (bet_id) VALUES (?)", (bet_id,)
            )

        conn.commit()
        conn.close()

    def get_profit_summary(self):
        conn = sqlite3.connect(self.bets_db_path)

        query = """
            SELECT 
                result,
                profit,
                handicap,
                estimated_roi,
                bet_edge,
                league_name
            FROM bets
            WHERE result IS NOT NULL AND bet_type = 'Total' AND selection LIKE 'Under%'
        """

        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def format_bet_messages(self, league_bets):
        league_name = league_bets.iloc[0]["league_name"]

        league_icons = {
            "Czech Liga Pro": "🇨🇿",
            "TT Elite Series": "⭐",
            "Challenger Series TT": "🏓",
            "TT Cup": "🏆",
            "Setka Cup": "🇺🇦",
            "Setka Cup Women": "♀️🇺🇦",
        }

        icon = league_icons.get(league_name, "🏓")
        header = f"{icon} *{league_name}*\n"
        header += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        messages = []
        current_message = header

        def format_under_section(bets):
            if bets.empty:
                return ""

            bets_sorted = bets.sort_values("event_time")
            section = "🔻 *UNDER*\n"

            for _, bet in bets_sorted.iterrows():
                time_str = pd.to_datetime(bet["event_time"]).strftime("%d/%m %H:%M")
                tip = f"{bet['selection']} {bet['handicap']:.1f}"

                section += f"🆚 {bet['home_team']} vs {bet['away_team']}\n"
                section += f"🎯 {tip} | 📊 {bet['odds']:.2f} | ⏰ {time_str}\n"
                section += f"📈 ROI: {bet['estimated_roi']:.1f}%\n\n"

            return section

        if not league_bets.empty:
            under_section = format_under_section(league_bets)

            if len(current_message + under_section) > self.MAX_MESSAGE_LENGTH:
                if len(current_message) > len(header):
                    messages.append(current_message)
                    current_message = header

                temp_section = "🔻 *UNDER*\n"
                under_sorted = league_bets.sort_values("event_time")

                for _, bet in under_sorted.iterrows():
                    time_str = pd.to_datetime(bet["event_time"]).strftime("%d/%m %H:%M")
                    tip = f"{bet['selection']} {bet['handicap']:.1f}"

                    bet_text = f"🆚 {bet['home_team']} vs {bet['away_team']}\n"
                    bet_text += f"🎯 {tip} | 📊 {bet['odds']:.2f} | ⏰ {time_str}\n"
                    bet_text += f"📈 ROI: {bet['estimated_roi']:.1f}%\n\n"

                    if (
                        len(current_message + temp_section + bet_text)
                        > self.MAX_MESSAGE_LENGTH
                    ):
                        if temp_section != "🔻 *UNDER*\n":
                            current_message += temp_section
                            messages.append(current_message)
                            current_message = header
                            temp_section = "🔻 *UNDER*\n"

                    temp_section += bet_text

                if temp_section != "🔻 *UNDER*\n":
                    current_message += temp_section
            else:
                current_message += under_section

        if len(current_message) > len(header):
            messages.append(current_message)

        return messages

    def format_profit_message(self, profit_data):
        if profit_data.empty:
            return "📊 *RESUMO DE LUCROS UNDER*\n\nNenhum dado disponível ainda."

        baseline = profit_data[
            profit_data["league_name"].isin(["Setka Cup", "Czech Liga Pro"])
        ]
        baseline_profit = baseline["profit"].sum()
        baseline_bets = len(baseline)
        baseline_wins = (baseline["result"] == 1).sum()
        baseline_losses = (baseline["result"] == 0).sum()
        baseline_roi = (
            (baseline_profit / baseline_bets * 100) if baseline_bets > 0 else 0
        )

        filtro_df = profit_data[
            ((profit_data["handicap"] == 76.5) & (profit_data["estimated_roi"] >= 20))
            | ((profit_data["handicap"] == 77.5) & (profit_data["estimated_roi"] >= 40))
            | (profit_data["handicap"] == 78.5)
        ]
        filtro_profit = filtro_df["profit"].sum()
        filtro_bets = len(filtro_df)
        filtro_wins = (filtro_df["result"] == 1).sum()
        filtro_losses = (filtro_df["result"] == 0).sum()
        filtro_roi = (filtro_profit / filtro_bets * 100) if filtro_bets > 0 else 0

        h78 = profit_data[profit_data["handicap"] >= 78.5]
        h78_profit = h78["profit"].sum()
        h78_bets = len(h78)
        h78_wins = (h78["result"] == 1).sum()
        h78_losses = (h78["result"] == 0).sum()
        h78_roi = (h78_profit / h78_bets * 100) if h78_bets > 0 else 0

        message = "💰 *RESUMO DE LUCROS UNDER*\n"
        message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        status_b = "✅" if baseline_profit > 0 else "❌"
        message += f"📊 *Setka + Czech*\n"
        message += f"{status_b} {baseline_profit:+.2f}u | ROI: {baseline_roi:+.1f}% | {baseline_wins}W-{baseline_losses}L ({baseline_bets})\n\n"

        status_f = "✅" if filtro_profit > 0 else "❌"
        message += f"🎯 *Filtro Estratégico*\n"
        message += f"{status_f} {filtro_profit:+.2f}u | ROI: {filtro_roi:+.1f}% | {filtro_wins}W-{filtro_losses}L ({filtro_bets})\n\n"

        status_h = "✅" if h78_profit > 0 else "❌"
        message += f"🔥 *Handicap ≥ 78.5*\n"
        message += f"{status_h} {h78_profit:+.2f}u | ROI: {h78_roi:+.1f}% | {h78_wins}W-{h78_losses}L ({h78_bets})"

        return message

    async def send_message(self, text):
        try:
            await self.bot.send_message(
                chat_id=self.chat_id, text=text, parse_mode=ParseMode.MARKDOWN
            )
            return True
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem: {e}")
            return False

    async def send_new_bets(self):
        new_bets = self.get_new_bets()

        if new_bets.empty:
            logger.info("Nenhuma aposta nova para enviar")
            return 0

        logger.info(f"Encontradas {len(new_bets)} apostas novas")

        sent_count = 0
        sent_bet_ids = []

        for league in new_bets["league_name"].unique():
            league_bets = new_bets[new_bets["league_name"] == league]
            messages = self.format_bet_messages(league_bets)

            league_success = True
            for i, message in enumerate(messages):
                if await self.send_message(message):
                    logger.info(
                        f"✅ Enviada parte {i + 1}/{len(messages)} da liga {league}"
                    )
                    await asyncio.sleep(1)
                else:
                    logger.error(f"❌ Falha ao enviar parte {i + 1} da liga {league}")
                    league_success = False
                    break

            if league_success:
                sent_bet_ids.extend(league_bets["id"].tolist())
                sent_count += len(league_bets)
                logger.info(
                    f"✅ Todas as {len(league_bets)} apostas da liga {league} enviadas"
                )

        if sent_bet_ids:
            self.mark_bets_as_sent(sent_bet_ids)
            logger.info(f"Total de apostas enviadas: {sent_count}")

        return sent_count

    async def send_profit_summary(self):
        profit_data = self.get_profit_summary()
        message = self.format_profit_message(profit_data)

        if await self.send_message(message):
            logger.info("✅ Resumo de lucros enviado")
        else:
            logger.error("❌ Falha ao enviar resumo de lucros")

    async def run(self):
        try:
            sent_count = await self.send_new_bets()
            await asyncio.sleep(2)
            await self.send_profit_summary()
            logger.info("✅ Execução concluída")
        except Exception as e:
            logger.error(f"❌ Erro durante execução: {e}")
            raise


async def main():
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
