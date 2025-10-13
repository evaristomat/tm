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
        self.MAX_MESSAGE_LENGTH = 4096  # Limite do Telegram

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

        # Buscar apostas não enviadas (apenas Total)
        query = """
        SELECT 
            b.id, b.league_name, b.home_team, b.away_team, 
            b.event_time, b.bet_type, b.selection, b.handicap, 
            b.odds, b.estimated_roi
        FROM bets b
        LEFT JOIN telegram_sent_bets t ON b.id = t.bet_id
        WHERE t.bet_id IS NULL AND b.bet_type = 'Total'
        ORDER BY b.league_name, b.event_time ASC
        """

        df = pd.read_sql_query(query, conn)

        if not df.empty:
            logger.info(f"Apostas Over/Under não enviadas encontradas: {len(df)}")

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
        """Busca resumo de lucros por liga, focado em O/U"""
        conn = sqlite3.connect(self.bets_db_path)

        query = """
        SELECT 
            league_name,
            SUM(CASE WHEN result = 1 THEN profit WHEN result = 0 THEN -1 ELSE 0 END) as total_profit,
            COUNT(CASE WHEN result IS NOT NULL THEN 1 END) as total_bets,
            COUNT(CASE WHEN result = 1 THEN 1 END) as wins,
            COUNT(CASE WHEN result = 0 THEN 1 END) as losses,
            SUM(CASE WHEN bet_type = 'Total' AND selection LIKE 'Over%' AND result = 1 THEN profit 
                     WHEN bet_type = 'Total' AND selection LIKE 'Over%' AND result = 0 THEN -1 ELSE 0 END) as over_profit,
            COUNT(CASE WHEN bet_type = 'Total' AND selection LIKE 'Over%' AND result IS NOT NULL THEN 1 END) as over_total,
            SUM(CASE WHEN bet_type = 'Total' AND selection LIKE 'Under%' AND result = 1 THEN profit 
                     WHEN bet_type = 'Total' AND selection LIKE 'Under%' AND result = 0 THEN -1 ELSE 0 END) as under_profit,
            COUNT(CASE WHEN bet_type = 'Total' AND selection LIKE 'Under%' AND result IS NOT NULL THEN 1 END) as under_total
        FROM bets
        WHERE result IS NOT NULL AND bet_type = 'Total'
        GROUP BY league_name
        ORDER BY total_profit DESC
        """

        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def format_bet_messages(self, league_bets):
        """Formata mensagens de apostas por liga, dividindo se necessário (apenas O/U)"""
        league_name = league_bets.iloc[0]["league_name"]

        league_icons = {
            "Czech Liga Pro": "🇨🇿",
            "TT Elite Series": "⭐",
            "Challenger Series TT": "🏓",
            "TT Cup": "🏆",
            "Setka Cup": "🇺🇦",  # Adicionado ícone para Setka Cup
            "Setka Cup Women": "♀️🇺🇦",  # Adicionado ícone para Setka Cup Women
        }

        icon = league_icons.get(league_name, "🏓")
        header = f"{icon} *{league_name}*\n"
        header += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        messages = []
        current_message = header

        def format_ou_section(bets, section_title):
            if bets.empty:
                return ""

            bets_sorted = bets.sort_values("event_time")

            section = f"{section_title}\n"
            for _, bet in bets_sorted.iterrows():
                time_str = pd.to_datetime(bet["event_time"]).strftime("%d/%m %H:%M")
                tip = f"{bet['selection']} {bet['handicap']:.1f}"

                section += f"🆚 {bet['home_team']} vs {bet['away_team']}\n"
                section += f"🎯 {tip} | 📊 {bet['odds']:.2f} | ⏰ {time_str}\n"
                section += f"📈 ROI: {bet['estimated_roi']:.1f}%\n\n"

            return section

        # Processar OU bets
        if not league_bets.empty:
            ou_section = format_ou_section(league_bets, "🔢 *OVER/UNDER*")

            if len(current_message + ou_section) > self.MAX_MESSAGE_LENGTH:
                # Finalizar mensagem atual se tiver conteúdo
                if len(current_message) > len(header):
                    messages.append(current_message)
                    current_message = header

                # Dividir OU bets
                temp_section = "🔢 *OVER/UNDER*\n"
                ou_sorted = league_bets.sort_values("event_time")
                for _, bet in ou_sorted.iterrows():
                    time_str = pd.to_datetime(bet["event_time"]).strftime("%d/%m %H:%M")
                    tip = f"{bet['selection']} {bet['handicap']:.1f}"

                    bet_text = f"🆚 {bet['home_team']} vs {bet['away_team']}\n"
                    bet_text += f"🎯 {tip} | 📊 {bet['odds']:.2f} | ⏰ {time_str}\n"
                    bet_text += f"📈 ROI: {bet['estimated_roi']:.1f}%\n\n"

                    if (
                        len(current_message + temp_section + bet_text)
                        > self.MAX_MESSAGE_LENGTH
                    ):
                        if temp_section != "🔢 *OVER/UNDER*\n":
                            current_message += temp_section
                            messages.append(current_message)
                            current_message = header
                            temp_section = "🔢 *OVER/UNDER*\n"

                    temp_section += bet_text

                if temp_section != "🔢 *OVER/UNDER*\n":
                    current_message += temp_section
            else:
                current_message += ou_section

        # Adicionar última mensagem
        if len(current_message) > len(header):
            messages.append(current_message)

        return messages

    def format_profit_message(self, profit_data):
        """Formata mensagem de resumo de lucros (apenas O/U)"""
        if profit_data.empty:
            return "📊 *RESUMO DE LUCROS O/U*\n\nNenhum dado disponível ainda."

        message = "💰 *RESUMO DE LUCROS O/U*\n"
        message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        total_profit_overall = 0
        total_bets_overall = 0
        total_over_bets_overall = 0
        total_under_bets_overall = 0
        total_over_profit_overall = 0
        total_under_profit_overall = 0

        for _, row in profit_data.iterrows():
            roi = (
                (row["total_profit"] / row["total_bets"] * 100)
                if row["total_bets"] > 0
                else 0
            )
            status = "✅" if row["total_profit"] > 0 else "❌"

            message += f"🏓 *{row['league_name']}*\n"
            message += f"{status} {row['total_profit']:+.2f}u | ROI: {roi:+.1f}% | {row['wins']}W-{row['losses']}L\n"
            message += (
                f"├ Over: {row['over_profit']:+.2f}u ({row['over_total']} apostas)\n"
            )
            message += f"└ Under: {row['under_profit']:+.2f}u ({row['under_total']} apostas)\n\n"

            total_profit_overall += row["total_profit"]
            total_bets_overall += row["total_bets"]
            total_over_bets_overall += row["over_total"]
            total_under_bets_overall += row["under_total"]
            total_over_profit_overall += row["over_profit"]
            total_under_profit_overall += row["under_profit"]

        total_roi_overall = (
            (total_profit_overall / total_bets_overall * 100)
            if total_bets_overall > 0
            else 0
        )
        total_status_overall = "✅" if total_profit_overall > 0 else "❌"

        message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        message += f"📊 *TOTAL GERAL O/U*\n"
        message += f"{total_status_overall} {total_profit_overall:+.2f}u | ROI: {total_roi_overall:+.1f}% | {total_bets_overall} apostas\n"
        message += f"├ Total Over: {total_over_profit_overall:+.2f}u ({total_over_bets_overall} apostas)\n"
        message += f"└ Total Under: {total_under_profit_overall:+.2f}u ({total_under_bets_overall} apostas)"

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
