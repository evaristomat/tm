import sqlite3
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("db_update")


def add_actual_result_column():
    """Adiciona a coluna actual_result ao banco de dados bets.db"""

    try:
        conn = sqlite3.connect("bets.db")
        cursor = conn.cursor()

        # Verificar se a coluna já existe
        cursor.execute("PRAGMA table_info(bets)")
        columns = [column[1] for column in cursor.fetchall()]

        if "actual_result" in columns:
            logger.info("✅ Coluna 'actual_result' já existe")
        else:
            # Adicionar a coluna
            cursor.execute("""
                ALTER TABLE bets 
                ADD COLUMN actual_result TEXT
            """)
            conn.commit()
            logger.info("✅ Coluna 'actual_result' adicionada com sucesso")

        # Verificar a estrutura atualizada
        cursor.execute("PRAGMA table_info(bets)")
        columns = cursor.fetchall()

        logger.info("\n📋 Estrutura atualizada da tabela 'bets':")
        for col in columns:
            logger.info(f"  • {col[1]}: {col[2]}")

        conn.close()

    except sqlite3.Error as e:
        logger.error(f"❌ Erro ao adicionar coluna: {e}")
    except Exception as e:
        logger.error(f"❌ Erro inesperado: {e}")


if __name__ == "__main__":
    add_actual_result_column()
