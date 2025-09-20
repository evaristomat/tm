import subprocess
import time
import logging
from datetime import datetime
import asyncio
import sys
import os

# Carregar variáveis de ambiente
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    # Fallback caso dotenv não esteja disponível
    pass

# Configurar logging básico
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(
            f"run_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", encoding="utf-8"
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("main_runner")

# Obter configurações das variáveis de ambiente
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Verificar se as variáveis necessárias estão definidas
if not BOT_TOKEN or not CHAT_ID:
    logger.error("BOT_TOKEN ou CHAT_ID não configurados")
    logger.error("Certifique-se de definir estas variáveis de ambiente")
    sys.exit(1)


def run_script(script_name, description):
    """Executa um script Python e registra o resultado"""
    logger.info("=" * 60)
    logger.info(f"INICIANDO: {description.upper()}")
    logger.info(f"   Script: {script_name}")

    try:
        start_time = time.time()

        # Executar o script com encoding UTF-8
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"

        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True,
            check=True,
            env=env,
            encoding="utf-8",
        )

        elapsed_time = time.time() - start_time

        # Registrar saída se houver
        if result.stdout:
            # Filtrar apenas linhas importantes
            important_lines = [
                line
                for line in result.stdout.split("\n")
                if any(
                    keyword in line
                    for keyword in [
                        "partidas",
                        "apostas",
                        "atualizad",
                        "encontrad",
                        "processad",
                    ]
                )
            ]
            if important_lines:
                logger.info(f"   Saída relevante:")
                for line in important_lines[:5]:
                    logger.info(f"      {line.strip()}")

        logger.info(f"{description.upper()} CONCLUÍDO em {elapsed_time:.2f} segundos")
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"ERRO AO EXECUTAR {script_name}:")
        if e.stderr:
            error_lines = [
                line
                for line in e.stderr.split("\n")
                if any(
                    keyword in line.lower()
                    for keyword in ["error", "fail", "exception", "traceback"]
                )
            ]
            if error_lines:
                for line in error_lines[:3]:
                    logger.error(f"   {line.strip()}")
        return False
    except FileNotFoundError:
        logger.error(f"SCRIPT NÃO ENCONTRADO: {script_name}")
        return False
    except Exception as e:
        logger.error(f"ERRO INESPERADO AO EXECUTAR {script_name}: {e}")
        return False


async def run_telegram_script():
    """Executa o script do Telegram de forma assíncrona"""
    logger.info("=" * 60)
    logger.info(f"INICIANDO: ENVIO DE APOSTAS PARA TELEGRAM")

    try:
        # Importar e executar o script do Telegram
        from send_telegram import TelegramBetNotifier

        notifier = TelegramBetNotifier(BOT_TOKEN, CHAT_ID)
        await notifier.process_and_send_bets()

        logger.info(f"ENVIO PARA TELEGRAM CONCLUÍDO")
        return True

    except ImportError:
        logger.error(f"NAO FOI POSSIVEL IMPORTAR send_telegram.py")
        return False
    except Exception as e:
        logger.error(f"ERRO AO EXECUTAR SCRIPT DO TELEGRAM: {e}")
        return False

def commit_and_push_changes():
    """Faz commit e push de mudanças no repositório"""
    try:
        subprocess.run(
            ["git", "config", "--global", "user.email", "action@github.com"], check=True
        )
        subprocess.run(
            ["git", "config", "--global", "user.name", "GitHub Action"], check=True
        )

        # Verificar status
        result = subprocess.run(
            ["git", "status"], capture_output=True, text=True, check=True
        )
        logger.info(f"Git status:\n{result.stdout}")

        # Adicionar todos os arquivos modificados
        subprocess.run(["git", "add", "."], check=True)

        # Fazer commit
        commit_message = f"Auto-update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        subprocess.run(["git", "commit", "-m", commit_message], check=True)

        # Fazer push
        subprocess.run(["git", "push"], check=True)
        logger.info("Mudanças commitadas e pushadas com sucesso")

    except subprocess.CalledProcessError as e:
        logger.error(f"Erro ao commitar mudanças: {e}")

def main():
    """Função principal que executa todos os scripts em ordem"""

    logger.info("\n" + "=" * 60)
    logger.info("INICIANDO EXECUCAO COMPLETA DO PROJETO")
    logger.info(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # Lista de scripts para executar em ordem
    scripts = [
        {
            "file": "get_matches_last30.py",
            "description": "Buscar partidas dos últimos 30 dias",
            "required": True,
        },
        {
            "file": "monitor.py",
            "description": "Pega as odds",
            "required": True,
        },
        {
            "file": "db_get_bets.py",
            "description": "Processar e identificar apostas valiosas",
            "required": True,
        },
        {
            "file": "db_get_bet_results.py",
            "description": "Obter resultados das apostas",
            "required": False,
        },
    ]

    # Contador de sucessos e falhas
    success_count = 0
    fail_count = 0
    executed_scripts = []

    # Executar cada script
    for script in scripts:
        success = run_script(script["file"], script["description"])
        executed_scripts.append(
            {
                "name": script["file"],
                "description": script["description"],
                "success": success,
            }
        )

        if success:
            success_count += 1
        else:
            fail_count += 1

            if script["required"]:
                logger.error("SCRIPT OBRIGATORIO FALHOU. INTERROMPENDO EXECUCAO.")
                break

        time.sleep(2)

    # Executar script do Telegram
    telegram_success = False
    if success_count > 0:
        logger.info("\n" + "=" * 60)
        logger.info("EXECUTANDO ENVIO PARA TELEGRAM...")

        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            telegram_success = loop.run_until_complete(run_telegram_script())
            loop.close()
        except Exception as e:
            logger.error(f"ERRO NO LOOP ASSINCRONO: {e}")
            telegram_success = False

        if telegram_success:
            success_count += 1
        else:
            fail_count += 1

    # Resumo final
    logger.info("\n" + "=" * 60)
    logger.info("EXECUCAO FINALIZADA")
    logger.info(f"Scripts executados com sucesso: {success_count}")
    logger.info(f"Scripts com erro: {fail_count}")
    logger.info(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # Detalhamento dos scripts executados
    logger.info("\nDETALHAMENTO DA EXECUCAO:")
    logger.info("-" * 60)
    for script in executed_scripts:
        status = "✅" if script["success"] else "❌"
        logger.info(f"   {status} {script['description']}")

    if telegram_success is not None:
        status = "✅" if telegram_success else "❌"
        logger.info(f"   {status} Envio para Telegram")

    if success_count > 0:
        commit_and_push_changes()

    return success_count, fail_count


if __name__ == "__main__":
    main()
