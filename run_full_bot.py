import subprocess
import time
import logging
from datetime import datetime
import asyncio
import sys
import os
from colorama import init, Fore, Back, Style

# Inicializar colorama para cores no Windows
init()


# Configurar logging com cores
class ColoredFormatter(logging.Formatter):
    """Formata logs com cores"""

    COLORS = {
        "DEBUG": Fore.CYAN,
        "INFO": Fore.GREEN,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "CRITICAL": Fore.RED + Back.WHITE + Style.BRIGHT,
    }

    RESET = Style.RESET_ALL

    def format(self, record):
        # Adicionar emoji baseado no nível
        emoji = ""
        if record.levelname == "INFO":
            emoji = "📋 "
        elif record.levelname == "WARNING":
            emoji = "⚠️ "
        elif record.levelname == "ERROR":
            emoji = "❌ "
        elif record.levelname == "DEBUG":
            emoji = "🐛 "

        # Aplicar cor baseada no nível
        color = self.COLORS.get(record.levelname, Fore.WHITE)
        message = super().format(record)
        return f"{color}{emoji}{message}{self.RESET}"


# Configurar logging
def setup_logging():
    """Configura o sistema de logging com cores"""
    logger = logging.getLogger("main_runner")
    logger.setLevel(logging.INFO)

    # Remover handlers existentes
    if logger.handlers:
        logger.handlers.clear()

    # Handler para console com cores
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Formato para console
    console_formatter = ColoredFormatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_formatter)

    # Handler para arquivo (sem cores)
    file_handler = logging.FileHandler(
        f"run_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", encoding="utf-8"
    )
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_formatter)

    # Adicionar ambos handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


# Criar logger
logger = setup_logging()


def run_script(script_name, description):
    """Executa um script Python e registra o resultado"""
    logger.info("=" * 60)
    logger.info(f"🔄 INICIANDO: {description.upper()}")
    logger.info(f"   📂 Script: {script_name}")

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
                logger.info(f"   📊 Saída relevante:")
                for line in important_lines[:5]:  # Limitar a 5 linhas
                    logger.info(f"      {line.strip()}")

        logger.info(
            f"✅ {description.upper()} CONCLUÍDO em {elapsed_time:.2f} segundos"
        )
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"❌ ERRO AO EXECUTAR {script_name}:")
        if e.stderr:
            # Filtrar linhas de erro importantes
            error_lines = [
                line
                for line in e.stderr.split("\n")
                if any(
                    keyword in line.lower()
                    for keyword in ["error", "fail", "exception", "traceback"]
                )
            ]
            if error_lines:
                for line in error_lines[:3]:  # Limitar a 3 linhas de erro
                    logger.error(f"   🐛 {line.strip()}")
        return False
    except FileNotFoundError:
        logger.error(f"❌ SCRIPT NÃO ENCONTRADO: {script_name}")
        return False
    except Exception as e:
        logger.error(f"❌ ERRO INESPERADO AO EXECUTAR {script_name}: {e}")
        return False


async def run_telegram_script():
    """Executa o script do Telegram de forma assíncrona"""
    logger.info("=" * 60)
    logger.info(f"📱 INICIANDO: ENVIO DE APOSTAS PARA TELEGRAM")

    try:
        # Importar e executar o script do Telegram
        from send_telegram import TelegramBetNotifier

        # Configurações do Telegram (substitua pelos valores reais)
        BOT_TOKEN = "8393179861:AAE_5vgkSBHk9nMupfrEUX0spuz9lYt0i9c"
        CHAT_ID = "-1002840666957"

        notifier = TelegramBetNotifier(BOT_TOKEN, CHAT_ID)
        await notifier.process_and_send_bets()

        logger.info(f"✅ ENVIO PARA TELEGRAM CONCLUÍDO")
        return True

    except ImportError:
        logger.error(f"❌ NÃO FOI POSSÍVEL IMPORTAR send_telegram.py")
        return False
    except Exception as e:
        logger.error(f"❌ ERRO AO EXECUTAR SCRIPT DO TELEGRAM: {e}")
        return False


def main():
    """Função principal que executa todos os scripts em ordem"""

    logger.info(Fore.CYAN + "╔" + "═" * 58 + "╗")
    logger.info(
        Fore.CYAN
        + "║"
        + Fore.YELLOW
        + "          INICIANDO EXECUÇÃO COMPLETA DO PROJETO         "
        + Fore.CYAN
        + "║"
    )
    logger.info(
        Fore.CYAN
        + "║"
        + Fore.WHITE
        + f"    Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}    "
        + Fore.CYAN
        + "║"
    )
    logger.info(Fore.CYAN + "╚" + "═" * 58 + "╝" + Style.RESET_ALL)

    # Lista de scripts para executar em ordem
    scripts = [
        {
            "file": "get_matches_last30.py",
            "description": "Buscar partidas dos últimos 30 dias",
            "required": True,
        },
        {
            "file": "monitor.py",
            "description": "Monitorar e atualizar partidas ao vivo",
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
            "required": False,  # Pode falhar se não houver resultados ainda
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

            # Se o script é obrigatório e falhou, parar execução
            if script["required"]:
                logger.error("🛑 SCRIPT OBRIGATÓRIO FALHOU. INTERROMPENDO EXECUÇÃO.")
                break

        # Aguardar um pouco entre scripts para não sobrecarregar
        time.sleep(2)

    # Executar script do Telegram (assíncrono)
    telegram_success = False
    if success_count > 0:  # Só enviar se pelo menos um script funcionou
        logger.info("\n" + "=" * 60)
        logger.info("🤖 EXECUTANDO ENVIO PARA TELEGRAM...")

        # Criar loop assíncrono para o Telegram
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            telegram_success = loop.run_until_complete(run_telegram_script())
            loop.close()
        except Exception as e:
            logger.error(f"❌ ERRO NO LOOP ASSÍNCRONO: {e}")
            telegram_success = False

        if telegram_success:
            success_count += 1
        else:
            fail_count += 1

    # Resumo final
    logger.info(Fore.CYAN + "\n╔" + "═" * 58 + "╗")
    logger.info(
        Fore.CYAN
        + "║"
        + Fore.YELLOW
        + "               EXECUÇÃO FINALIZADA               "
        + Fore.CYAN
        + "║"
    )
    logger.info(Fore.CYAN + "╠" + "═" * 58 + "╣")
    logger.info(
        Fore.CYAN
        + "║"
        + Fore.WHITE
        + f"   Scripts executados com sucesso: {success_count}        "
        + Fore.CYAN
        + "║"
    )
    logger.info(
        Fore.CYAN
        + "║"
        + Fore.WHITE
        + f"   Scripts com erro: {fail_count}                 "
        + Fore.CYAN
        + "║"
    )
    logger.info(
        Fore.CYAN
        + "║"
        + Fore.WHITE
        + f"   Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}   "
        + Fore.CYAN
        + "║"
    )
    logger.info(Fore.CYAN + "╚" + "═" * 58 + "╝" + Style.RESET_ALL)

    # Detalhamento dos scripts executados
    logger.info("\n" + "📊 DETALHAMENTO DA EXECUÇÃO:")
    logger.info("-" * 60)
    for script in executed_scripts:
        status = "✅" if script["success"] else "❌"
        logger.info(f"   {status} {script['description']}")

    if telegram_success is not None:
        status = "✅" if telegram_success else "❌"
        logger.info(f"   {status} Envio para Telegram")

    return success_count, fail_count


def run_continuous(interval_minutes=30):
    """Executa o processo continuamente em intervalos"""

    logger.info(f"🔁 MODO CONTÍNUO ATIVADO - INTERVALO: {interval_minutes} MINUTOS")

    while True:
        try:
            # Executar todos os scripts
            success, fails = main()

            # Aguardar próximo ciclo
            logger.info(
                f"⏰ AGUARDANDO {interval_minutes} MINUTOS PARA PRÓXIMA EXECUÇÃO..."
            )
            time.sleep(interval_minutes * 60)

        except KeyboardInterrupt:
            logger.info("🛑 EXECUÇÃO CONTÍNUA INTERROMPIDA PELO USUÁRIO")
            break
        except Exception as e:
            logger.error(f"❌ ERRO NO LOOP CONTÍNUO: {e}")
            logger.info(
                f"⏰ AGUARDANDO {interval_minutes} MINUTOS PARA TENTAR NOVAMENTE..."
            )
            time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    # Verificar se colorama está instalado, se não, instalar
    try:
        import colorama
    except ImportError:
        logger.warning("📦 INSTALANDO COLORAMA PARA SUPORTE A CORES...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "colorama"])
        import colorama

        init()  # Reinicializar após instalação

    import argparse

    parser = argparse.ArgumentParser(
        description="Executar todos os scripts do projeto de apostas"
    )
    parser.add_argument(
        "--continuous", action="store_true", help="Executar continuamente em intervalos"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Intervalo em minutos para execução contínua (padrão: 30)",
    )

    args = parser.parse_args()

    if args.continuous:
        run_continuous(args.interval)
    else:
        main()
