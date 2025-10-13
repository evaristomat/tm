import sqlite3
import pandas as pd
import numpy as np
import logging
from colorama import Fore, Style, init

init(autoreset=True)  # Inicializa colorama para resetar a cor automaticamente

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("bet_stats_analyzer")


class BetStatsAnalyzer:
    def __init__(self, bets_db_path="bets.db"):
        self.bets_db_path = bets_db_path
        self.df_bets = self._load_bets_data()

    def _load_bets_data(self):
        """Carrega os dados da tabela 'bets' do banco de dados."""
        try:
            conn = sqlite3.connect(self.bets_db_path)
            df = pd.read_sql_query("SELECT * FROM bets WHERE result IS NOT NULL", conn)
            conn.close()
            # Converte 'handicap' para numérico, tratando possíveis erros
            df["handicap"] = pd.to_numeric(df["handicap"], errors="coerce")
            # Remove linhas onde 'handicap' se tornou NaN após a conversão (se houver)
            df.dropna(subset=["handicap"], inplace=True)
            logger.info(f"Dados de {len(df)} apostas carregados com sucesso.")
            return df
        except Exception as e:
            logger.error(f"Erro ao carregar dados do banco de dados: {e}")
            return pd.DataFrame()

    def _calculate_metrics(self, df):
        """Calcula métricas de desempenho para um DataFrame de apostas."""
        if df.empty:
            return {
                "Total de Apostas": 0,
                "Vitórias": 0,
                "Derrotas": 0,
                "Taxa de Acerto": "0.00%",
                "Lucro Total (u)": 0.0,
                "ROI (%)": "0.00%",
            }

        total_bets = len(df)
        wins = df["result"].sum()
        losses = total_bets - wins
        win_rate = (wins / total_bets) * 100 if total_bets > 0 else 0
        total_profit = df["profit"].sum()
        roi = (total_profit / total_bets) * 100 if total_bets > 0 else 0

        return {
            "Total de Apostas": total_bets,
            "Vitórias": wins,
            "Derrotas": losses,
            "Taxa de Acerto": f"{win_rate:.2f}%",
            "Lucro Total (u)": f"{total_profit:.2f}",
            "ROI (%)": f"{roi:.2f}%",
        }

    def get_overall_stats(self):
        """Retorna estatísticas gerais de todas as apostas."""
        logger.info("Calculando estatísticas gerais...")
        return self._calculate_metrics(self.df_bets)

    def get_stats_by_league(self):
        """Retorna estatísticas agrupadas por liga."""
        logger.info("Calculando estatísticas por liga...")
        if self.df_bets.empty:
            return pd.DataFrame()

        # Usar observed=False para evitar o FutureWarning com categorias
        grouped = self.df_bets.groupby("league_name", observed=False).apply(
            self._calculate_metrics
        )
        return grouped.apply(pd.Series)

    def get_stats_by_market_by_league(self):
        """Retorna estatísticas agrupadas por liga e tipo de mercado."""
        logger.info("Calculando estatísticas por mercado por liga...")
        if self.df_bets.empty:
            return pd.DataFrame()

        grouped = self.df_bets.groupby(
            ["league_name", "bet_type"], observed=False
        ).apply(self._calculate_metrics)
        return grouped.apply(pd.Series)

    def get_stats_by_odds_range(self):
        """Retorna estatísticas agrupadas por liga, mercado e faixa de odds."""
        logger.info("Calculando estatísticas por faixa de odds...")
        if self.df_bets.empty:
            return pd.DataFrame()

        # Define as faixas de odds
        bins = [0, 1.5, 2.0, 3.0, 5.0, 10.0, np.inf]
        labels = ["<1.5", "1.5-2.0", "2.0-3.0", "3.0-5.0", "5.0-10.0", ">10.0"]
        self.df_bets["odds_range"] = pd.cut(
            self.df_bets["odds"], bins=bins, labels=labels, right=False
        )

        grouped = self.df_bets.groupby(
            ["league_name", "bet_type", "odds_range"], observed=False
        ).apply(self._calculate_metrics)
        return grouped.apply(pd.Series)

    def get_stats_by_roi_range(self):
        """Retorna estatísticas agrupadas por liga, mercado e faixa de ROI."""
        logger.info("Calculando estatísticas por faixa de ROI...")
        if self.df_bets.empty:
            return pd.DataFrame()

        # Define as faixas de ROI
        bins = [-np.inf, -50, -25, 0, 5, 10, 25, 50, np.inf]
        labels = [
            "<-50%",
            "-50% a -25%",
            "-25% a 0%",
            "0% a 5%",
            "5% a 10%",
            "10% a 25%",
            "25% a 50%",
            ">50%",
        ]
        self.df_bets["roi_range"] = pd.cut(
            self.df_bets["estimated_roi"], bins=bins, labels=labels, right=False
        )

        grouped = self.df_bets.groupby(
            ["league_name", "bet_type", "roi_range"], observed=False
        ).apply(self._calculate_metrics)
        return grouped.apply(pd.Series)


def main():
    analyzer = BetStatsAnalyzer()

    print(f"\n{Fore.CYAN}{Style.BRIGHT}{'=' * 60}{Style.RESET_ALL}")
    print(
        f"{Fore.CYAN}{Style.BRIGHT}{'ESTATÍSTICAS GERAIS DE APOSTAS'.center(60)}{Style.RESET_ALL}"
    )
    print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 60}{Style.RESET_ALL}")
    overall_stats = analyzer.get_overall_stats()
    for k, v in overall_stats.items():
        print(f"{Fore.GREEN}{k:<20}{Style.RESET_ALL}: {v}")

    print(f"\n{Fore.CYAN}{Style.BRIGHT}{'=' * 60}{Style.RESET_ALL}")
    print(
        f"{Fore.CYAN}{Style.BRIGHT}{'ESTATÍSTICAS POR LIGA'.center(60)}{Style.RESET_ALL}"
    )
    print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 60}{Style.RESET_ALL}")
    stats_by_league = analyzer.get_stats_by_league()
    if not stats_by_league.empty:
        print(stats_by_league.to_markdown(numalign="left", stralign="left"))
    else:
        print("Nenhuma estatística por liga disponível.")

    print(f"\n{Fore.CYAN}{Style.BRIGHT}{'=' * 60}{Style.RESET_ALL}")
    print(
        f"{Fore.CYAN}{Style.BRIGHT}{'ESTATÍSTICAS POR MERCADO E LIGA'.center(60)}{Style.RESET_ALL}"
    )
    print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 60}{Style.RESET_ALL}")
    stats_by_market_by_league = analyzer.get_stats_by_market_by_league()
    if not stats_by_market_by_league.empty:
        print(stats_by_market_by_league.to_markdown(numalign="left", stralign="left"))
    else:
        print("Nenhuma estatística por mercado por liga disponível.")

    print(f"\n{Fore.CYAN}{Style.BRIGHT}{'=' * 60}{Style.RESET_ALL}")
    print(
        f"{Fore.CYAN}{Style.BRIGHT}{'ESTATÍSTICAS POR FAIXA DE ODDS (LIGA E MERCADO)'.center(60)}{Style.RESET_ALL}"
    )
    print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 60}{Style.RESET_ALL}")
    stats_by_odds_range = analyzer.get_stats_by_odds_range()
    if not stats_by_odds_range.empty:
        print(stats_by_odds_range.to_markdown(numalign="left", stralign="left"))
    else:
        print("Nenhuma estatística por faixa de odds disponível.")

    print(f"\n{Fore.CYAN}{Style.BRIGHT}{'=' * 60}{Style.RESET_ALL}")
    print(
        f"{Fore.CYAN}{Style.BRIGHT}{'ESTATÍSTICAS POR FAIXA DE ROI (LIGA E MERCADO)'.center(60)}{Style.RESET_ALL}"
    )
    print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 60}{Style.RESET_ALL}")
    stats_by_roi_range = analyzer.get_stats_by_roi_range()
    if not stats_by_roi_range.empty:
        print(stats_by_roi_range.to_markdown(numalign="left", stralign="left"))
    else:
        print("Nenhuma estatística por faixa de ROI disponível.")


if __name__ == "__main__":
    main()
