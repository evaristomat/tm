import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from colorama import Fore, Style, init

init(autoreset=True)


class BetCleaner:
    def __init__(self, db_path="bets.db"):
        self.db_path = db_path
        self.conn = None
        self.duplicates = None

    def connect(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
            return True
        except sqlite3.Error as e:
            print(f"{Fore.RED}Erro ao conectar ao banco de dados: {e}")
            return False

    def find_duplicates(self, time_threshold_hours=6):
        query = """
        SELECT 
            id, event_id, league_name, home_team, away_team, 
            event_time, bet_type, selection, odds, created_at
        FROM bets
        WHERE result IS NULL
        ORDER BY league_name, home_team, away_team, event_time
        """

        try:
            df = pd.read_sql_query(query, self.conn)
            df["event_time"] = pd.to_datetime(df["event_time"])

            duplicates = []
            grouped = df.groupby(
                ["league_name", "home_team", "away_team", "bet_type", "selection"]
            )

            for name, group in grouped:
                if len(group) > 1:
                    group = group.sort_values("event_time")
                    for i in range(len(group)):
                        for j in range(i + 1, len(group)):
                            time_diff = abs(
                                (
                                    group.iloc[i]["event_time"]
                                    - group.iloc[j]["event_time"]
                                ).total_seconds()
                                / 3600
                            )
                            if time_diff <= time_threshold_hours:
                                duplicates.append(
                                    {
                                        "group_key": name,
                                        "ids": [
                                            int(group.iloc[i]["id"]),
                                            int(group.iloc[j]["id"]),
                                        ],
                                        "rows": [
                                            group.iloc[i].to_dict(),
                                            group.iloc[j].to_dict(),
                                        ],
                                        "time_diff_hours": time_diff,
                                    }
                                )

            self.duplicates = duplicates
            return duplicates

        except Exception as e:
            print(f"{Fore.RED}Erro ao buscar duplicatas: {e}")
            return []

    def display_duplicates(self):
        if not self.duplicates:
            print(f"{Fore.YELLOW}Nenhuma duplicata encontrada.")
            return

        print(f"\n{Fore.CYAN}=== APOSTAS DUPLICADAS ENCONTRADAS ==={Style.RESET_ALL}")

        for i, dup in enumerate(self.duplicates):
            print(f"\n{Fore.MAGENTA}Grupo {i + 1}: {dup['group_key']}")
            print(
                f"{Fore.YELLOW}Diferença de horário: {dup['time_diff_hours']:.2f} horas"
            )

            for j, row in enumerate(dup["rows"]):
                status = f"{Fore.GREEN}MANTER" if j == 0 else f"{Fore.RED}REMOVER"
                print(f"\n{status} - ID: {row['id']}")
                print(f"Event ID: {row['event_id']}")
                print(f"Horário: {row['event_time']}")
                print(f"Odds: {row['odds']}")

    def remove_duplicates(self):
        if not self.duplicates:
            print(f"{Fore.YELLOW}Nenhuma duplicata para remover.")
            return

        cursor = self.conn.cursor()
        removed_count = 0

        try:
            for dup in self.duplicates:
                ids = dup["ids"]
                keep_id = max(ids)

                for bet_id in ids:
                    if bet_id != keep_id:
                        cursor.execute("DELETE FROM bets WHERE id = ?", (bet_id,))
                        removed_count += 1
                        print(f"{Fore.RED}Removendo aposta ID: {bet_id}")

            self.conn.commit()
            print(f"\n{Fore.GREEN}Total de apostas removidas: {removed_count}")

        except Exception as e:
            self.conn.rollback()
            print(f"{Fore.RED}Erro ao remover duplicatas: {e}")

    def run(self):
        if not self.connect():
            return

        print(f"{Fore.CYAN}=== LIMPEZA DE APOSTAS DUPLICADAS ==={Style.RESET_ALL}")

        self.find_duplicates()
        self.display_duplicates()

        if self.duplicates:
            self.remove_duplicates()

        self.conn.close()


if __name__ == "__main__":
    cleaner = BetCleaner()
    cleaner.run()
