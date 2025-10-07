import sqlite3
import pandas as pd
from datetime import datetime
from tabulate import tabulate


def analyze_duplicates(db_path="bets.db", dry_run=True):
    """
    Analisa e remove apostas duplicadas do banco de dados

    Args:
        db_path (str): Caminho para o banco de dados
        dry_run (bool): Se True, apenas mostra o que seria removido sem executar
    """

    conn = sqlite3.connect(db_path)

    print("🔍 ANÁLISE DE APOSTAS DUPLICADAS")
    print("=" * 60)

    # Query para encontrar duplicatas baseada no UNIQUE constraint
    # (event_id, bet_type, selection, handicap)
    query_duplicates = """
    SELECT 
        event_id, bet_type, selection, handicap,
        COUNT(*) as total_duplicates,
        GROUP_CONCAT(id) as duplicate_ids,
        GROUP_CONCAT(created_at) as created_dates,
        GROUP_CONCAT(estimated_roi) as roi_values
    FROM bets 
    GROUP BY event_id, bet_type, selection, handicap
    HAVING COUNT(*) > 1
    ORDER BY total_duplicates DESC, event_id
    """

    df_duplicates = pd.read_sql_query(query_duplicates, conn)

    if df_duplicates.empty:
        print("✅ NENHUMA DUPLICATA ENCONTRADA!")
        print("Seu banco de dados está limpo.")
        conn.close()
        return

    print(f"🚨 ENCONTRADAS {len(df_duplicates)} GRUPOS DE DUPLICATAS")
    print(
        f"📊 Total de apostas duplicadas: {df_duplicates['total_duplicates'].sum() - len(df_duplicates)}"
    )

    # Mostrar estatísticas por tipo
    print(f"\n📈 ESTATÍSTICAS POR TIPO DE APOSTA:")
    print("-" * 40)

    ml_duplicates = df_duplicates[df_duplicates["bet_type"] == "To Win"]
    ou_duplicates = df_duplicates[df_duplicates["bet_type"] == "Total"]

    print(f"Money Line (ML): {len(ml_duplicates)} grupos duplicados")
    print(f"Over/Under (O/U): {len(ou_duplicates)} grupos duplicados")

    # Mostrar estatísticas por liga
    query_league_stats = """
    SELECT 
        league_name,
        COUNT(*) as duplicate_groups,
        SUM(duplicate_count - 1) as extra_bets
    FROM (
        SELECT 
            league_name,
            event_id, bet_type, selection, handicap,
            COUNT(*) as duplicate_count
        FROM bets 
        GROUP BY league_name, event_id, bet_type, selection, handicap
        HAVING COUNT(*) > 1
    ) 
    GROUP BY league_name
    ORDER BY extra_bets DESC
    """

    df_league_stats = pd.read_sql_query(query_league_stats, conn)

    if not df_league_stats.empty:
        print(f"\n📊 DUPLICATAS POR LIGA:")
        print(tabulate(df_league_stats, headers="keys", tablefmt="grid"))

    # Mostrar exemplos de duplicatas
    print(f"\n📋 PRIMEIROS 10 GRUPOS DE DUPLICATAS:")
    print("-" * 80)

    display_df = df_duplicates.head(10).copy()
    display_df["duplicate_ids"] = display_df["duplicate_ids"].str[:50] + "..."
    display_df["created_dates"] = display_df["created_dates"].str[:50] + "..."

    print(tabulate(display_df, headers="keys", tablefmt="grid", showindex=False))

    if len(df_duplicates) > 10:
        print(f"\n... e mais {len(df_duplicates) - 10} grupos de duplicatas")

    # Análise detalhada de algumas duplicatas
    print(f"\n🔍 ANÁLISE DETALHADA (Primeiras 3 duplicatas):")
    print("-" * 60)

    for idx, row in df_duplicates.head(3).iterrows():
        event_id = row["event_id"]
        bet_type = row["bet_type"]
        selection = row["selection"]
        handicap = row["handicap"]
        ids = row["duplicate_ids"].split(",")

        print(f"\n📌 Grupo {idx + 1}:")
        print(f"   Event ID: {event_id} | Tipo: {bet_type} | Seleção: {selection}")
        if handicap:
            print(f"   Handicap: {handicap}")

        # Buscar detalhes de cada duplicata
        detail_query = """
        SELECT id, home_team, away_team, league_name, odds, estimated_roi, 
               created_at, result, profit
        FROM bets 
        WHERE id IN ({})
        ORDER BY created_at
        """.format(",".join(["?" for _ in ids]))

        df_details = pd.read_sql_query(detail_query, conn, params=ids)

        for _, detail in df_details.iterrows():
            status = "✅ Concluída" if detail["result"] is not None else "⏳ Pendente"
            profit_info = (
                f"(Lucro: {detail['profit']:.2f}u)"
                if detail["profit"] is not None
                else ""
            )
            print(
                f"   ID {detail['id']}: {detail['home_team']} vs {detail['away_team']}"
            )
            print(
                f"   Liga: {detail['league_name']} | Odds: {detail['odds']:.2f} | ROI: {detail['estimated_roi']:.1f}%"
            )
            print(f"   Criada: {detail['created_at']} | Status: {status} {profit_info}")

    if dry_run:
        print(f"\n🔍 DRY RUN - Nenhuma duplicata foi removida")
        print(f"Para executar a remoção, rode: analyze_duplicates(dry_run=False)")

        # Mostrar estratégia de remoção
        print(f"\n📋 ESTRATÉGIA DE REMOÇÃO:")
        print("-" * 30)
        print("1. ✅ Manter a aposta mais ANTIGA (primeiro registro)")
        print("2. ❌ Remover todas as duplicatas posteriores")
        print("3. 🛡️ Preservar apostas com resultado/lucro se existirem")
        print("4. 📊 Relatório detalhado do que foi removido")

    else:
        # Executar remoção
        print(f"\n⚠️  ATENÇÃO: Você está prestes a REMOVER duplicatas!")
        total_to_remove = df_duplicates["total_duplicates"].sum() - len(df_duplicates)
        print(f"📊 Total de apostas a serem removidas: {total_to_remove}")

        confirm = input("Digite 'CONFIRMAR' para prosseguir: ")

        if confirm == "CONFIRMAR":
            remove_duplicates(conn, df_duplicates)
        else:
            print("❌ Operação cancelada pelo usuário")

    conn.close()


def remove_duplicates(conn, df_duplicates):
    """Remove duplicatas mantendo sempre o registro mais antigo"""

    cursor = conn.cursor()
    total_removed = 0

    print(f"\n🗑️  INICIANDO REMOÇÃO DE DUPLICATAS...")
    print("-" * 50)

    for idx, row in df_duplicates.iterrows():
        event_id = row["event_id"]
        bet_type = row["bet_type"]
        selection = row["selection"]
        handicap = row["handicap"]
        ids = row["duplicate_ids"].split(",")

        # Buscar detalhes ordenados por data de criação
        detail_query = """
        SELECT id, created_at, result, profit
        FROM bets 
        WHERE id IN ({})
        ORDER BY created_at ASC
        """.format(",".join(["?" for _ in ids]))

        df_details = pd.read_sql_query(detail_query, conn, params=ids)

        # Manter o primeiro (mais antigo), remover os outros
        keep_id = df_details.iloc[0]["id"]
        remove_ids = df_details.iloc[1:]["id"].tolist()

        print(f"📌 Evento {event_id} - {bet_type} {selection}:")
        print(f"   ✅ Mantendo ID {keep_id} (mais antigo)")
        print(f"   ❌ Removendo IDs: {', '.join(map(str, remove_ids))}")

        # Remover duplicatas
        for remove_id in remove_ids:
            try:
                cursor.execute("DELETE FROM bets WHERE id = ?", (remove_id,))
                if cursor.rowcount > 0:
                    total_removed += 1
                    print(f"   🗑️  ID {remove_id} removido com sucesso")
                else:
                    print(f"   ⚠️  ID {remove_id} não encontrado")
            except Exception as e:
                print(f"   ❌ Erro ao remover ID {remove_id}: {e}")

    # Commit das alterações
    conn.commit()

    print(f"\n✅ REMOÇÃO CONCLUÍDA!")
    print(f"📊 Total de duplicatas removidas: {total_removed}")

    # Verificação final
    verify_query = """
    SELECT COUNT(*) as remaining_duplicates
    FROM (
        SELECT event_id, bet_type, selection, handicap, COUNT(*) as cnt
        FROM bets 
        GROUP BY event_id, bet_type, selection, handicap
        HAVING COUNT(*) > 1
    )
    """

    remaining = pd.read_sql_query(verify_query, conn)["remaining_duplicates"].iloc[0]

    if remaining == 0:
        print("🎉 SUCESSO: Nenhuma duplicata restante!")
    else:
        print(f"⚠️  ATENÇÃO: Ainda restam {remaining} grupos de duplicatas")


def backup_database(db_path="bets.db", backup_path=None):
    """Cria backup do banco antes de fazer alterações"""
    if backup_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"bets_backup_dedup_{timestamp}.db"

    # Conectar aos bancos
    source = sqlite3.connect(db_path)
    backup = sqlite3.connect(backup_path)

    # Fazer backup
    source.backup(backup)

    source.close()
    backup.close()

    print(f"✅ Backup criado: {backup_path}")
    return backup_path


def main():
    print("🗑️  REMOVEDOR DE APOSTAS DUPLICADAS")
    print("=" * 60)

    # Opção 1: Fazer backup primeiro
    print("\n1️⃣  CRIAR BACKUP (RECOMENDADO)")
    create_backup = input("Criar backup antes de prosseguir? (s/n): ").lower()

    if create_backup == "s":
        backup_path = backup_database()
        print(f"Backup salvo em: {backup_path}")

    print("\n2️⃣  ANÁLISE DE DUPLICATAS")

    # Opção 2: Dry run primeiro
    print("Executando análise (dry run)...")
    analyze_duplicates(dry_run=True)

    # Opção 3: Executar remoção
    print("\n3️⃣  EXECUTAR REMOÇÃO")
    execute_removal = input("\nExecutar remoção das duplicatas? (s/n): ").lower()

    if execute_removal == "s":
        analyze_duplicates(dry_run=False)
    else:
        print("❌ Remoção cancelada")


if __name__ == "__main__":
    main()
