import sqlite3
import pandas as pd
from datetime import datetime


def remove_tt_elite_ml_bets(db_path="bets.db", dry_run=True):
    """
    Remove apostas ML concluídas da TT Elite Series do banco de dados

    Args:
        db_path (str): Caminho para o banco de dados
        dry_run (bool): Se True, apenas mostra o que seria removido sem executar
    """

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Primeiro, vamos ver quantas apostas serão afetadas
    query_count = """
    SELECT 
        COUNT(*) as total_apostas,
        SUM(CASE WHEN result IS NOT NULL THEN 1 ELSE 0 END) as apostas_concluidas,
        SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as apostas_pendentes
    FROM bets 
    WHERE league_name LIKE '%TT Elite%' 
    AND bet_type = 'To Win'
    """

    df_count = pd.read_sql_query(query_count, conn)

    print("🔍 ANÁLISE DAS APOSTAS TT ELITE ML")
    print("=" * 50)
    print(f"Total de apostas ML TT Elite: {df_count['total_apostas'].iloc[0]}")
    print(f"Apostas concluídas (com result): {df_count['apostas_concluidas'].iloc[0]}")
    print(f"Apostas pendentes (sem result): {df_count['apostas_pendentes'].iloc[0]}")

    # Mostrar detalhes das apostas concluídas
    query_details = """
    SELECT 
        id, event_id, home_team, away_team, selection, odds, 
        estimated_roi, result, profit, created_at
    FROM bets 
    WHERE league_name LIKE '%TT Elite%' 
    AND bet_type = 'To Win'
    AND result IS NOT NULL
    ORDER BY created_at DESC
    """

    df_details = pd.read_sql_query(query_details, conn)

    if not df_details.empty:
        print(f"\n📊 DETALHES DAS {len(df_details)} APOSTAS CONCLUÍDAS:")
        print("-" * 80)

        # Estatísticas resumidas
        total_profit = df_details["profit"].sum()
        win_rate = (df_details["result"] == 1).mean() * 100
        avg_roi_estimated = df_details["estimated_roi"].mean()

        print(f"💰 Lucro total: {total_profit:.2f}u")
        print(f"📈 Taxa de acerto: {win_rate:.1f}%")
        print(f"📊 ROI estimado médio: {avg_roi_estimated:.1f}%")

        # Mostrar algumas apostas como exemplo
        print(f"\n📋 PRIMEIRAS 10 APOSTAS A SEREM REMOVIDAS:")
        print("-" * 80)

        display_cols = [
            "id",
            "home_team",
            "away_team",
            "selection",
            "odds",
            "result",
            "profit",
        ]
        print(df_details[display_cols].head(10).to_string(index=False))

        if len(df_details) > 10:
            print(f"\n... e mais {len(df_details) - 10} apostas")

    # Executar remoção ou dry run
    if dry_run:
        print(f"\n🔍 DRY RUN - Nenhuma aposta foi removida")
        print(f"Para executar a remoção, rode: remove_tt_elite_ml_bets(dry_run=False)")
    else:
        # Confirmar antes de remover
        print(
            f"\n⚠️  ATENÇÃO: Você está prestes a REMOVER {df_count['apostas_concluidas'].iloc[0]} apostas!"
        )
        confirm = input("Digite 'CONFIRMAR' para prosseguir: ")

        if confirm == "CONFIRMAR":
            # Executar remoção
            delete_query = """
            DELETE FROM bets 
            WHERE league_name LIKE '%TT Elite%' 
            AND bet_type = 'To Win'
            AND result IS NOT NULL
            """

            cursor.execute(delete_query)
            removed_count = cursor.rowcount
            conn.commit()

            print(
                f"✅ SUCESSO: {removed_count} apostas ML da TT Elite Series foram removidas!"
            )

            # Verificar resultado final
            df_final = pd.read_sql_query(query_count, conn)
            print(f"\n📊 SITUAÇÃO FINAL:")
            print(
                f"Total de apostas ML TT Elite restantes: {df_final['total_apostas'].iloc[0]}"
            )
            print(f"Apostas pendentes: {df_final['apostas_pendentes'].iloc[0]}")

        else:
            print("❌ Operação cancelada pelo usuário")

    conn.close()


def backup_database(db_path="bets.db", backup_path=None):
    """
    Cria backup do banco antes de fazer alterações
    """
    if backup_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"bets_backup_{timestamp}.db"

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
    print("🗑️  REMOVEDOR DE APOSTAS ML TT ELITE SERIES")
    print("=" * 60)

    # Opção 1: Fazer backup primeiro
    print("\n1️⃣  CRIAR BACKUP (RECOMENDADO)")
    create_backup = input("Criar backup antes de prosseguir? (s/n): ").lower()

    if create_backup == "s":
        backup_path = backup_database()
        print(f"Backup salvo em: {backup_path}")

    print("\n2️⃣  ANÁLISE DAS APOSTAS")

    # Opção 2: Dry run primeiro
    print("Executando análise (dry run)...")
    remove_tt_elite_ml_bets(dry_run=True)

    # Opção 3: Executar remoção
    print("\n3️⃣  EXECUTAR REMOÇÃO")
    execute_removal = input("\nExecutar remoção das apostas? (s/n): ").lower()

    if execute_removal == "s":
        remove_tt_elite_ml_bets(dry_run=False)
    else:
        print("❌ Remoção cancelada")


if __name__ == "__main__":
    main()
