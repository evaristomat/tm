import sqlite3
import pandas as pd


def check_database_structure(db_path, db_name):
    """Verifica e exibe a estrutura de um banco de dados"""
    print(f"\n{'=' * 60}")
    print(f"📊 ESTRUTURA DO BANCO: {db_name}")
    print(f"   Arquivo: {db_path}")
    print(f"{'=' * 60}")

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Listar todas as tabelas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        print(f"\n📋 Tabelas encontradas: {len(tables)}")

        for table in tables:
            table_name = table[0]
            print(f"\n▶️  Tabela: {table_name}")
            print("-" * 40)

            # Obter estrutura da tabela
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()

            print("Colunas:")
            for col in columns:
                col_id, col_name, col_type, not_null, default, pk = col
                pk_marker = " [PK]" if pk else ""
                null_marker = " NOT NULL" if not_null else ""
                default_marker = f" DEFAULT {default}" if default else ""
                print(
                    f"  • {col_name}: {col_type}{pk_marker}{null_marker}{default_marker}"
                )

            # Contar registros
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"\n  Total de registros: {count}")

            # Mostrar amostra se houver dados
            if count > 0 and count <= 5:
                print(f"\n  Amostra dos dados:")
                df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 3", conn)
                print(df.to_string(index=False))

        conn.close()

    except sqlite3.Error as e:
        print(f"❌ Erro ao acessar {db_name}: {e}")
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")


def main():
    print("\n" + "🔍 VERIFICAÇÃO DE ESTRUTURA DOS BANCOS DE DADOS".center(60, "="))

    # Verificar bets.db
    check_database_structure("bets.db", "BETS.DB")

    # Verificar table_tennis_results.db
    check_database_structure("table_tennis_results.db", "TABLE_TENNIS_RESULTS.DB")

    print("\n" + "=" * 60)
    print("✅ Verificação concluída!")
    print("=" * 60)


if __name__ == "__main__":
    main()
