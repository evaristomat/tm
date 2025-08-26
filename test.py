import sqlite3
import pandas as pd


def check_database_structure(db_path, db_name):
    """Verifica a estrutura de um banco de dados SQLite"""
    print(f"\n🔍 VERIFICANDO ESTRUTURA DO BANCO: {db_name}")
    print("=" * 60)

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Listar todas as tabelas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        print(f"📊 Tabelas encontradas em {db_name}:")
        for table in tables:
            table_name = table[0]
            print(f"\n📋 Tabela: {table_name}")

            # Obter informações das colunas
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()

            print("   Colunas:")
            for col in columns:
                print(f"     - {col[1]} ({col[2]})")

            # Mostrar algumas linhas de exemplo
            try:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 1;")
                sample = cursor.fetchone()
                if sample:
                    print("   📝 Exemplo de linha:")
                    cursor.execute(f"SELECT * FROM {table_name} LIMIT 1;")
                    col_names = [description[0] for description in cursor.description]
                    sample_data = dict(zip(col_names, sample))
                    for key, value in sample_data.items():
                        print(f"     {key}: {value}")
            except:
                print("   ⚠️  Não foi possível obter exemplo de linha")

        conn.close()

    except Exception as e:
        print(f"❌ Erro ao verificar o banco {db_name}: {e}")


def main():
    # Verificar estrutura do tm_data.db
    check_database_structure("tm_data.db", "tm_data.db")

    # Verificar estrutura do table_tennis_results.db
    check_database_structure("table_tennis_results.db", "table_tennis_results.db")

    print("\n" + "=" * 60)
    print("RECOMENDAÇÕES PARA O SCRIPT DE ANÁLISE")
    print("=" * 60)

    # Baseado na estrutura, vamos criar um script adequado
    print("""
Com base na estrutura dos bancos, vamos:

1. Usar a tabela 'events' do tm_data.db para obter jogos recentes
2. Usar a tabela 'events' do table_tennis_results.db para estatísticas
3. Ajustar as consultas para usar apenas colunas disponíveis

Vamos criar um script que:
- Busca um jogo do tm_data.db
- Encontra os últimos 10 jogos de cada jogador no table_tennis_results.db
- Calcula estatísticas baseadas no placar (coluna 'score')
""")


if __name__ == "__main__":
    main()
