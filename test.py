import sqlite3

import pandas as pd

conn = sqlite3.connect("bets.db")
df = pd.read_sql_query("SELECT * FROM bets", conn)
conn.close()

# Filtrar apostas Under para ligas Setka Cup e Czech Liga Pro
df_under = df[
    (df["selection"].str.contains("Under", na=False))
    & (df["league_name"].isin(["Setka Cup", "Czech Liga Pro"]))
]


def analisar(df_filtrado, nome):
    if len(df_filtrado) == 0:
        return None
    win_rate = (df_filtrado["result"] == 1).sum() / len(df_filtrado) * 100
    lucro = df_filtrado["profit"].sum()
    roi = (lucro / len(df_filtrado)) * 100
    return {
        "filtro": nome,
        "volume": len(df_filtrado),
        "win_rate": win_rate,
        "lucro": lucro,
        "roi": roi,
    }


# Resultado geral
print("=" * 130)
print("RESULTADO GERAL - Under Setka e Czech")
print("=" * 130)
geral = analisar(df_under, "Geral Under Setka e Czech")
if geral:
    print(
        f"{geral['filtro']:30} | Vol: {geral['volume']:4} | WR: {geral['win_rate']:5.2f}% | Lucro: {geral['lucro']:8.2f} | ROI: {geral['roi']:7.2f}%"
    )
print()

# Aplicando a estratégia de filtros de ROI para cada handicap
print("=" * 130)
print("RESULTADO COM ESTRATÉGIA DE FILTRAGEM POR HANDICAP E ROI")
print("=" * 130)

roi_filtros = {
    76.5: 20,
    77.5: 40,
    78.5: 0,  # sem filtro de ROI para 78.5
}

dfs_filtrados = []
for handicap, roi_min in roi_filtros.items():
    df_filt = df_under[df_under["handicap"] == handicap]
    if roi_min > 0:
        df_filt = df_filt[df_filt["estimated_roi"] >= roi_min]
    dfs_filtrados.append(df_filt)

df_estrategia = pd.concat(dfs_filtrados)

estrategia = analisar(df_estrategia, "Estratégia aplicada")
if estrategia:
    print(
        f"{estrategia['filtro']:30} | Vol: {estrategia['volume']:4} | WR: {estrategia['win_rate']:5.2f}% | Lucro: {estrategia['lucro']:8.2f} | ROI: {estrategia['roi']:7.2f}%"
    )
print()

# Filtro apenas handicap >= 78.5
print("=" * 130)
print("RESULTADO FILTRO APENAS HANDICAP >= 78.5")
print("=" * 130)

df_handicap78 = df_under[df_under["handicap"] >= 78.5]
handicap78 = analisar(df_handicap78, "Handicap >= 78.5")
if handicap78:
    print(
        f"{handicap78['filtro']:30} | Vol: {handicap78['volume']:4} | WR: {handicap78['win_rate']:5.2f}% | Lucro: {handicap78['lucro']:8.2f} | ROI: {handicap78['roi']:7.2f}%"
    )
