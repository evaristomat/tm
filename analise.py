import sqlite3

import pandas as pd

conn = sqlite3.connect("bets.db")
df = pd.read_sql_query("SELECT * FROM bets", conn)
conn.close()

df = df[df["selection"].str.contains("Under", na=False)]
df = df[~df["league_name"].str.contains("TT Cup", na=False)]


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


resultados = []

print("=" * 130)
print("BASELINE (Sem TT Cup)")
print("=" * 130)
base = analisar(df, "Todas Under")
print(
    f"{base['filtro']:50} | Vol: {base['volume']:4} | WR: {base['win_rate']:5.2f}% | Lucro: {base['lucro']:7.2f} | ROI: {base['roi']:6.2f}%"
)
print()

print("=" * 130)
print("FILTROS INDIVIDUAIS - HANDICAP")
print("=" * 130)
for h in [75.5, 76.5, 77.5, 78.5]:
    r = analisar(df[df["handicap"] >= h], f"Handicap >= {h}")
    if r:
        print(
            f"{r['filtro']:50} | Vol: {r['volume']:4} | WR: {r['win_rate']:5.2f}% | Lucro: {r['lucro']:7.2f} | ROI: {r['roi']:6.2f}%"
        )
        resultados.append(r)
print()

print("=" * 130)
print("FILTROS INDIVIDUAIS - ESTIMATED ROI")
print("=" * 130)
for roi_min in [25, 30, 35, 40, 45, 50]:
    r = analisar(df[df["estimated_roi"] >= roi_min], f"ROI >= {roi_min}%")
    if r:
        print(
            f"{r['filtro']:50} | Vol: {r['volume']:4} | WR: {r['win_rate']:5.2f}% | Lucro: {r['lucro']:7.2f} | ROI: {r['roi']:6.2f}%"
        )
        resultados.append(r)
print()

print("=" * 130)
print("FILTROS INDIVIDUAIS - BET EDGE")
print("=" * 130)
for edge in [0.15, 0.20, 0.25, 0.30, 0.35]:
    r = analisar(df[df["bet_edge"] >= edge], f"Edge >= {edge}")
    if r:
        print(
            f"{r['filtro']:50} | Vol: {r['volume']:4} | WR: {r['win_rate']:5.2f}% | Lucro: {r['lucro']:7.2f} | ROI: {r['roi']:6.2f}%"
        )
        resultados.append(r)
print()

print("=" * 130)
print("COMBINAÇÕES (mínimo 50 apostas)")
print("=" * 130)

combinacoes = []
for handicap in [76.5, 77.5, 78.5]:
    for roi_min in [30, 35, 40, 45, 50]:
        for edge_min in [0.15, 0.20, 0.25, 0.30]:
            df_filtrado = df[
                (df["handicap"] >= handicap)
                & (df["estimated_roi"] >= roi_min)
                & (df["bet_edge"] >= edge_min)
            ]

            nome = f"H>={handicap} + ROI>={roi_min}% + Edge>={edge_min}"
            r = analisar(df_filtrado, nome)
            if r and r["volume"] >= 50:
                combinacoes.append(r)

df_comb = pd.DataFrame(combinacoes).sort_values("roi", ascending=False)

for _, row in df_comb.head(15).iterrows():
    print(
        f"{row['filtro']:50} | Vol: {row['volume']:4} | WR: {row['win_rate']:5.2f}% | Lucro: {row['lucro']:7.2f} | ROI: {row['roi']:6.2f}%"
    )
