import sqlite3

import pandas as pd

conn = sqlite3.connect("bets.db")
df = pd.read_sql_query("SELECT * FROM bets", conn)
conn.close()

# Filtrar apenas apostas com selection contendo "Under"
df = df[df["selection"].str.contains("Under", na=False)]

# Excluir liga "TT Cup"
df = df[~df["league_name"].str.contains("TT Cup", na=False)]

# Filtrar para as ligas Czech Liga Pro e Setka Cup
df = df[df["league_name"].isin(["Czech Liga Pro", "Setka Cup"])]

# Excluir jogos com handicap menor ou igual a 75.5
df = df[df["handicap"] > 75.5]


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


print("=" * 130)
print("BASELINE Comparações (Under Czech e Setka com filtros de handicap)")
print("=" * 130)
bases = [
    ("Todas Under com H <= 78.5", df[df["handicap"] <= 78.5]),
    ("Todas Under com H <= 77.5", df[df["handicap"] <= 77.5]),
    ("Todas Under com H <= 76.5", df[df["handicap"] <= 76.5]),
    ("Todas Under com H == 76.5", df[df["handicap"] == 76.5]),
    ("Todas Under com H == 77.5", df[df["handicap"] == 77.5]),
    ("Todas Under com H == 78.5", df[df["handicap"] == 78.5]),
]
for nome, d in bases:
    r = analisar(d, nome)
    if r:
        print(
            f"{r['filtro']:40} | Vol: {r['volume']:4} | WR: {r['win_rate']:5.2f}% | Lucro: {r['lucro']:8.2f} | ROI: {r['roi']:7.2f}%"
        )
print()

print("=" * 130)
print("COMBINAÇÕES (mínimo 50 apostas) - Handicap e ROI")
print("=" * 130)
combinacoes = []
for handicap in [76.5, 77.5, 78.5]:
    for roi_min in [30, 35, 40, 45, 50]:
        df_filtrado = df[
            (df["handicap"] >= handicap) & (df["estimated_roi"] >= roi_min)
        ]
        nome = f"H>={handicap} + ROI>={roi_min}%"
        r = analisar(df_filtrado, nome)
        if r and r["volume"] >= 50:
            combinacoes.append(r)

df_comb = pd.DataFrame(combinacoes).sort_values("roi", ascending=False)
for _, row in df_comb.head(15).iterrows():
    print(
        f"{row['filtro']:40} | Vol: {row['volume']:4} | WR: {row['win_rate']:5.2f}% | Lucro: {row['lucro']:8.2f} | ROI: {row['roi']:7.2f}%"
    )

print("\n" + "=" * 130)
print("ANÁLISE EXTRA: ROI para handicap == 77.5")
print("=" * 130)

roi_vals = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
for roi_min in roi_vals:
    df_roi = df[(df["handicap"] == 77.5) & (df["estimated_roi"] >= roi_min)]
    r = analisar(df_roi, f"H == 77.5 + ROI>={roi_min}%")
    if r:
        print(
            f"{r['filtro']:30} | Vol: {r['volume']:4} | WR: {r['win_rate']:5.2f}% | Lucro: {r['lucro']:8.2f} | ROI: {r['roi']:7.2f}%"
        )

print("\n" + "=" * 130)
print(
    "ANÁLISE EXTRA: ROI independente do handicap (todas as linhas 76.5, 77.5 e 78.5 juntas)"
)
print("\n" + "=" * 130)
print("ANÁLISE EXTRA: ROI por linha de handicap (76.5, 77.5 e 78.5 individualmente)")
print("=" * 130)

roi_vals = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
handicap_lines = [76.5, 77.5, 78.5]

for handicap in handicap_lines:
    print(f"\nHandicap == {handicap}")
    for roi_min in roi_vals:
        df_filtrado = df[
            (df["handicap"] == handicap) & (df["estimated_roi"] >= roi_min)
        ]
        r = analisar(df_filtrado, f"H == {handicap} + ROI>={roi_min}%")
        if r:
            print(
                f"{r['filtro']:30} | Vol: {r['volume']:4} | WR: {r['win_rate']:5.2f}% | Lucro: {r['lucro']:8.2f} | ROI: {r['roi']:7.2f}%"
            )
