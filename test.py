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
print("ANÁLISE FOCADA - ESTRATÉGIA PROPOSTA")
print("=" * 130)
print()

# CONFIGURAÇÃO 1: Setka Cup apenas H == 76.5
print("CONFIGURAÇÃO 1: Setka Cup H == 76.5")
print("-" * 130)
df_setka_76 = df[(df["league_name"] == "Setka Cup") & (df["handicap"] == 76.5)]
r = analisar(df_setka_76, "Setka Cup H == 76.5")
if r:
    print(
        f"{r['filtro']:40} | Vol: {r['volume']:4} | WR: {r['win_rate']:5.2f}% | Lucro: {r['lucro']:8.2f} | ROI: {r['roi']:7.2f}%"
    )
print()

# CONFIGURAÇÃO 2: Czech Liga Pro H == 76.5 e H == 78.5
print("CONFIGURAÇÃO 2: Czech Liga Pro H == 76.5 e H == 78.5")
print("-" * 130)
df_czech_76 = df[(df["league_name"] == "Czech Liga Pro") & (df["handicap"] == 76.5)]
r = analisar(df_czech_76, "Czech H == 76.5")
if r:
    print(
        f"{r['filtro']:40} | Vol: {r['volume']:4} | WR: {r['win_rate']:5.2f}% | Lucro: {r['lucro']:8.2f} | ROI: {r['roi']:7.2f}%"
    )

df_czech_78 = df[(df["league_name"] == "Czech Liga Pro") & (df["handicap"] == 78.5)]
r = analisar(df_czech_78, "Czech H == 78.5")
if r:
    print(
        f"{r['filtro']:40} | Vol: {r['volume']:4} | WR: {r['win_rate']:5.2f}% | Lucro: {r['lucro']:8.2f} | ROI: {r['roi']:7.2f}%"
    )

df_czech_combined = df[
    (df["league_name"] == "Czech Liga Pro") & (df["handicap"].isin([76.5, 78.5]))
]
r = analisar(df_czech_combined, "Czech H 76.5 + 78.5 (combinado)")
if r:
    print(
        f"{r['filtro']:40} | Vol: {r['volume']:4} | WR: {r['win_rate']:5.2f}% | Lucro: {r['lucro']:8.2f} | ROI: {r['roi']:7.2f}%"
    )
print()

# RESULTADO COMBINADO DA ESTRATÉGIA PROPOSTA
print("=" * 130)
print("RESULTADO TOTAL DA ESTRATÉGIA PROPOSTA")
print("=" * 130)
df_estrategia = pd.concat(
    [
        df[(df["league_name"] == "Setka Cup") & (df["handicap"] == 76.5)],
        df[
            (df["league_name"] == "Czech Liga Pro")
            & (df["handicap"].isin([76.5, 78.5]))
        ],
    ]
)
r = analisar(df_estrategia, "ESTRATÉGIA COMPLETA")
if r:
    print(
        f"{r['filtro']:40} | Vol: {r['volume']:4} | WR: {r['win_rate']:5.2f}% | Lucro: {r['lucro']:8.2f} | ROI: {r['roi']:7.2f}%"
    )
print()

# COMPARAÇÃO COM ESTRATÉGIA ANTERIOR (todas as linhas ≥76.5)
print("=" * 130)
print("COMPARAÇÃO COM ESTRATÉGIA ANTERIOR")
print("=" * 130)
df_anterior = df[df["handicap"] >= 76.5]
r = analisar(df_anterior, "Estratégia Anterior (H ≥ 76.5)")
if r:
    print(
        f"{r['filtro']:40} | Vol: {r['volume']:4} | WR: {r['win_rate']:5.2f}% | Lucro: {r['lucro']:8.2f} | ROI: {r['roi']:7.2f}%"
    )
print()

# ANÁLISE DETALHADA POR LIGA COM FILTROS DE ROI
print("=" * 130)
print("ANÁLISE COM FILTROS DE ROI - SETKA CUP H == 76.5")
print("=" * 130)
for roi_min in [0, 10, 20, 30, 40, 50]:
    df_filtrado = df[
        (df["league_name"] == "Setka Cup")
        & (df["handicap"] == 76.5)
        & (df["estimated_roi"] >= roi_min)
    ]
    r = analisar(df_filtrado, f"Setka H76.5 ROI>={roi_min}%")
    if r:
        print(
            f"{r['filtro']:40} | Vol: {r['volume']:4} | WR: {r['win_rate']:5.2f}% | Lucro: {r['lucro']:8.2f} | ROI: {r['roi']:7.2f}%"
        )
print()

print("=" * 130)
print("ANÁLISE COM FILTROS DE ROI - CZECH LIGA PRO H == 76.5")
print("=" * 130)
for roi_min in [0, 10, 20, 30, 40, 50]:
    df_filtrado = df[
        (df["league_name"] == "Czech Liga Pro")
        & (df["handicap"] == 76.5)
        & (df["estimated_roi"] >= roi_min)
    ]
    r = analisar(df_filtrado, f"Czech H76.5 ROI>={roi_min}%")
    if r:
        print(
            f"{r['filtro']:40} | Vol: {r['volume']:4} | WR: {r['win_rate']:5.2f}% | Lucro: {r['lucro']:8.2f} | ROI: {r['roi']:7.2f}%"
        )
print()

print("=" * 130)
print("ANÁLISE COM FILTROS DE ROI - CZECH LIGA PRO H == 78.5")
print("=" * 130)
for roi_min in [0, 10, 20, 30, 40, 50]:
    df_filtrado = df[
        (df["league_name"] == "Czech Liga Pro")
        & (df["handicap"] == 78.5)
        & (df["estimated_roi"] >= roi_min)
    ]
    r = analisar(df_filtrado, f"Czech H78.5 ROI>={roi_min}%")
    if r:
        print(
            f"{r['filtro']:40} | Vol: {r['volume']:4} | WR: {r['win_rate']:5.2f}% | Lucro: {r['lucro']:8.2f} | ROI: {r['roi']:7.2f}%"
        )
print()

# ANÁLISE FINAL: POTENCIAL MELHORIA COM FILTROS COMBINADOS
print("=" * 130)
print("POTENCIAL DE OTIMIZAÇÃO - FILTROS DE ROI COMBINADOS")
print("=" * 130)

# Testando combinações de filtros de ROI para cada componente
combinacoes_otimizadas = []

# Setka 76.5 com diferentes ROIs
for setka_roi in [0, 15, 20, 25, 30]:
    # Czech 76.5 com diferentes ROIs
    for czech_76_roi in [0, 10, 15, 20, 25]:
        # Czech 78.5 com diferentes ROIs (geralmente sem filtro pois já é boa)
        for czech_78_roi in [0, 10, 20]:
            df_estrategia_otimizada = pd.concat(
                [
                    df[
                        (df["league_name"] == "Setka Cup")
                        & (df["handicap"] == 76.5)
                        & (df["estimated_roi"] >= setka_roi)
                    ],
                    df[
                        (df["league_name"] == "Czech Liga Pro")
                        & (df["handicap"] == 76.5)
                        & (df["estimated_roi"] >= czech_76_roi)
                    ],
                    df[
                        (df["league_name"] == "Czech Liga Pro")
                        & (df["handicap"] == 78.5)
                        & (df["estimated_roi"] >= czech_78_roi)
                    ],
                ]
            )
            r = analisar(
                df_estrategia_otimizada,
                f"Setka76.5(ROI>={setka_roi}) + Czech76.5(ROI>={czech_76_roi}) + Czech78.5(ROI>={czech_78_roi})",
            )
            if r and r["volume"] >= 200:  # Mínimo de volume para ter significância
                combinacoes_otimizadas.append(r)

# Ordenar por ROI
df_otimizacoes = pd.DataFrame(combinacoes_otimizadas).sort_values(
    "roi", ascending=False
)
print("TOP 15 COMBINAÇÕES DE FILTROS (mínimo 200 apostas):")
print("-" * 130)
for _, row in df_otimizacoes.head(15).iterrows():
    print(
        f"{row['filtro']:80} | Vol: {row['volume']:4} | WR: {row['win_rate']:5.2f}% | Lucro: {row['lucro']:8.2f} | ROI: {row['roi']:7.2f}%"
    )
