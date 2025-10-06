import pandas as pd
import sqlite3

def apply_filters_v2(df):
    """Aplica os filtros V2 (V1 + novo filtro Czech ML ROI 100%+)"""
    df = df.copy()
    df["exclude"] = False

    # Filtro 1: Czech ML - Odds e ROI
    czech_ml_mask = (df["league_name"].str.contains("Czech", case=False, na=False)) & (df["bet_type"] == "To Win")
    df.loc[czech_ml_mask, "exclude"] = (df.loc[czech_ml_mask, "odds"] >= 3.5) | (df.loc[czech_ml_mask, "odds"] < 1.5)
    df.loc[czech_ml_mask, "exclude"] = df.loc[czech_ml_mask, "exclude"] | (df.loc[czech_ml_mask, "estimated_roi"] < 20) | (df.loc[czech_ml_mask, "estimated_roi"] >= 100)  # Mudança: 150 -> 100

    # Filtro 2: Czech O/U (apenas Under)
    czech_ou_mask = (df["league_name"].str.contains("Czech", case=False, na=False)) & (df["bet_type"] == "Total")
    df.loc[czech_ou_mask, "exclude"] = ~df.loc[czech_ou_mask, "selection"].str.contains("Under", case=False, na=False)

    # Filtro 3: TT Elite ML (não apostar)
    tt_ml_mask = (df["league_name"].str.contains("TT Elite", case=False, na=False)) & (df["bet_type"] == "To Win")
    df.loc[tt_ml_mask, "exclude"] = True

    # Filtro 4: TT Elite O/U (apenas Under)
    tt_ou_mask = (df["league_name"].str.contains("TT Elite", case=False, na=False)) & (df["bet_type"] == "Total")
    df.loc[tt_ou_mask, "exclude"] = ~df.loc[tt_ou_mask, "selection"].str.contains("Under", case=False, na=False)

    df_filtered = df[df["exclude"] != True].copy()
    df_filtered.drop(columns=["exclude"], inplace=True)
    return df_filtered

def calculate_league_stats(df, league_name):
    """Calcula estatísticas para uma liga específica"""
    league_df = df[df["league_name"].str.contains(league_name, case=False, na=False)]
    
    if league_df.empty:
        return None
    
    # Estatísticas gerais
    total_profit = league_df["profit"].sum()
    total_volume = len(league_df)
    wins = league_df["result"].sum()
    losses = total_volume - wins
    roi = (total_profit / total_volume) * 100 if total_volume > 0 else 0
    
    # Estatísticas por mercado
    ml_df = league_df[league_df["bet_type"] == "To Win"]
    ou_df = league_df[league_df["bet_type"] == "Total"]
    
    ml_profit = ml_df["profit"].sum() if not ml_df.empty else 0
    ml_volume = len(ml_df)
    
    ou_profit = ou_df["profit"].sum() if not ou_df.empty else 0
    ou_volume = len(ou_df)
    
    return {
        "league": league_name,
        "total_profit": total_profit,
        "roi": roi,
        "wins": wins,
        "losses": losses,
        "ml_profit": ml_profit,
        "ml_volume": ml_volume,
        "ou_profit": ou_profit,
        "ou_volume": ou_volume
    }

def format_profit(profit):
    """Formata o lucro com sinal + ou -"""
    if profit >= 0:
        return f"+{profit:.2f}u"
    else:
        return f"{profit:.2f}u"

def format_roi(roi):
    """Formata o ROI com sinal + ou -"""
    if roi >= 0:
        return f"+{roi:.1f}%"
    else:
        return f"{roi:.1f}%"

def generate_summary(df, title):
    """Gera o resumo no formato solicitado"""
    leagues = ["Czech Liga Pro", "Challenger Series TT", "TT Cup", "TT Elite Series"]
    
    print(f"\n💰 {title}")
    print("━" * 30)
    
    total_profit = 0
    total_volume = 0
    
    for league in leagues:
        stats = calculate_league_stats(df, league)
        if stats is None:
            continue
            
        total_profit += stats["total_profit"]
        total_volume += stats["ml_volume"] + stats["ou_volume"]
        
        # Emoji baseado no lucro
        emoji = "✅" if stats["total_profit"] >= 0 else "❌"
        
        print(f"\n🏓 {stats['league']}")
        print(f"{emoji} {format_profit(stats['total_profit'])} | ROI: {format_roi(stats['roi'])} | {stats['wins']}W-{stats['losses']}L")
        
        if stats["ml_volume"] > 0:
            print(f"├ ML: {format_profit(stats['ml_profit'])} ({stats['ml_volume']} apostas)")
        if stats["ou_volume"] > 0:
            print(f"└ O/U: {format_profit(stats['ou_profit'])} ({stats['ou_volume']} apostas)")
    
    print("\n" + "━" * 30)
    print("📊 TOTAL GERAL")
    total_roi = (total_profit / total_volume) * 100 if total_volume > 0 else 0
    emoji = "✅" if total_profit >= 0 else "❌"
    print(f"{emoji} {format_profit(total_profit)} | ROI: {format_roi(total_roi)}")

def main():
    # Conectar ao banco de dados
    conn = sqlite3.connect("bets.db")
    df_original = pd.read_sql_query("SELECT * FROM bets WHERE result IS NOT NULL AND profit IS NOT NULL", conn)
    conn.close()
    
    # Aplicar filtros V2
    df_filtered = apply_filters_v2(df_original)
    
    # Gerar resumos
    generate_summary(df_original, "RESUMO ORIGINAL (SEM FILTROS)")
    generate_summary(df_filtered, "RESUMO FILTRADO (V2)")
    
    # Estatísticas de comparação
    print(f"\n📈 COMPARAÇÃO")
    print("━" * 30)
    
    original_profit = df_original["profit"].sum()
    filtered_profit = df_filtered["profit"].sum()
    original_volume = len(df_original)
    filtered_volume = len(df_filtered)
    
    profit_diff = filtered_profit - original_profit
    volume_diff = filtered_volume - original_volume
    
    original_roi = (original_profit / original_volume) * 100 if original_volume > 0 else 0
    filtered_roi = (filtered_profit / filtered_volume) * 100 if filtered_volume > 0 else 0
    roi_diff = filtered_roi - original_roi
    
    print(f"Lucro: {format_profit(original_profit)} → {format_profit(filtered_profit)} ({format_profit(profit_diff)})")
    print(f"Volume: {original_volume} → {filtered_volume} ({volume_diff:+d} apostas)")
    print(f"ROI: {format_roi(original_roi)} → {format_roi(filtered_roi)} ({format_roi(roi_diff)})")
    
    # Filtros aplicados
    print(f"\n🔧 FILTROS APLICADOS (V2)")
    print("━" * 30)
    print("Czech Liga Pro ML:")
    print("  ❌ Odds >= 3.5 ou < 1.5")
    print("  ❌ ROI estimado < 20% ou >= 100%")  # Mudança aqui
    print("Czech Liga Pro O/U:")
    print("  ✅ Apenas apostas Under")
    print("TT Elite Series ML:")
    print("  ❌ Todas as apostas ML")
    print("TT Elite Series O/U:")
    print("  ✅ Apenas apostas Under")

if __name__ == "__main__":
    main()
