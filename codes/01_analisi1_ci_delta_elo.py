import json
import pandas as pd
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt

input_activity = r"output\lichess_activity_matched.jsonl"
input_csv = r"analisi\output_analisi\analisi1_elo_clustering.csv"

PAD_ANNUAL_12  = True 

w_freq = 0.50
w_fast = 0.50
w_ultra  = 1.00
w_bullet = 0.60
w_blitz  = 0.30

def load_activity_jsonl(path):
    """
    Legge il JSONL di activity e crea un dataframe con colonne:
    user, year, month, ultrabullet, bullet, blitz, rapid
    """
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)
            for user, sessions in rec.items():
                for s in sessions:
                    start = datetime.utcfromtimestamp(s["interval"]["start"] / 1000.0)
                    games = s.get("games", {}) or {}

                    def ssum(tag):
                        d = games.get(tag, {}) or {}
                        return int(d.get("win", 0)) + int(d.get("loss", 0)) + int(d.get("draw", 0))

                    rows.append({
                        "user": user,
                        "year": start.year,
                        "month": start.month,
                        "ultrabullet": ssum("ultraBullet"),
                        "bullet":     ssum("bullet"),
                        "blitz":      ssum("blitz"),
                        "rapid":      ssum("rapid"),
                    })

    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=["user","year","month","ultrabullet","bullet","blitz","rapid"])
    return df

def compute_ci_tables(df):
    """
    Calcola tabelle annuale e trimestrale con:
    - frequency, fast_quality, peakness, CI
    - diagnostiche: games/day, P95_games_per_day_used, heavy_count, heavy_pct_legacy

    P95 globale (P95_games_day) per annuale e trimestrale.
    """
    df['tot_ultra_bullet_blitz_games'] = df['ultrabullet'] + df['bullet'] + df['blitz']
    max_ultra_bullet_blitz_games_year = df.groupby(["user","year"])['tot_ultra_bullet_blitz_games'].sum().max()

    annual_rows = []

    for (user, year), g in df.groupby(["user","year"], sort=False):
        U = int(g["ultrabullet"].sum()); B = int(g["bullet"].sum())
        Z = int(g["blitz"].sum());      R = int(g["rapid"].sum())
        T = U + B + Z + R

        # --- Annuale ---
        if T == 0:
            ultra_bullet_blitz_games_year = 0.0
            frequency = 0.0
            fast_quality = 0.0
        else:
            ultra_bullet_blitz_games_year = U + B + Z
            frequency = min(1.0, ultra_bullet_blitz_games_year / max_ultra_bullet_blitz_games_year) 
            fast_quality = (w_ultra*U + w_bullet*B + w_blitz*Z)/T

        CI = (w_freq * frequency) + (w_fast * fast_quality)

        annual_rows.append({
            "user": user,
            "year": int(year),
            "frequency": round(frequency, 4),
            "fast_quality": round(fast_quality, 4),
            "CI": round(CI, 4),
            "ultra_bullet_blitz_games_year": int(ultra_bullet_blitz_games_year),
            "max_ultra_bullet_blitz_games_year": int(max_ultra_bullet_blitz_games_year),  
            "ultrabullet": U, "bullet": B, "blitz": Z, "rapid": R, "total": T,
        })

    return pd.DataFrame(annual_rows) 

def agg_annual(group: pd.DataFrame) -> pd.Series:
    """Aggregazione annuale dei Δ Elo."""
    rating_level = group.iloc[0]['rating_level']
    start_rating = group.iloc[0]["start_rating"]
    end_rating   = group.iloc[-1]["end_rating"]
    delta_total  = end_rating - start_rating
    active_sum = int(group["month_active"].sum())
    return pd.Series({
        "rating_level": rating_level,
        "start_rating_year": start_rating,
        "end_rating_year": end_rating,
        "delta_elo_year": delta_total,
        "active_months_year": active_sum,
    })

df_activity = load_activity_jsonl(input_activity)
ci_ann = compute_ci_tables(df_activity)
df_elo  = pd.read_csv(input_csv)

ci_ann = ci_ann.loc[ci_ann["ultra_bullet_blitz_games_year"] > 0]

df_elo["year"] = df_elo["month"].str.slice(0, 4).astype(int)
elo_ann = (
        df_elo.groupby(["user_id","year"], as_index=False)
        .apply(agg_annual).reset_index(drop=True)
    )
elo_ann = elo_ann.loc[elo_ann["active_months_year"] > 0]

ann_inner = ci_ann.merge(elo_ann, left_on=["user","year"], right_on=["user_id","year"], how="inner")
ann_inner.to_csv(r"Analisi\output_analisi\analisi1_ci_delta_elo_join_inner.csv", index=False)



ann_inner["1-CI"] = 1 - ann_inner["CI"]
ann_inner["delta_elo_year"] = ann_inner["delta_elo_year"]/10
bins = [i/10 for i in range(11)]
ann_inner["bin"] = pd.cut(ann_inner["1-CI"], bins=bins, include_lowest=True)
levels = ["Beginner", "Intermedio", "Avanzato", "Esperto"]
fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharex=True, sharey=True)
axes = axes.flatten()
color_map = {
    "Beginner": "blue",
    "Intermedio": "orange",
    "Avanzato": "green",
    "Esperto": "red"
}

# Crea un plot per ogni livello
for ax, level in zip(axes, levels):
    subset = ann_inner[ann_inner["rating_level"] == level]
    
    # scatter
    ax.scatter(
        subset["1-CI"],
        subset["delta_elo_year"],
        color=color_map.get(level, "gray"),
        alpha=0.6,
        edgecolors="w",
        linewidth=0.5
    )
    
    # calcola il 75° percentile SOLO per questo livello
    p75_values = subset.groupby("bin")["delta_elo_year"].quantile(0.75)
    bin_centers = [interval.mid for interval in p75_values.index]
    
    ax.plot(
        bin_centers,
        p75_values.values,
        color="black",
        marker="o",
        linewidth=2,
        label="75° percentile"
    )
    
    # etichette e titolo
    ax.set_title(f"Rating Level: {level}")
    ax.set_ylim(-30, 30)
    # ticks e griglia a passo 0.1 su X
    ax.set_xticks(np.arange(0, 1.01, 0.1))
    ax.set_yticks(np.arange(-30, 31, 10))
    ax.grid(True, which="both", linestyle="--", alpha=0.6)
    ax.legend()

fig.text(0.5, 0.04, "1-CI", ha="center", fontsize=12)
fig.text(0.04, 0.5, "Delta Elo", va="center", rotation="vertical", fontsize=12)
fig.suptitle("Scatter plot per rating_level con 75° percentile calcolato per ciascun gruppo", 
             fontsize=14, fontweight="bold")
plt.tight_layout(rect=[0.03, 0.03, 1, 0.95])
plt.savefig(r"analisi\output_analisi\analisi1_ci_delta_elo_scatter_plot.png", dpi=300, bbox_inches="tight")
plt.close()