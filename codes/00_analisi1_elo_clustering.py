#struttura di output (Δ Elo mensili + percentili per fascia).

import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta
import numpy as np


# Configurazioni
input_file = r"output\fide_scraping_user.jsonl"
output_file = r"analisi\output_analisi\analisi1_elo_clustering.csv"      # output Δ mensili + quartili
CUTOFF_DATE = datetime(2024, 3, 1)                                         # data rivalutazione FIDE

# Fasce di rating
rating_levels = {
    "Beginner":   (0, 1399),
    "Intermedio": (1400, 1899),
    "Avanzato":   (1900, 2200),
    "Master":     (2201, 4000),
}

def get_rating_level(rating):
    for level, (low, high) in rating_levels.items():
        if low <= rating <= high:
            return level
    return "unknown"

def parse_period(period_str: str):
    try:
        return datetime.strptime(period_str, "%Y-%b")
    except Exception:
        return None

def month_key_from_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m")

def month_key(period_str: str):
    dt = parse_period(period_str)
    return month_key_from_dt(dt) if dt else None

def derivaluta_2024_standard(elo_post: int, date_obj: datetime) -> float:
    if date_obj >= CUTOFF_DATE and elo_post < 2000:
        elo_pre = (elo_post - 800.0) / 0.60
        elo_pre = max(0.0, round(elo_pre))
        return float(elo_pre)
    return float(elo_post)

def build_continuous_months(user_months_dict):
    if not user_months_dict:
        return []
    months_sorted = sorted(user_months_dict.keys())
    cur = months_sorted[0].replace(day=1)
    end = months_sorted[-1].replace(day=1)
    rows = []
    last_elo = None
    while cur <= end:
        if cur in user_months_dict:
            elo_corr, games = user_months_dict[cur]
            last_elo = elo_corr
            month_effective = 1
        else:
            elo_corr = last_elo if last_elo is not None else None
            games = 0
            month_effective = 0
        rows.append((month_key_from_dt(cur), elo_corr, games, month_effective))
        cur = cur + relativedelta(months=1)
    return rows

def classify_zero_delta(delta, games_played):
    if delta != 0:
        return ""
    return "zero_with_games" if games_played >= 1 else "zero_no_games"

all_rows = []

with open(input_file, "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        data = json.loads(line)
        for user_id, user_info in data.items():
            rating_history = user_info.get("FIDE_Profile", {}).get("RatingHistory", [])
            if not isinstance(rating_history, list):
                continue

            months_std = {}  # { datetime -> (elo_corrected, games_int) }
            for entry in rating_history:
                dt = parse_period(entry.get("Period", ""))
                if not dt:
                    continue
                std = entry.get("Standard", {})
                rating_raw = std.get("Rating", None)
                games_raw  = std.get("Games", None)
                if rating_raw is None:
                    continue
                try:
                    elo_post = int(str(rating_raw).strip())
                except Exception:
                    continue
                games = 0
                if games_raw is not None:
                    try:
                        games = int(str(games_raw).strip())
                    except Exception:
                        games = 0
                elo_corr = derivaluta_2024_standard(elo_post, dt)
                months_std[dt.replace(day=1)] = (elo_corr, games)

            if not months_std:
                continue

            timeline = build_continuous_months(months_std)

            prev_elo = None
            for mkey, elo_corr, games, month_effective in timeline:
                if prev_elo is not None and elo_corr is not None:
                    delta = int(elo_corr - prev_elo)
                    start_rating = int(prev_elo)
                    end_rating   = int(elo_corr)
                    rating_level = get_rating_level(start_rating)
                    month_active = 1 if games >= 1 else 0

                    all_rows.append({
                        "user_id": user_id,
                        "game_type": "Standard",
                        "month": mkey,
                        "start_rating": start_rating,
                        "end_rating": end_rating,
                        "delta_elo": delta,
                        "rating_level": rating_level,
                        "games_played": games,
                        "month_effective": month_effective,
                        "month_active": month_active,
                        "zero_delta_reason": classify_zero_delta(delta, games)
                    })
                prev_elo = elo_corr

df = pd.DataFrame(all_rows)

df["zero_label"] = np.where(
    (df["delta_elo"] == 0) & (df["games_played"] >= 1), "zero_with_games",
    np.where((df["delta_elo"] == 0) & (df["games_played"] == 0), "zero_no_games", "")
)

output_rows = []
if not df.empty:
    for (rating_level, game_type), subset in df.groupby(["rating_level", "game_type"]):
        # === MOD: calcolo quartili SOLO sui mesi ATTIVI (coerente col confronto sul campione) ===
        subset_active = subset[subset["month_active"] == 1]

        if subset_active.empty:
            # === MOD: nessun mese attivo nella fascia -> non assegniamo quartili ===
            for _, row in subset.iterrows():
                output_rows.append({
                    **row.to_dict(),
                    "delta_percentile": "",
                    "quartile_min": np.nan,
                    "quartile_max": np.nan,
                    "quartile_note": ""  # nessuna nota specifica (gruppo senza attivi)
                })
            continue

        # === MOD: boundaries TEORICI fissi per gruppo (fascia × game_type), calcolati sui soli ATTIVI ===
        q25, q50, q75 = subset_active["delta_elo"].quantile([0.25, 0.5, 0.75])
        min_delta = subset_active["delta_elo"].min()
        max_delta = subset_active["delta_elo"].max()

        # === MOD: assegnazione quartile con RANGE FISSI per ogni riga ATTIVA;
        #          per le righe INATTIVE, lasciamo i quartili vuoti e annotiamo la nota ===
        for _, row in subset.iterrows():
            if row["month_active"] == 0:
                # mese senza partite -> non confrontabile col campione attivo
                output_rows.append({
                    **row.to_dict(),
                    "delta_percentile": "",
                    "quartile_min": np.nan,
                    "quartile_max": np.nan,
                    "quartile_note": "inactive_month"  # === MOD
                })
                continue

            delta = row["delta_elo"]
            if delta <= q25:
                pct_label = "0-25";    qmin, qmax = float(min_delta), float(q25)
            elif delta <= q50:
                pct_label = "25-50";   qmin, qmax = float(q25),     float(q50)
            elif delta <= q75:
                pct_label = "50-75";   qmin, qmax = float(q50),     float(q75)
            else:
                pct_label = "75-100";  qmin, qmax = float(q75),     float(max_delta)

            output_rows.append({
                **row.to_dict(),
                "delta_percentile": pct_label,   # NB: lasciato il nome per retro-compatibilità
                "quartile_min": qmin,            # === MOD: boundaries FISSI (teorici)
                "quartile_max": qmax,            # === MOD: boundaries FISSI (teorici)
                "quartile_note": "active_month"  # === MOD
            })

output_df = pd.DataFrame(output_rows)
Path(output_file).parent.mkdir(parents=True, exist_ok=True)
output_df.to_csv(output_file, index=False)
print(f"CSV creato: {output_file}  | righe: {len(output_df)}")
