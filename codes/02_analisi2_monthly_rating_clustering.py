import json
import pandas as pd
from datetime import datetime
from pathlib import Path
import os

# Configurazioni
input_file = "lichess_users.jsonl"
output_dir = r"..\csvs"
output_file = os.path.join(output_dir, "monthly_delta_rating_percentiles.csv")

# Fasce di rating di partenza
rating_levels = {
    "too_low": (0, 1400),
    "beginner": (1401, 1600),
    "intermediate": (1601, 1800),
    "advanced": (1801, 2200),
    "expert": (2201, 2500),
    "super_expert": (2501, 4000)
}

def get_rating_level(rating):
    for level, (low, high) in rating_levels.items():
        if low <= rating <= high:
            return level
    return "unknown"

def parse_date_shifted(date_str: str):
    """
    Converte stringhe tipo '1-0-2023' in date reali.
    Aggiunge +1 al mese (0 → gennaio, 1 → febbraio, ..., 11 → dicembre).
    Se il giorno non esiste (es. 31/1/2023 → 31 febbraio),
    abbassa il giorno fino a trovare una data valida.
    """
    parts = date_str.split("-")
    if len(parts) != 3:
        return None

    day, month, year = map(int, parts)
    month += 1  # correzione da base 0 a base 1

    if month > 12:
        return None  # mese non valido

    while True:
        try:
            return datetime(year, month, day)
        except ValueError:
            day -= 1
            if day <= 0:
                return None

def month_key(date_str):
    dt = parse_date_shifted(date_str)
    return dt.strftime("%Y-%m") if dt else None

# Lista per accumulare tutte le righe
all_rows = []

# Leggi il JSONL
with open(input_file, "r", encoding="utf-8") as f:
    for line in f:
        data = json.loads(line)
        for user_id, games in data.items():
            for game_type, game_data in games.items():
                rating_history = game_data.get("rating_history", {})
                # Ordina le date interpretate
                sorted_dates = sorted(
                    rating_history.keys(),
                    key=lambda x: parse_date_shifted(x)
                )
                monthly = {}
                for date_str in sorted_dates:
                    dt = parse_date_shifted(date_str)
                    if not dt or dt.year < 2023:  # <<< filtro sui mesi dal 2023
                        continue
                    month = dt.strftime("%Y-%m")
                    rating = rating_history[date_str]
                    monthly[month] = rating  # prende l’ultimo rating del mese

                # Calcola delta mensile
                prev_rating = None
                for month, rating in sorted(monthly.items()):
                    if prev_rating is not None:
                        delta = rating - prev_rating
                        start_rating = prev_rating
                        rating_level = get_rating_level(start_rating)
                        all_rows.append({
                            "user_id": user_id,
                            "game_type": game_type,
                            "month": month,
                            "start_rating": start_rating,
                            "end_rating": rating,
                            "delta_rating": delta,
                            "rating_level": rating_level
                        })
                    prev_rating = rating

# Crea DataFrame
df = pd.DataFrame(all_rows)

# Calcolo dei percentili per ciascun rating_level + game_type
output_rows = []

for (rating_level, game_type), subset in df.groupby(["rating_level", "game_type"]):
    q25, q50, q75 = subset["delta_rating"].quantile([0.25, 0.5, 0.75])
    min_delta = subset["delta_rating"].min()
    max_delta = subset["delta_rating"].max()
    
    for idx, row in subset.iterrows():
        delta = row["delta_rating"]
        if delta <= q25:
            pct_label = "0-25"
            quartile_min = min_delta
            quartile_max = q25
        elif delta <= q50:
            pct_label = "25-50"
            quartile_min = q25
            quartile_max = q50
        elif delta <= q75:
            pct_label = "50-75"
            quartile_min = q50
            quartile_max = q75
        else:
            pct_label = "75-100"
            quartile_min = q75
            quartile_max = max_delta

        output_rows.append({
            **row,
            "delta_percentile": pct_label,
            "quartile_min": quartile_min,
            "quartile_max": quartile_max
        })

# Scrivi CSV finale
output_df = pd.DataFrame(output_rows)
output_df.to_csv(output_file, index=False)

print(f"CSV creato: {output_file}")
