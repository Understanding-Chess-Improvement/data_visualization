import json
import pandas as pd
from datetime import datetime
from pathlib import Path
import os

# Configurazioni
input_file = r"output\lichess_users.jsonl"
#output_dir = r"..\csvs"
#os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, r"analisi\output_analisi\analisi2_global_rating_clustering.csv")

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
    """Converte date con mese base-0 in datetime valido."""
    parts = date_str.split("-")
    if len(parts) != 3:
        return None
    day, month, year = map(int, parts)
    month += 1
    if month > 12:
        return None
    while True:
        try:
            return datetime(year, month, day)
        except ValueError:
            day -= 1
            if day <= 0:
                return None

# Lista per accumulare tutte le righe
all_rows = []

# Leggi il JSONL
with open(input_file, "r", encoding="utf-8") as f:
    for line in f:
        data = json.loads(line)
        for user_id, games in data.items():
            n_puzzles = games['puzzle'].get('games')
            for game_type, game_data in games.items():

                rating_history = game_data.get("rating_history", {})
                if not rating_history:
                    continue

                # Filtra solo date dal 2023 in poi
                filtered_dates = [
                    d for d in rating_history.keys()
                    if parse_date_shifted(d) and parse_date_shifted(d).year >= 2023
                ]
                if not filtered_dates:
                    continue  # se non ci sono date nel 2023+, salta

                # Ordina le date per trovare primo e ultimo rating
                sorted_dates = sorted(
                    filtered_dates,
                    key=lambda x: parse_date_shifted(x)
                )

                first_date = sorted_dates[0]
                last_date = sorted_dates[-1]

                first_rating = rating_history[first_date]
                last_rating = rating_history[last_date]
                delta_rating = last_rating - first_rating
                rating_level = get_rating_level(first_rating)

                all_rows.append({
                    "user_id": user_id,
                    "n_puzzles": n_puzzles,
                    "game_type": game_type,
                    "first_date": parse_date_shifted(first_date),
                    "last_date": parse_date_shifted(last_date),
                    "start_rating": first_rating,
                    "end_rating": last_rating,
                    "delta_rating": delta_rating,
                    "rating_level": rating_level
                })

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
            quartile_min, quartile_max = min_delta, q25
        elif delta <= q50:
            pct_label = "25-50"
            quartile_min, quartile_max = q25, q50
        elif delta <= q75:
            pct_label = "50-75"
            quartile_min, quartile_max = q50, q75
        else:
            pct_label = "75-100"
            quartile_min, quartile_max = q75, max_delta

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
