import pandas as pd
import json
from datetime import datetime

def load_activity_totals(jsonl_path):
    """
    Legge il jsonl delle activity e restituisce:
    - df_global: [user_id, tot_games]
    - df_monthly: [user_id, month, tot_games]
    """
    global_totals = {}
    monthly_records = []

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            for user_id, activities in obj.items():
                tot_user = 0
                for act in activities:
                    # mese dal campo interval.start
                    start_ts = act["interval"]["start"]
                    dt = datetime.utcfromtimestamp(start_ts / 1000)
                    month = dt.strftime("%Y-%m")

                    games = act.get("games", {})
                    tot_act = 0
                    for gtype, gstats in games.items():
                        tot_act += gstats.get("win", 0) + gstats.get("loss", 0) + gstats.get("draw", 0)

                    tot_user += tot_act
                    monthly_records.append({
                        "user_id": user_id,
                        "month": month,
                        "tot_matches": tot_act
                    })

                global_totals[user_id] = global_totals.get(user_id, 0) + tot_user

    df_global = pd.DataFrame([{"user_id": u, "tot_matches": g} for u, g in global_totals.items()])
    df_monthly = pd.DataFrame(monthly_records).groupby(["user_id", "month"], as_index=False).agg({"tot_matches": "sum"})
    return df_global, df_monthly


def add_total_games_global(csv_path, jsonl_path, output_path):
    # Carica CSV
    df = pd.read_csv(csv_path)
    # Rinomina la colonna tot_games se presente
    if "tot_matches" in df.columns:
        df.rename(columns={"tot_matches": "tot_analysed_matches"}, inplace=True)

    # Calcola totali da activity
    df_global, _ = load_activity_totals(jsonl_path)

    # Merge
    df_merged = pd.merge(df, df_global, on="user_id", how="left")

    # Salva
    df_merged.to_csv(output_path, index=False)
    print(f"Salvato {output_path}")


def add_total_games_monthly(csv_path, jsonl_path, output_path):
    # Carica CSV
    df = pd.read_csv(csv_path)
    # Rinomina la colonna tot_games se presente
    if "tot_matches" in df.columns:
        df.rename(columns={"tot_matches": "tot_analysed_matches"}, inplace=True)

    # Calcola totali da activity
    _, df_monthly = load_activity_totals(jsonl_path)

    # Merge su user_id + month
    df_merged = pd.merge(df, df_monthly, on=["user_id", "month"], how="left")

    df_merged["tot_matches"] = df_merged["tot_matches"].fillna(0).astype(int)

    # Salva
    df_merged.to_csv(output_path, index=False)
    print(f"Salvato {output_path}")

add_total_games_global(
    csv_path="global_stats_lichess.csv",
    jsonl_path="lichess_activity_matched.jsonl",
    output_path="global_stats_lichess_tot_matches.csv"
)

add_total_games_monthly(
    csv_path="monthly_stats_lichess.csv",
    jsonl_path="lichess_activity_matched.jsonl",
    output_path="monthly_stats_lichess_tot_matches.csv"
)