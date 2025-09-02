import pandas as pd
import json
from datetime import datetime
import re

# --- CONFIG ---
csv_path = "monthly_delta_rating_percentiles.csv" 
jsonl_path = r"C:\Users\user\OneDrive\Desktop\progetto scacchi\definitivi\lichess_games_matched.jsonl"
openings_path = r"C:\Users\user\OneDrive\Desktop\progetto scacchi\analisi_2\openings_with_ply.tsv"

# --- CARICA CSV e openings ---
df_csv = pd.read_csv(csv_path)  # contiene già user_id, game_type, month
df_openings = pd.read_csv(openings_path, sep="\t")  # eco, name, ply_theoretical, pgn

# Trasforma la PGN teorica in lista di mosse
def pgn_to_moves(pgn_line):
    if isinstance(pgn_line, str) and pgn_line.strip():
        pgn_clean = re.sub(r'\d+\.\s*', '', pgn_line)  # rimuove numeri mosse
        moves = [m for m in pgn_clean.split() if m.strip() != '']
        return moves
    return []

# Dizionario: nome variante → lista mosse teoriche
opening_moves_dict = {
    row['name']: pgn_to_moves(row['pgn']) for _, row in df_openings.iterrows()
}

# --- FUNZIONE: estrae info sullo svantaggio ---
def check_disadvantage(user_analysis, step, user_color, threshold):
    for i in range(step, len(user_analysis), 2):
        eval_data = user_analysis[i]
        if "eval" in eval_data:
            eval_value = eval_data["eval"]
            if (user_color == "white" and eval_value <= -threshold) or \
               (user_color == "black" and eval_value >= threshold):
                return i
    return None

# --- FUNZIONE: processa un singolo game JSON (dettaglio) ---
def process_game(detail, username):
    try:
        created_at = detail.get("createdAt")
        if not created_at:
            return None
        date = datetime.utcfromtimestamp(created_at / 1000)
        month = date.strftime("%Y-%m")  

        user_id = username
        game_type = detail.get("speed", "unknown").lower()

        eco = None
        ply_theoretical = None
        opening_name = None
        if "opening" in detail and isinstance(detail["opening"], dict):
            eco = detail["opening"].get("eco")
            ply_theoretical = detail["opening"].get("ply")
            opening_name = detail["opening"].get("name")

        # Mosse giocate
        game_moves_str = detail.get("moves", "")
        game_moves_list = [m for m in game_moves_str.split() if m.strip() != ""]

        # Calcolo opening_gap
        opening_gap = None
        opening_gap_above_7_ply = None
        if opening_name and opening_name in opening_moves_dict:
            opening_moves_theory = opening_moves_dict[opening_name][:ply_theoretical]
            for i, move in enumerate(opening_moves_theory):
                if i >= len(game_moves_list) or move != game_moves_list[i]:
                    opening_gap = ply_theoretical - i
                    break
            else:
                opening_gap = 0
            if ply_theoretical and ply_theoretical > 7:
                opening_gap_above_7_ply = opening_gap

        # --- Analisi giocatore ---
        analysis_data = None
        user_rating = None
        user_color = None
        user_resigned = False
        step = None
        opponent_rating = None
        winner = detail.get('winner', 'draw')
        user_analysis = detail.get("analysis", [])
        user_clocks = detail.get('clocks', [])
        division = detail.get('division', {})
        middle = division.get("middle", 20)
        end = division.get("end", 40)
        time_opening = []
        time_middle_game = []
        time_end_game = []

        if "players" in detail:
            for color in ["white", "black"]:
                player = detail["players"].get(color, {})
                user = player.get("user", {})
                if user and user.get("id") == user_id.lower():
                    analysis_data = player.get("analysis", {})
                    user_rating = player.get("rating")
                    user_color = color
                    step = 0 if user_color == 'white' else 1
                else:
                    opponent_rating = player.get("rating") if player.get("rating") else None

        if detail.get("status") == "resign" and user_color and user_color != winner:
            user_resigned = True

        if analysis_data:
            inaccuracy_avg = analysis_data.get("inaccuracy")
            mistake_avg = analysis_data.get("mistake")
            blunder_avg = analysis_data.get("blunder")
            acpl_avg = analysis_data.get("acpl")
            accuracy_avg = analysis_data.get("accuracy")
        else:
            inaccuracy_avg = mistake_avg = blunder_avg = acpl_avg = accuracy_avg = None

        diff_opponent = None
        if user_rating is not None and opponent_rating is not None:
            diff_opponent = user_rating - opponent_rating

        if user_clocks and user_color:
            user_opening = [t for i, t in enumerate(user_clocks) if i % 2 == step and i < middle]
            user_middle_game = [t for i, t in enumerate(user_clocks) if i % 2 == step and middle <= i < end]
            user_end_game = [t for i, t in enumerate(user_clocks) if i % 2 == step and i >= end]

            time_opening = [user_opening[i] - user_opening[i+1] for i in range(len(user_opening)-1)]
            time_middle_game = [user_middle_game[i] - user_middle_game[i+1] for i in range(len(user_middle_game)-1)]
            time_end_game = [user_end_game[i] - user_end_game[i+1] for i in range(len(user_end_game)-1)]

        # svantaggio >= 150 cp
        idx_disadv = check_disadvantage(user_analysis, step, user_color, threshold=150) if user_color else None
        moves_after_disadvantage_150 = None
        if idx_disadv is not None:
            total_user_moves = len(user_analysis[step::2])
            moves_until_disadv = (idx_disadv - step)//2 + 1
            moves_after_disadvantage_150 = total_user_moves - moves_until_disadv

        # svantaggio >= 200 cp
        idx_extreme = check_disadvantage(user_analysis, step, user_color, threshold=200) if user_color else None
        moves_after_disadvantage_200 = None
        if idx_extreme is not None:
            total_user_moves = len(user_analysis[step::2])
            moves_until_disadv = (idx_extreme - step)//2 + 1
            moves_after_disadvantage_200 = total_user_moves - moves_until_disadv

        # vittorie da svantaggio
        wins_from_disadvantage_150 = 0
        wins_from_disadvantage_150_outoftime = 0
        wins_from_disadvantage_200 = 0
        wins_from_disadvantage_200_outoftime = 0
        draws_from_disadvantage_150 = 0

        if user_analysis:
            if user_color == winner:
                if idx_disadv is not None:
                    wins_from_disadvantage_150 = 1
                    if detail.get("status") == 'outoftime':
                        wins_from_disadvantage_150_outoftime = 1
                if idx_extreme is not None:
                    wins_from_disadvantage_200 = 1
                    if detail.get("status") == 'outoftime':
                        wins_from_disadvantage_200_outoftime = 1
            elif winner == 'draw':
                if idx_disadv is not None:
                    draws_from_disadvantage_150 = 1

        return {
            "user_id": user_id,
            "game_type": game_type,
            "month": month,   
            "eco": eco,
            "opening_name": opening_name,
            "ply_theoretical": ply_theoretical,
            "opening_gap": opening_gap,
            "opening_gap_above_7_ply": opening_gap_above_7_ply,
            "inaccuracy_avg": inaccuracy_avg,
            "mistake_avg": mistake_avg,
            "blunder_avg": blunder_avg,
            "acpl_avg": acpl_avg,
            "accuracy_avg": accuracy_avg,
            "diff_opponent": diff_opponent,
            "avg_time_opening": sum(time_opening)/len(time_opening)/100 if time_opening else None,
            "avg_time_middle": sum(time_middle_game)/len(time_middle_game)/100 if time_middle_game else None,
            "avg_time_end": sum(time_end_game)/len(time_end_game)/100 if time_end_game else None,
            "wins_from_disadvantage_150": wins_from_disadvantage_150,
            "wins_from_disadvantage_150_outoftime": wins_from_disadvantage_150_outoftime,
            "wins_from_disadvantage_200": wins_from_disadvantage_200,
            "wins_from_disadvantage_200_outoftime": wins_from_disadvantage_200_outoftime,
            "draws_from_disadvantage_150": draws_from_disadvantage_150,
            "moves_after_disadvantage_150": moves_after_disadvantage_150,
            "moves_after_disadvantage_200": moves_after_disadvantage_200,
            "user_resigned": 1 if user_resigned else 0,
            "tot_matches_disadvantage_150": 1 if idx_disadv is not None else 0,
            "tot_matches_disadvantage_200": 1 if idx_extreme is not None else 0,
            "tot_matches": 1
        }
    except Exception as e:
        print(f"Errore parsing game: {e}")
        return None

# --- LEGGI JSONL E PROCESSA ---
all_records = []
with open(jsonl_path, "r", encoding="utf-8") as f:
    for i, line in enumerate(f, start=1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            for username, games_list in obj.items():
                for g in games_list:
                    for detail in g.get("details", []):
                        rec = process_game(detail, username)
                        if rec:
                            all_records.append(rec)
            print(f"Riga {i} valida, aggiungo")
        except json.JSONDecodeError:
            print(f"Riga {i} non valida, salto")

# --- CREA DATAFRAME ---
df_json = pd.DataFrame(all_records)

# --- AGGREGAZIONE PER UTENTE + GAME_TYPE + MESE ---
df_json_monthly = (
    df_json.groupby(["user_id", "game_type", "month"], as_index=False)
    .agg(
        unique_eco=("eco", "nunique"),
        ply_theoretical_avg=("ply_theoretical", "mean"),
        opening_gap_avg=("opening_gap", "mean"),
        opening_gap_above_7_ply_avg=("opening_gap_above_7_ply", "mean"),
        inaccuracy_avg=("inaccuracy_avg", "mean"),
        mistake_avg=("mistake_avg", "mean"),
        blunder_avg=("blunder_avg", "mean"),
        acpl_avg=("acpl_avg", "mean"),
        accuracy_avg=("accuracy_avg", "mean"),
        diff_opponent_avg=("diff_opponent", "mean"),
        avg_time_opening=("avg_time_opening", "mean"),
        avg_time_middle=("avg_time_middle", "mean"),
        avg_time_end=("avg_time_end", "mean"),
        wins_from_disadvantage_150=("wins_from_disadvantage_150", "sum"),
        wins_from_disadvantage_150_outoftime=("wins_from_disadvantage_150_outoftime", "sum"),
        wins_from_disadvantage_200=("wins_from_disadvantage_200", "sum"),
        wins_from_disadvantage_200_outoftime=("wins_from_disadvantage_200_outoftime", "sum"),
        draws_from_disadvantage_150=("draws_from_disadvantage_150", "sum"),
        moves_after_disadvantage_150=("moves_after_disadvantage_150", "mean"),
        moves_after_disadvantage_200=("moves_after_disadvantage_200", "mean"),
        tot_resigned=("user_resigned", "sum"),
        tot_matches_disadvantage_150=("tot_matches_disadvantage_150", "sum"),
        tot_matches_disadvantage_200=("tot_matches_disadvantage_200", "sum"),
        tot_matches=("tot_matches", "sum")
    )
)

# --- MERGE CON CSV MENSILE ---
df_merged = pd.merge(
    df_csv,
    df_json_monthly,
    how="inner",
    on=["user_id", "game_type", "month"]   
)

# --- SALVA RISULTATO ---
output_path = "monthly_stats_lichess.csv"
df_merged.to_csv(output_path, index=False)
print(f"File salvato come {output_path}")
