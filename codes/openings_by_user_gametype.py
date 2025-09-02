import json
import csv
from collections import defaultdict

input_file = r"C:\Users\user\OneDrive\Desktop\progetto scacchi\definitivi\lichess_games_matched.jsonl"
output_file = "openings_by_user_gametype.csv"

# Dizionario: {(username, game_type): {apertura: conteggio}}
user_openings = defaultdict(lambda: defaultdict(int))

with open(input_file, "r", encoding="utf-8") as f:
    next(f)  # salta la prima riga
    for line in f:
        if not line.strip():
            continue
        data = json.loads(line)

        for _, sessions in data.items():
            for session in sessions:
                for game in session.get("details", []):
                    username = game.get("username")
                    game_type = game.get("speed")  # tipo di partita
                    opening = game.get("opening", {}).get("name")
                    if username and game_type and opening:
                        user_openings[(username, game_type)][opening] += 1

# Trova tutte le aperture giocate (per creare le colonne del CSV)
all_openings = sorted({opening for openings in user_openings.values() for opening in openings})

# Scrive il CSV
with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    # Header: username + game_type + nomi aperture
    writer.writerow(["username", "game_type"] + all_openings)

    # Righe: username + game_type + conteggi aperture
    for (user, game_type), openings in user_openings.items():
        row = [user, game_type] + [openings.get(op, 0) for op in all_openings]
        writer.writerow(row)

print(f"✅ CSV generato in: {output_file}")
