import csv

# File input/output
openings_file = "openings_by_user_gametype.csv"
stats_file = "global_stats_lichess.csv"
output_file = "openings_by_user_gametype_enriched.csv"

# 1. Carica i dati global_stats_lichess.csv in un dizionario {(username, game_type): (delta_percentile, rating_level)}
user_stats = {}
with open(stats_file, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        username = row["user_id"]
        game_type = row["game_type"]
        delta_percentile = row.get("delta_percentile", "")
        rating_level = row.get("rating_level", "")
        user_stats[(username, game_type)] = (delta_percentile, rating_level)

# 2. Legge il CSV delle aperture e aggiunge delta_percentile e rating_level
with open(openings_file, "r", encoding="utf-8") as infile, \
     open(output_file, "w", newline="", encoding="utf-8") as outfile:
    
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames + ["delta_percentile", "rating_level"]
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    
    writer.writeheader()
    
    for row in reader:
        key = (row["username"], row["game_type"])
        delta, rating = user_stats.get(key, ("", ""))
        row["delta_percentile"] = delta
        row["rating_level"] = rating
        writer.writerow(row)

print(f"✅ CSV arricchito generato in: {output_file}")
