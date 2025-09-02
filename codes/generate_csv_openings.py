import pandas as pd
import glob
from pathlib import Path
import re

# Percorso dei TSV
path = Path(r"C:\Users\user\OneDrive\Desktop\progetto scacchi\analisi_2")
all_files = glob.glob(str(path / "*.tsv"))

df_list = []
for file in all_files:
    df = pd.read_csv(file, sep="\t")
    df_list.append(df)

df_openings = pd.concat(df_list, ignore_index=True)

# Funzione per calcolare ply da PGN
def calculate_ply_from_pgn(pgn_line):
    if isinstance(pgn_line, str) and pgn_line.strip():
        # rimuove numeri e punti
        pgn_clean = re.sub(r'\d+\.\s*', '', pgn_line)
        # split per spazi, scarta vuoti
        moves = [m for m in pgn_clean.split() if m.strip() != '']
        return len(moves)
    return None

if "pgn" in df_openings.columns:
    df_openings["ply_theoretical"] = df_openings["pgn"].apply(calculate_ply_from_pgn)
else:
    df_openings["ply_theoretical"] = None

# Rimuove duplicati: se ci sono più righe per lo stesso eco+name, tieni quella con ply non nullo
df_openings = df_openings.sort_values(by="ply_theoretical", na_position='last')
df_openings = df_openings.drop_duplicates(subset=["eco", "name"], keep="first")

# Mantieni solo colonne essenziali
#df_openings = df_openings[["eco", "name", "ply_theoretical"]].reset_index(drop=True)

# Salva in TSV
output_tsv = path / "openings_with_ply.tsv"
df_openings.to_csv(output_tsv, sep="\t", index=False)

print(f"File aggregato salvato con successo in: {output_tsv}")
