import pandas as pd

# === 1. Caricamento del CSV wide ===
df = pd.read_csv("openings_by_user_gametype_enriched.csv")

# Supponiamo che il CSV abbia queste colonne fisse:
# username, rating_level, game_type, quartile, e poi tutte le aperture

# === 2. Individuare le colonne da unpivotare (aperture) ===
id_vars = ["username", "rating_level", "game_type", "delta_percentile"]
aperture_cols = [c for c in df.columns if c not in id_vars]

# === 3. Unpivot (melt) ===
df_long = df.melt(
    id_vars=id_vars,
    value_vars=aperture_cols,
    var_name="Apertura",
    value_name="Conteggio"
)

# === 4. Rimuovere righe con conteggio 0 o NaN ===
df_long = df_long[df_long["Conteggio"] > 0]

# === 5. Aggregare per quartile, apertura, game_type, rating_level ===
df_final = (
    df_long.groupby(["delta_percentile", "rating_level", "game_type", "Apertura"], as_index=False)
           .agg({"Conteggio": "sum"})
)

# === 6. Esporta il nuovo CSV ===
df_final.to_csv("openings_definitivo.csv", index=False)

print("✅ CSV generato: output_aperture_long.csv")
