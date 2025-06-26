import os
import pandas as pd

# 1. Chemins
RAW_DIR    = os.path.join('data', 'raw', 'dvf2024')
OUTPUT_DIR = os.path.join('data', 'processed')
os.makedirs(OUTPUT_DIR, exist_ok=True)

INPUT_FILE  = os.path.join(RAW_DIR, 'valeursfoncieres-2024.txt')
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'transactions_2024.csv')

# 2. Colonnes cibles en snake_case (avec 'commune' au lieu de 'nom_commune')
TARGET_COLS = [
    'date_mutation',
    'nature_mutation',
    'valeur_fonciere',
    'code_postal',
    'commune',
    'type_local',
    'surface_reelle_bati',
    'nombre_pieces_principales'
]

print(f"Lecture du fichier brut : {INPUT_FILE}")
# 3. Lecture sans parse_dates
df = pd.read_csv(
    INPUT_FILE,
    sep='|',
    low_memory=False
)

# 4. Normaliser les noms de colonnes
df.columns = (
    df.columns
      .str.strip()
      .str.lower()
      .str.replace(' ', '_', regex=False)
      .str.normalize('NFKD')
      .str.encode('ascii', errors='ignore')
      .str.decode('utf-8')
)

# 5. Vérifier la présence des colonnes cibles
missing = set(TARGET_COLS) - set(df.columns)
if missing:
    raise KeyError(f"Colonnes manquantes après normalisation : {missing}")

# 6. Filtrer et nettoyer
df = df[TARGET_COLS]
# Conversion explicite de la date (format JJ/MM/AAAA)
df['date_mutation'] = pd.to_datetime(df['date_mutation'], dayfirst=True, errors='coerce')
df = df.drop_duplicates()
df = df.dropna(subset=['date_mutation', 'valeur_fonciere', 'code_postal'])

# 7. Export
print(f"Écriture du CSV nettoyé : {OUTPUT_FILE}")
df.to_csv(OUTPUT_FILE, index=False)

print("Terminé.")