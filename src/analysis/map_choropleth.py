import os
import pandas as pd
import geopandas as gpd
import folium

# 1. Chemins
CSV_FILE     = os.path.join('data', 'processed', 'transactions_2024.csv')
GEOJSON_FILE = os.path.join('data', 'raw', 'geo', 'departements_simplifie.geojson')
OUTPUT_DIR   = os.path.join('outputs', 'maps')
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_HTML  = os.path.join(OUTPUT_DIR, 'map_choropleth.html')

# 2. Charger et préparer les données
df = pd.read_csv(CSV_FILE, parse_dates=['date_mutation'], dtype={'code_postal': str})
df = df[df['surface_reelle_bati'] > 0].copy()
df['valeur_fonciere'] = (
    df['valeur_fonciere']
      .str.replace(' ', '')
      .str.replace(',', '.', regex=False)
      .astype(float)
)
df['prix_m2'] = df['valeur_fonciere'] / df['surface_reelle_bati']
df['dept'] = df['code_postal'].str[:2]

# 3. Calcul du prix moyen par département
price_by_dept = (
    df.groupby('dept')['prix_m2']
      .mean()
      .reset_index()
      .rename(columns={'dept': 'code', 'prix_m2': 'prix_m2_moyen'})
)

# 4. Charger le GeoJSON des départements
gdf = gpd.read_file(GEOJSON_FILE)[['code', 'nom', 'geometry']]

# 5. Fusionner
gdf = gdf.merge(price_by_dept, on='code', how='left')

# 6. Créer la carte
m = folium.Map(location=[46.6, 2.4], zoom_start=5)
folium.Choropleth(
    geo_data=gdf,
    name='Choropleth',
    data=gdf,
    columns=['code', 'prix_m2_moyen'],
    key_on='feature.properties.code',
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name='Prix moyen (€ / m²)'
).add_to(m)
folium.LayerControl().add_to(m)

# 7. Sauvegarder
m.save(OUTPUT_HTML)
print(f"Carte choroplèthe enregistrée → {OUTPUT_HTML}")