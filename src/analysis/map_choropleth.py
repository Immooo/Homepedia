import os
import sqlite3
import pandas as pd
import geopandas as gpd
import folium

# 1. Connexion SQLite
DB_PATH = os.path.join('data', 'homepedia.db')
conn = sqlite3.connect(DB_PATH)

# 2. Charger les prix moyens par département
query = """
SELECT
    substr(code_postal,1,2) AS dept,
    AVG(
      CAST(REPLACE(REPLACE(valeur_fonciere, ' ', ''), ',', '.') AS REAL)
      /
      surface_reelle_bati
    ) AS prix_m2_moyen
FROM transactions
WHERE surface_reelle_bati > 0
GROUP BY dept
"""
prix_dept = pd.read_sql_query(query, conn)
prix_dept = prix_dept.rename(columns={'dept':'code'})

conn.close()

# 3. Charger le GeoJSON des départements
GEOJSON_FILE = os.path.join('data', 'raw', 'geo', 'departements_simplifie.geojson')
gdf = gpd.read_file(GEOJSON_FILE)[['code', 'geometry']]

# 4. Fusionner
gdf = gdf.merge(prix_dept, on='code', how='left')

# 5. Créer et sauvegarder la carte
OUTPUT_DIR  = os.path.join('outputs', 'maps')
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_HTML = os.path.join(OUTPUT_DIR, 'map_choropleth.html')

m = folium.Map(location=[46.6, 2.4], zoom_start=5)
folium.Choropleth(
    geo_data=gdf,
    data=gdf,
    columns=['code', 'prix_m2_moyen'],
    key_on='feature.properties.code',
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name='Prix moyen (€ / m²)',
    nan_fill_color='white'
).add_to(m)
folium.LayerControl().add_to(m)

m.save(OUTPUT_HTML)
print(f"Carte choroplèthe enregistrée → {OUTPUT_HTML}")