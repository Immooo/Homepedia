import os
import sqlite3
import pandas as pd
import geopandas as gpd
import folium

# 1. Connexion SQLite et calcul des moyens
DB_PATH = os.path.join('data', 'homepedia.db')
conn = sqlite3.connect(DB_PATH)
query = """
SELECT
  substr(code_postal,1,2) AS dept,
  AVG(
    CAST(REPLACE(REPLACE(valeur_fonciere,' ',''),',','.') AS REAL)
    / surface_reelle_bati
  ) AS prix_m2_moyen
FROM transactions
WHERE surface_reelle_bati > 0
GROUP BY dept
"""
prix_dept = pd.read_sql_query(query, conn)
prix_dept = prix_dept.rename(columns={'dept':'code'})
conn.close()

# 2. Charger le GeoJSON
geojson = os.path.join('data', 'raw', 'geo', 'departements_simplifie.geojson')
gdf = gpd.read_file(geojson)[['code','geometry']]

# 3. Fusionner et créer la carte
gdf = gdf.merge(prix_dept, on='code', how='left')
m = folium.Map(location=[46.6,2.4], zoom_start=5)
folium.Choropleth(
    geo_data=gdf,
    data=gdf,
    columns=['code','prix_m2_moyen'],
    key_on='feature.properties.code',
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name='Prix moyen (€ / m²)',
    nan_fill_color='white'
).add_to(m)
folium.LayerControl().add_to(m)

# 4. Sauvegarder
out_dir = os.path.join('outputs','maps')
os.makedirs(out_dir, exist_ok=True)
html_file = os.path.join(out_dir,'map_choropleth.html')
m.save(html_file)
print(f"Carte choroplèthe enregistrée → {html_file}")