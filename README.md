# Homepedia — Dashboard Immobilier & Socio-économique 🇫🇷

En une phrase. Homepedia transforme des données publiques (DVF + INSEE) en indicateurs lisibles et cartes interactives pour comprendre le marché immobilier français, par département.

# 1) À quoi ça sert

Visualiser le prix moyen au m² par département et par type de bien.

Croiser le marché avec des indicateurs socio-éco : revenu médian, chômage, pauvreté, population.

Filtrer (départements, type de bien, bornes de prix/surface, période) et exporter les résultats.

# 2) Données utilisées

DVF 2024 (~2 M de transactions).

INSEE : FILOSOFI (revenu médian 2021), Chômage T1 2025, Pauvreté 2021, Population 2024.

# 3) Principales métriques

Prix moyen €/m², distributions (histogrammes, box-plots).

Corrélations entre revenu, chômage, pauvreté et prix.

Cartes choroplèthes au niveau départemental.

# 4) Pile technique

ETL : Python 3.11 (pandas, PySpark pour les agrégations massives, GeoPandas, SQLAlchemy).

Base : SQLite (db/homepedia.db) — légère, portable, prête pour Docker.

App : Streamlit + Folium/Matplotlib pour les visualisations.

Qualité : tests pytest (ingestion/transformations).

# 5) Architecture (simplifiée)

Ingestion & nettoyage des CSV (DVF + INSEE) → calcul de price_m2, normalisation des codes départements.

Chargement des tables indicateurs + transactions dans SQLite avec index (dept, price_m2, date_mutation).

Interface Streamlit qui lit homepedia.db et expose cartes + graphiques + filtres.

# 6) Lancer le projet (local)
python -m venv .venv
Windows: .venv\Scripts\activate   |   Linux/Mac: source .venv/bin/activate
pip install -r requirements.txt

# ETL (≈2–3 min selon machine)
python src/backend/ingest_valeursfoncieres.py
python src/backend/ingest_insee_population.py
python src/backend/ingest_insee_poverty.py
python src/backend/ingest_insee_unemployment.py
python src/backend/ingest_insee_income.py
python src/backend/spark_dvf_analysis.py

# UI
streamlit run src/app/streamlit_app.py   
http://localhost:8501
Option : docker compose up --build si tu utilises Docker.
