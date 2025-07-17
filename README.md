Homepedia — Dashboard Immobilier & Socio‑économique 🇫🇷

Version : juillet 2025 — documentation finale

1. Contexte & objectifs

Homepedia est une application interactive (Streamlit + Docker) permettant :

d’explorer 2 M+ transactions DVF 2024 ;

de les croiser avec des indicateurs INSEE (revenu médian, chômage, pauvreté, population) ;

de fournir un jeu de visualisations et d’insights simples pour les décideurs publics & privés.

2. Stack & architecture

CSV / Scraping  →  Scripts ETL (Python)  →  SQLite (homepedia.db)
                           │
                           └───►  Streamlit 5 vues  →  Docker

Backend ETL : Python 3.11, pandas, PySpark, GeoPandas, SQLAlchemy.

BDD : SQLite (léger, portable, parfait pour dockeriser l’app).

Frontend : Streamlit + Folium + Matplotlib/Seaborn.

Tests : pytest (12 tests d’ingestion).

CI : optionnel (GitHub Actions) — voir dossier .github/workflows.

3. Pré‑processing des données

Source

Script

Nettoyage / Transfo clés

Gain apporté

DVF 2024 (≈ 4 Go)

ingest_valeursfoncieres.py

conversion valeur_fonciere → float, suppression surfaces 0, extraction code dept depuis CP

calcul fiable prix €/m², jointure rapide dept

Revenu médian

ingest_insee_income.py

filtrage MED_SL, EUR_YR, an 2021, gestion Corse/DOM‑TOM, médiane dept

comparabilité pouvoir d’achat

Taux chômage T1 2025

ingest_insee_unemployment.py

conversion ,→. float, agrégation dept

variable marché du travail

Pauvreté 2021

ingest_insee_poverty.py

nettoyage %, agrégation dept

vulnérabilité socio‑éco

Population 2024

ingest_insee_population.py

suppression espaces milliers, cast int

taille de marché

Pipeline assuré : exécution séquentielle des scripts → BDD prête → lancement Streamlit.

4. Schéma BDD & justification stockage



Transactions : clé primaire auto‑inc. id + code département dérivé.

Tables indicateurs (population, poverty, income, unemployment) : clé code (varchar 2‑3).

Indexes créés sur transactions.dept, price_m2, dates.

Pourquoi SQLite ? léger, sans serveur, parfait pour Docker ; volume < 300 Mo compressé.

5. Métriques choisies & justification

Métrique

Raison d’être

Source

Prix moyen €/m²

Indicateur clé du marché immobilier

DVF 2024

Revenu médian (€ / an)

Pouvoir d’achat local, corrélé aux prix

FILOSOFI 2021

Taux de chômage %

Mesure dynamique emploi → demande logement

INSEE Chômage T1 2025

Taux de pauvreté %

Vulnérabilité socio‑éco & tension logement

INSEE Pauvreté 2021

Population (hab.)

Taille de marché et densité

INSEE Pop 2024

6. Librairies data‑science mises en œuvre

pandas : manipulation tabulaire (merge, groupby, to_sql).

GeoPandas : jointure GeoJSON + indicateurs → cartes choroplèthes.

Matplotlib / Seaborn : histogrammes, box‑plots, scatter, heatmaps.

PySpark : agrégations rapides sur le CSV DVF brut (≈ 2 M lignes).

Folium : rendu cartographie interactive dans Streamlit.

7. Visualisations clés (≥ 3 affichages distincts)

Vue Streamlit

Visualisation

Insight rapide

Standard

Carte choroplèthe prix/m² dept

Hot‑spots nationaux ↔ littoral & IDF

Standard

Histogramme prix/m² filtrable

queue longue → valeurs extrêmes

Standard

Box‑plot prix/m² ↔ type_local

appartements + chers/m²

Socio‑éco

Scatter Revenu ↔ Chômage

pente négative R² 0.4

Région

Heatmap corrélation multivariée

corrélation négative Revenu ↔ Pauvreté

8. Contraintes sujet : conformité

Aucune donnée manuelle : tout provient de scripts ETL automatisés.

Docker‑ready : docker-compose up -d lance ETL + Streamlit.

Tests unitaires : pytest -q → 12 tests OK.

Performances : index SQL + agrégations PySpark (<15 s build table).

9. Exécution rapide

# 1) Pré‑requis
python -m venv .venv && .\.venv\Scripts\activate
pip install -r requirements.txt

# 2) Lancer pipeline ETL (≈ 2‑3 min)
python src/backend/ingest_valeursfoncieres.py
python src/backend/ingest_insee_population.py
python src/backend/ingest_insee_poverty.py
python src/backend/ingest_insee_unemployment.py
python src/backend/ingest_insee_income.py
python src/backend/spark_dvf_analysis.py

# 3) Frontend
streamlit run src/app/streamlit_app.py
Alternativement : docker compose up --build

10. Roadmap post‑soutenance

Ajout indicateurs démographie/âge moyen (INSEE).

Tests UI Streamlit avec Playwright.

CI GitHub Actions (lint + pytest + build Docker).

Déploiement public (Railway / Render) avec SQLite montée en readonly.

11. Auteur

Adrien TROISE — Epitech Nice (Architecte logiciel) 2023‑2025
