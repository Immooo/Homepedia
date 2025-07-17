# Homepedia — Projet d’analyse immobilière France

## Contexte général
Homepedia est une application interactive développée en Python avec Streamlit pour explorer les transactions immobilières françaises DVF 2024 enrichies par des indicateurs socio-économiques INSEE (chômage, revenu médian, pauvreté, population...).

## Architecture technique
- Python 3.11, PySpark, pandas, geopandas, matplotlib, seaborn, folium
- Base SQLite `homepedia.db` contenant les données nettoyées
- Frontend Streamlit multi-onglets avec cartes, graphiques, wordcloud, matrices de corrélation
- Environnement Dockerisé avec Dockerfile et docker-compose.yml

## Préprocessing des données

Chaque source de données INSEE et DVF subit un traitement automatisé pour garantir qualité et cohérence des analyses :

- **Transactions DVF** : nettoyage des surfaces et valeurs foncières, suppression des valeurs aberrantes, extraction du code département depuis le code postal.
- **Revenu médian INSEE** : filtrage des mesures fiables (FILOSOFI_MEASURE = MED_SL), conversion des codes INSEE communes en codes départements (gestion spécifique Corse et DOM-TOM), agrégation par médiane départementale.
- **Taux de chômage, pauvreté, population** : nettoyage des formats numériques, conversion en float/int, gestion des valeurs manquantes.
- Ces traitements facilitent les jointures, assurent cohérence des analyses croisées et optimisent les performances SQL.

## Structure de la base de données SQLite

La base est relationnelle, organisée autour de plusieurs tables principales :

| Table        | Description                                 | Clé primaire       |
|--------------|---------------------------------------------|--------------------|
| transactions | Transactions immobilières DVF               | id (auto-increment)|
| population   | Population par département                   | code (département) |
| poverty      | Taux de pauvreté par département            | code (département) |
| income       | Revenu médian par département                | code (département) |
| spark_dept_analysis | Résultats pré-agrégés DVF par département | -                  |

Les clés sont des codes départements normalisés (2 ou 3 caractères), garantissant la cohérence dans les jointures.

SQLite a été choisi pour sa légèreté, simplicité d’usage et compatibilité avec Streamlit, suffisante pour les volumes et analyses ciblés.

## Choix des métriques

Les métriques clés sont :

- **Prix moyen au m²** : indicateur principal extrait des transactions DVF.
- **Revenu médian** : reflète le niveau de vie médian dans chaque département.
- **Taux de chômage** : indicateur clé de la santé économique régionale.
- **Taux de pauvreté** : indicateur socio-économique complémentaire.
- **Population** : taille des marchés immobiliers départementaux.

Chaque métrique est nettoyée et agrégée pour assurer comparabilité et pertinence analytique.

## Librairies Data Science utilisées

- **pandas** : manipulation efficace des données tabulaires.
- **geopandas** : gestion et affichage des données géographiques (GeoJSON).
- **matplotlib & seaborn** : visualisation statistique avancée.
- **PySpark** : traitement big data des transactions DVF volumineuses.
- **folium** : cartes interactives choroplèthes.

## Utilisation

1. Lancer les scripts d’ingestion dans l’ordre pour créer et remplir la base SQLite :

```bash
python src/backend/ingest_insee_income.py
python src/backend/ingest_insee_unemployment.py
python src/backend/ingest_insee_poverty.py
python src/backend/ingest_insee_population.py
python src/backend/ingest_valeursfoncieres.py
python src/backend/spark_dvf_analysis.py

streamlit run src/app/streamlit_app.py

Auteurs
Adrien TROISE — Étudiant architecte logiciel, Epitech Nice 2023-2025