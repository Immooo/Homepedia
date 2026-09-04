# Homepedia — Analyse fiable du projet

> État de l'analyse : inspection statique du dépôt et des bases locales, actualisée
> le 22 août 2026 après consolidation de la qualité DVF, du cluster Spark, de
> l'interface et de la séparation du stockage temps réel. Les éléments marqués
> **Constaté** sont démontrés par le dépôt ou les bases locales ; les éléments
> marqués **Hypothèse** restent à confirmer.

> Précisions données par le porteur du projet le 12 août 2026 : le dépôt et la
> base ont été copiés depuis son ancien PC portable ; certains environnements ou
> artefacts peuvent donc manquer. La branche officielle retenue est la plus
> récente localement, `feat/realtime-dq-dedup`. MongoDB, Metabase, PostgreSQL,
> SeLoger et l'analyse d'avis font partie du périmètre de soutenance, même lorsque
> leur intégration est expérimentale ou incomplète.

## 1. Résumé exécutif

Homepedia est un projet de data engineering et de data visualisation appliqué au
marché immobilier français. Son cœur historique transforme les transactions DVF
2024 et plusieurs indicateurs INSEE en jeux de données nettoyés, agrégats
départementaux/régionaux et visualisations Streamlit. Une extension plus récente
collecte périodiquement des indices de prix des logements publiés par l'INSEE,
applique des contrôles de qualité et conserve un état courant, un historique des
changements et un journal d'exécution dans SQLite et MongoDB
(`README.md`, `src/backend/ingest_valeursfoncieres.py`,
`src/backend/spark_dvf_analysis.py`, `src/app/streamlit_app.py`,
`src/realtime_price/worker.py`).

Ce n'est pas une architecture Hadoop/Kafka de production. Spark est utilisé dans
un cluster standalone Docker mono-machine (un master et deux workers) pour la
pré-agrégation, et le « temps réel » est du **polling micro-batch**
toutes les 300 secondes par défaut, pas un flux événementiel continu
(`src/backend/spark_dvf_analysis.py`, `src/realtime_price/config.py`,
`src/realtime_price/worker.py`).

La base analytique constitue une preuve d'exécution significative : après retrait
de plus de 3,9 millions de lignes dupliquées ou aberrantes, elle contient
1 884 593 transactions DVF uniques, 94 agrégats départementaux et 13 agrégats
régionaux, Corse incluse. Le flux périodique est isolé dans
`data/realtime_price.db`, avec son état courant, son historique et ses runs.

Le contrat canonique documenté dans `docs/DATA_CONTRACT.md` aligne désormais les
noms français utilisés par SQLite, MongoDB, Streamlit, Metabase et les tests. Le
risque résiduel porte sur la discipline de migration et les anciens scripts ou
artefacts qui pourraient encore suivre un schéma historique.

## 2. Problème résolu

Le projet cherche à rendre exploitables et compréhensibles des sources publiques
hétérogènes :

- les mutations immobilières DVF 2024 ;
- la population, le revenu médian, le chômage et la pauvreté INSEE ;
- des indices trimestriels de prix des logements INSEE collectés périodiquement ;
- un jeu annexe d'avis hôteliers pour une démonstration NoSQL et NLP.

Il normalise ces données, calcule notamment un prix au m², les agrège à des
mailles géographiques et les rend explorables par filtres, cartes, distributions,
corrélations et exports (`README.md`, `src/backend/ingest_*.py`,
`src/backend/aggregate_by_region.py`, `src/app/streamlit_app.py`).

## 3. Utilisateurs et systèmes concernés

### Utilisateurs

- un analyste ou étudiant qui explore le marché immobilier français ;
- un examinateur qui évalue la chaîne data, les choix de stockage et les
  visualisations ;
- potentiellement un utilisateur métier comparant territoires et indicateurs
  socio-économiques.

Cette cible est déduite de l'interface et du positionnement du README ; aucun
persona formel ni cahier des charges métier n'est présent
(`README.md`, `src/app/streamlit_app.py`).

### Systèmes

- fichiers publics DGFiP/DVF et INSEE en entrée ;
- site web INSEE comme source HTTP du worker ;
- SQLite comme stockage analytique local ;
- MongoDB comme miroir/document store et tampon du temps réel ;
- Streamlit comme interface ;
- Metabase comme outil BI optionnel ;
- Docker Compose comme environnement local multi-service
  (`infra/docker-compose.yml`, `src/realtime_price/config.py`,
  `src/etl/sqlite_to_mongo_all_tables.py`).

RabbitMQ et des paramètres d'authentification apparaissent dans les noms de
variables de `.env`, mais aucune utilisation n'a été trouvée dans le code ou le
Compose : ce sont des vestiges ou travaux non intégrés, pas des composants
démontrés (`.env`, inspection limitée aux noms de variables ;
`infra/docker-compose.yml`, recherche dans `src/`).

## 4. Entrées et sorties

| Catégorie | Entrée | Sortie | Preuve |
|---|---|---|---|
| DVF batch | `data/raw/dvf2024/ValeursFoncieres-2024.txt` séparé par `|` | `data/processed/transactions_2024.csv` | `src/backend/ingest_valeursfoncieres.py` |
| INSEE population | CSV départemental `;` | CSV traité et table de population | `src/backend/ingest_insee_population.py`, `src/backend/load_to_sqlite.py` |
| INSEE revenus | FILOSOFI communal 2021 | médiane départementale, CSV/table | `src/backend/ingest_insee_income.py` |
| INSEE chômage | Excel T1 2025 | CSV/table départementale | `src/backend/ingest_insee_unemployment.py` |
| INSEE pauvreté | comparateur communal | médiane départementale, CSV/table | `src/backend/ingest_insee_poverty.py` |
| Géographie | correspondance département-région et GeoJSON | jointures, cartes départementales/régionales | `src/backend/aggregate_by_region.py`, `src/app/streamlit_app.py` |
| Temps réel | page HTML INSEE | points normalisés, latest/history/runs SQLite et 3 collections Mongo | `src/realtime_price/insee_scraper.py`, `src/realtime_price/worker.py` |
| Avis annexe | `Hotel_Reviews.csv` | CSV et TinyDB JSON | `src/backend/ingest_comments.py`, `src/backend/ingest_comments_nosql.py` |
| UI | SQLite, Parquet, GeoJSON, TinyDB | dashboard, graphiques, cartes, CSV exporté | `src/app/streamlit_app.py`, `src/app/pages/06_Temps_reel_prix.py` |
| Analyses hors UI | SQLite + GeoJSON | PNG et HTML dans `outputs/` | `src/analysis/analyze_transactions.py`, `src/analysis/map_choropleth.py` |

## 5. Parcours complet d'une donnée

### 5.1 Transaction DVF — chemin principal

1. Le fichier texte DVF est lu entièrement avec pandas.
2. Les noms de colonnes sont normalisés en minuscules ASCII.
3. Huit colonnes utiles sont gardées ; dates et codes postaux sont normalisés.
4. Les doublons et lignes sans date, valeur foncière ou code postal sont retirés.
5. Le résultat est écrit en CSV traité.
6. Un chargeur SQLite/PostgreSQL peut créer et alimenter `transactions`.
7. Le job Spark relit le CSV, convertit valeur et surface, filtre les surfaces
   positives, calcule `prix_m2`, extrait le département et agrège nombre de
   transactions et prix moyen.
8. L'application Streamlit relit les transactions filtrées par SQL, recalcule
   `prix_m2`, limite le volume chargé et produit KPIs, carte, histogrammes et
   export CSV.

Preuves : `src/backend/ingest_valeursfoncieres.py`,
`src/backend/load_to_sqlite.py`, `src/backend/load_to_db.py`,
`src/backend/spark_dvf_analysis.py`, `src/app/streamlit_app.py`.

### 5.2 Indicateur INSEE — exemple du revenu

1. Le CSV FILOSOFI 2021 est filtré sur la mesure `MED_SL`, l'unité annuelle en
   euros, le statut final et la période 2021.
2. Le code commune est transformé en code département, avec traitement des DOM
   et de la Corse.
3. La médiane des valeurs communales est calculée par département.
4. Le résultat est écrit en CSV et remplace une table SQLite.
5. Les fichiers Parquet dérivés alimentent directement les vues socio-économiques ;
   une autre agrégation fusionne les indicateurs au niveau régional.

Preuves : `src/backend/ingest_insee_income.py`, `scripts/csv_to_parquet.py`,
`src/backend/aggregate_by_region.py`, `src/app/streamlit_app.py`.

Point méthodologique à défendre : la « médiane des médianes communales » n'est pas
nécessairement la médiane départementale pondérée par population. C'est une
approximation et non un indicateur officiel équivalent
(`src/backend/ingest_insee_income.py`).

### 5.3 Indice de prix périodique

1. Le worker effectue une requête HTTP vers une page INSEE.
2. Le scraper note les tableaux HTML par heuristiques, choisit les meilleurs
   tableaux « indices » et « variation annuelle », détecte la dernière période
   trimestrielle et produit des `PricePoint`.
3. Des règles de qualité rejettent UID/périodes vides, périodes mal formées,
   valeurs non finies ou hors bornes et URL absentes.
4. SQLite reçoit un upsert dans `realtime_price_latest`. Une ligne d'historique
   n'est ajoutée que si la période ou la valeur change.
5. MongoDB conserve le debug brut du run, les observations idempotentes par
   `(metric_uid, scraped_at)` et le dernier état par `metric_uid`.
6. Un journal `realtime_price_runs` est écrit même en cas d'erreur.
7. Le worker recommence après l'intervalle configuré ; la page Streamlit se
   rafraîchit et présente l'état, les runs et l'historique.

Preuves : `src/realtime_price/insee_scraper.py`,
`src/realtime_price/worker.py`, `src/realtime_price/sqlite_store.py`,
`src/realtime_price/mongo_store.py`,
`src/app/pages/06_Temps_reel_prix.py`.

## 6. Composants principaux

| Composant | Rôle | État constaté |
|---|---|---|
| Ingestions DVF/INSEE | nettoyage et production CSV/SQLite | plusieurs scripts autonomes, conventions hétérogènes |
| Chargeurs SQL | SQLite principal ; ancien chemin PostgreSQL | le chemin PostgreSQL n'est pas intégré au Compose |
| Spark DVF | pré-agrégation départementale | cluster Docker 1 master + 2 workers ; petite sortie rapatriée avec `toPandas()` |
| Agrégation régionale | jointure DVF + indicateurs + référentiel géographique | contrat canonique ; 13 régions, Corse incluse |
| Export SQLite→Mongo | miroir par collection | destructif pour les collections cibles : `drop()` puis réinsertion |
| Worker temps réel | scraping périodique, DQ, persistance double | le sous-système le plus structuré et instrumenté |
| Dashboard principal | six vues analytiques | mélange SQLite, Parquet, GeoJSON et TinyDB |
| Page temps réel | état et historique des indices | mode mock activé par défaut dans l'UI |
| Metabase | BI optionnelle | service présent, configuration métier non versionnée |
| Tests/qualité code | pytest, Black, Ruff, mypy, detect-secrets | couverture fonctionnelle très limitée |

Preuves : `src/`, `infra/docker-compose.yml`, `tests/test_ingestion.py`,
`.pre-commit-config.yaml`.

## 7. Technologies et rôle exact

- **Python 3.11** : langage de tous les pipelines et de l'application
  (`Dockerfile`, `pyproject.toml`).
- **pandas** : ingestion, nettoyage, jointures et agrégations en mémoire
  (`src/backend/ingest_*.py`, `src/backend/aggregate_by_region.py`).
- **PySpark** : lecture/typage/agrégation départementale du CSV DVF
  (`src/backend/spark_dvf_analysis.py`).
- **SQLite** : `homepedia.db` sert l'analytique ; `realtime_price.db` isole les
  tables du worker périodique (`src/realtime_price/sqlite_store.py`).
- **SQLAlchemy** : définition/chargement de tables SQL et introspection ERD
  (`src/backend/load_to_sqlite.py`, `src/backend/load_to_db.py`,
  `scripts/generate_erd.py`).
- **MongoDB 7 / PyMongo** : miroir documentaire et tampon du temps réel
  (`infra/docker-compose.yml`, `src/realtime_price/mongo_store.py`).
- **Streamlit** : dashboard et pages interactives
  (`src/app/streamlit_app.py`, `src/app/pages/06_Temps_reel_prix.py`).
- **GeoPandas, Folium** : jointures géographiques et choroplèthes.
- **Matplotlib, Seaborn, SciPy** : graphiques, corrélations et régression.
- **Parquet/PyArrow** : fichiers analytiques compacts pour les indicateurs.
- **TinyDB, TextBlob, WordCloud** : démonstration NoSQL/NLP annexe sur des avis.
- **Requests, BeautifulSoup** : scraping HTTP/HTML.
- **Docker Compose** : app, worker, MongoDB, Metabase, master Spark, deux workers
  et job `spark-submit` optionnel.
- **Black, Ruff, mypy, detect-secrets, pytest** : hygiène de code et tests.

Preuves : `requirements.txt`, `Dockerfile`, `infra/docker-compose.yml`,
`src/app/streamlit_app.py`, `.pre-commit-config.yaml`.

## 8. Décisions techniques identifiables

1. **SQLite pour la portabilité locale.** Cohérent avec un projet scolaire et une
   démonstration sans serveur SQL, mais limité pour les écritures concurrentes et
   le scale-out (`README.md`, `infra/docker-compose.yml`).
2. **Pré-agréger avec Spark.** Réduit le travail interactif de l'UI. Le job
   exploite un cluster standalone Docker de deux workers et ne rapatrie dans
   pandas que la petite sortie agrégée (`src/backend/spark_dvf_analysis.py`).
3. **Parquet pour les indicateurs.** Bon format colonne pour lecture analytique,
   compression et typage ; ici les fichiers sont petits, donc le gain est surtout
   pédagogique (`scripts/csv_to_parquet.py`, `src/app/streamlit_app.py`).
4. **Double persistance du temps réel.** SQLite sert l'UI locale et Mongo garde
   brut/observations/latest ; cela améliore la démonstration polyglotte mais crée
   une double écriture sans transaction distribuée
   (`src/realtime_price/worker.py`).
5. **Historique seulement sur changement.** Réduit le volume et rend les
   changements significatifs ; les runs séparés conservent la preuve de collecte
   (`src/realtime_price/sqlite_store.py`).
6. **Idempotence renforcée.** Les upserts et index uniques protègent Mongo ; le
   chargement DVF nettoie, dédoublonne puis remplace la table pour rendre le rejeu
   du même batch idempotent (`src/realtime_price/mongo_store.py`,
   `src/backend/load_to_sqlite.py`).
7. **Cache et limites côté UI.** Les requêtes sont filtrées et plafonnées pour
   éviter de charger 1 884 593 lignes dans Streamlit
   (`src/app/streamlit_app.py`).

## 9. Forces

- cas d'usage concret et sources publiques traçables ;
- pipeline complet de l'ingestion à la visualisation ;
- plusieurs grains géographiques et formats de données ;
- volume DVF crédible pour discuter performance ;
- séparation latest/history/runs bien pensée dans le temps réel ;
- contrôles DQ explicites et journalisation des erreurs du worker ;
- index SQL et cache applicatif sur des chemins importants ;
- Docker Compose rend les services reproductibles en théorie ;
- données et résultats locaux permettent une démonstration hors recalcul complet.

Preuves : `src/realtime_price/worker.py`, `src/realtime_price/sqlite_store.py`,
`src/backend/setup_indexes.py`, `infra/docker-compose.yml`,
`data/homepedia.db`.

## 10. Limites, risques et incohérences

### Critiques

1. **Contrats de table divergents.** Le code, les tests, l'ERD, le Makefile et la
   base ne nomment pas les mêmes tables/colonnes. Plusieurs vues devraient échouer
   sur la base locale actuelle (`src/app/streamlit_app.py`,
   `src/backend/aggregate_by_region.py`, `tests/test_ingestion.py`,
   `docs/homepedia_erd.dot`, `data/homepedia.db`).
2. **Dépendances manquantes.** `pynsee`, `psycopg2`, `lxml` et
   `streamlit-autorefresh` sont importés/utilisés mais non déclarés explicitement
   dans `requirements.txt`. `altair` est importé facultativement et peut arriver
   transitivement (`src/backend/ingest_insee_region.py`,
   `src/backend/load_to_db.py`, `src/backend/scraper_insee_region.py`,
   `src/app/pages/06_Temps_reel_prix.py`).
3. **Environnements virtuels non portables.** Les deux interpréteurs locaux
   `.venv` et `venv` pointent vers un Python absent (`C:\Python311\python.exe`) ;
   les commandes Python locales ne sont donc pas immédiatement vérifiables
   (`.venv/`, `venv/`, constat du 30/07/2026).
4. **Healthcheck probablement inopérant.** Compose vérifie `/healthz`, alors que
   la cible `make health` utilise `/_stcore/health`. De plus, `wget` est installé
   dans l'étage `builder` mais pas dans l'image `runtime` qui exécute le
   healthcheck (`Dockerfile`, `infra/docker-compose.yml`, `infra/Makefile`).

### Importantes

5. `run_streamlit.ps1` contient un chemin utilisateur absolu obsolète.
6. Le cluster Spark reste mono-machine et lit un CSV local non partitionné : il
   démontre la distribution des calculs sans fournir la résilience d'un cluster
   de production.
7. Les agrégats régionaux de revenu, chômage et pauvreté utilisent encore des
   résumés de résumés ; leur pondération et leur interprétation doivent être
   expliquées avec prudence.
8. Le contrat commun réduit les divergences de noms, mais toute évolution doit
   rester accompagnée de tests de contrat et d'une migration explicite.
9. Le batch DVF est idempotent pour le rejeu du même jeu, mais le remplacement
   complet doit être sécurisé contre une interruption pendant l'écriture.
10. L'export global SQLite→Mongo supprime les collections avant réinsertion ; une
    panne intermédiaire laisse un miroir partiel.
11. SQLite et Mongo sont écrits séquentiellement par le worker : si SQLite réussit
    et Mongo échoue, les stores divergent.
12. Le scraping dépend de la structure HTML et d'heuristiques ; un changement de
    page peut produire zéro métrique ou une mauvaise sélection.
13. Le worker avale les exceptions après les avoir journalisées puis attend le
    prochain cycle ; il n'y a ni backoff exponentiel, ni alerte externe, ni
    dead-letter queue.
14. Le mode mock de la page temps réel est activé par défaut et ajoute du bruit
    uniquement visuel : il faut l'annoncer clairement en démonstration.
15. La base locale contient des dates de collecte jusqu'au 27 mars 2026, futures
    par rapport aux derniers commits de janvier 2026 mais antérieures à
    l'inspection ; l'origine exacte de cette exécution doit être confirmée.

Preuves : fichiers cités ci-dessus, plus `src/realtime_price/config.py`,
`src/realtime_price/insee_scraper.py`, `src/realtime_price/worker.py`,
`src/etl/sqlite_to_mongo_all_tables.py`,
`src/app/pages/06_Temps_reel_prix.py`.

### Qualité et sécurité

- Les tests couvrent seulement la présence de quatre CSV, un seuil de lignes et
  l'absence de NULL ; ils dépendent d'une base réelle et ne testent ni le scraper,
  ni DQ/idempotence, ni l'UI, ni les agrégats (`tests/test_ingestion.py`).
- Aucune CI versionnée n'a été trouvée ; les hooks locaux existent.
- Aucun secret n'est lu ici. `.env`, `kaggle.json` et les dumps sont ignorés par
  Git, et detect-secrets est configuré, ce qui est positif
  (`.gitignore`, `.pre-commit-config.yaml`).
- MongoDB est exposé sur le port 27017 sans authentification configurée dans le
  Compose ; acceptable uniquement sur une machine isolée de démonstration
  (`infra/docker-compose.yml`).
- Aucun contrôle d'accès applicatif n'est implémenté malgré des variables
  d'authentification présentes dans `.env` (noms inspectés, valeurs non lues).

## 11. Améliorations possibles, par priorité

1. Définir un contrat canonique de tables/colonnes et une migration versionnée.
2. Aligner le code courant, les tests, l'ERD, le README et le Makefile sur ce
   contrat.
3. Verrouiller et compléter les dépendances ; reconstruire un environnement propre.
4. Ajouter des tests unitaires du parsing, de la normalisation Corse/DOM, de la DQ,
   de l'idempotence et des agrégations, avec petits fixtures synthétiques.
5. Rendre les batchs idempotents : staging, clé naturelle/hash, `replace` contrôlé
   ou upsert transactionnel.
6. Remplacer la double écriture temps réel par un journal canonique/outbox, ou
   accepter explicitement la cohérence éventuelle et ajouter une réconciliation.
7. Partitionner le Parquet DVF par année/département et lire les colonnes utiles.
8. Pour une vraie montée en charge, déplacer les faits vers PostgreSQL/ClickHouse
   ou un data lake, et exécuter Spark sur stockage distribué.
9. Ajouter retries avec backoff, métriques, alertes et seuil de fraîcheur.
10. Désactiver le mock par défaut hors profil de démonstration.
11. Épingler les versions d'images Docker au lieu de `latest`.
12. Ajouter CI, documentation d'exploitation et sauvegarde/restauration testée.

## 12. Éléments absents ou à confirmer

- **Confirmé** : la branche de référence est la plus récente localement,
  `feat/realtime-dq-dedup`.
- La base locale provient de l'ancien PC portable. Le commit ou pipeline exact
  qui l'a produite reste inconnu ; cela explique plausiblement son décalage avec
  le code actuel, sans démontrer quelle branche l'a générée.
- La réduction de plus de 3,9 M de lignes doit être présentée comme une correction
  de qualité mesurée, conduisant à 1 884 593 transactions uniques.
- Le schéma canonique est documenté dans `docs/DATA_CONTRACT.md` et emploie les
  noms français partagés par les quatre consommateurs.
- **Confirmé dans le périmètre de soutenance** : MongoDB, Metabase, PostgreSQL,
  SeLoger et l'analyse d'avis. Leur présence dans le discours ne doit toutefois
  pas être confondue avec une intégration complète dans la branche courante.
- RabbitMQ et l'authentification restent à clarifier : leurs variables existent,
  mais leur usage n'est pas démontré dans le code.
- Metabase possède-t-il des dashboards exportables/non versionnés ?
- Quelles métriques de succès métier étaient attendues ?
- Quelle machine et quel environnement seront disponibles le jour de l'oral ?
- Les droits de réutilisation et conditions d'accès du scraping ont-ils été
  vérifiés ?
- Aucune preuve de déploiement cloud, orchestration externe, notebook, catalogue de
  données, lineage, sauvegarde ou plan de reprise n'est présente.

## 13. Fichiers les plus importants

| Fichier | Pourquoi il est central |
|---|---|
| `README.md` | intention et procédure historique |
| `infra/docker-compose.yml` | topologie de l'application, du temps réel, des stockages, de Metabase et de Spark |
| `infra/Makefile` / `infra/make.ps1` | orchestration manuelle batch et temps réel |
| `src/backend/ingest_valeursfoncieres.py` | entrée du pipeline DVF |
| `src/backend/ingest_insee_*.py` | transformations des indicateurs |
| `src/backend/load_to_sqlite.py` | création/chargement SQL historique |
| `src/backend/spark_dvf_analysis.py` | usage réel de Spark |
| `src/backend/aggregate_by_region.py` | jointure géographique et agrégation multi-source |
| `src/realtime_price/insee_scraper.py` | extraction HTML robuste par heuristiques |
| `src/realtime_price/worker.py` | orchestration, DQ et double persistance |
| `src/realtime_price/sqlite_store.py` | schéma latest/history/runs et déduplication |
| `src/realtime_price/mongo_store.py` | index uniques et upserts Mongo |
| `src/app/streamlit_app.py` | consommation batch et expérience utilisateur |
| `src/app/pages/06_Temps_reel_prix.py` | observabilité et visualisation temps réel |
| `tests/test_ingestion.py` | niveau de test réellement démontré |
| `data/homepedia.db` | état matériel de la donnée locale |

## 14. Positionnement conseillé à l'oral

Présenter Homepedia comme un **prototype analytique Big Data local et
reproductible**, pas comme une plateforme distribuée de production. La valeur
technique est la chaîne complète, le nettoyage de 1 884 593 transactions DVF
uniques, la pré-agrégation sur un cluster Spark Docker, la visualisation
géospatiale cohérente et l'extension de collecte périodique avec DQ/idempotence.
Reconnaître les limites mono-machine du cluster, la cohérence éventuelle de la
double écriture et les limites de SQLite démontre plus de maîtrise que de
survendre la solution.

## 15. Consolidation fonctionnelle du 22 août 2026

- **Qualité DVF** : plus de 3,9 M de lignes dupliquées ou aberrantes supprimées ;
  mutations symboliques à 1 € exclues ; codes postaux et prix au m² recalculés ;
  chargement idempotent.
- **Agrégations** : 94 départements et 13 régions, Corse incluse ; agrégation
  régionale construite depuis la pré-agrégation départementale pour éviter de
  rescanner les faits.
- **Contrat** : noms canoniques français partagés par SQLite, MongoDB, Streamlit,
  Metabase et les tests.
- **Interface** : filtres Standard cohérents sur KPI, tables, exports, cartes et
  graphiques ; cas sans données gérés ; requêtes SQL filtrées et échantillons pour
  limiter la mémoire.
- **Socio-économie** : chômage et population régionale appliqués au même ensemble
  de départements dans chaque visualisation ; matrice régionale à cinq variables.
- **Corrélations** : chaque coefficient décrit une relation indépendante ; ils
  n'ont aucune raison de totaliser 1 et ne prouvent pas une causalité.
- **Temps réel** : dates en heure de Paris, suivi renforcé des runs, erreurs et
  changements ; simulation de 30 à 180 points, explicitement non persistée.
- **Fiabilité SQLite** : bases analytique et temps réel séparées, lecture seule
  côté Streamlit, six tentatives, `busy_timeout`, WAL pour le worker et message
  utilisateur sans traceback.
