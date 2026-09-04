# Homepedia — Architecture

> Cette architecture décrit le code de la branche courante et distingue l'état
> théorique de l'état constaté dans `data/homepedia.db`.

## 1. Schéma global

```text
                         SOURCES PUBLIQUES / FICHIERS
        +---------------------+-------------------+------------------+
        |                     |                   |                  |
  DVF 2024 texte        INSEE CSV/XLS       GeoJSON France    Page HTML INSEE
  ~461 Mo local         socio-économie       départ./régions   indices de prix
        |                     |                   |                  |
        v                     v                   |          HTTP GET / 300 s
+----------------+   +--------------------+       |                  v
| ETL pandas DVF |   | ETL pandas INSEE   |       |     +----------------------+
| normalisation  |   | filtres/agrégats   |       |     | Worker temps réel    |
+-------+--------+   +---------+----------+       |     | scraper + DQ         |
        |                      |                  |     +----+-------------+---+
        v                      v                  |          |             |
 CSV transactions      CSV + Parquet             |          | SQL         | PyMongo
        |               départementaux            |          v             v
        +----------+-----------+                  |  +-------------+ +-------------+
                   |                              |  | SQLite      | | MongoDB 7   |
          +--------+---------+                    |  | latest      | | raw runs    |
          |                  |                    |  | history     | | observations|
          v                  v                    |  | runs        | | latest      |
  +---------------+   +-------------------+       |  +------+------+ +------+------+
  | Chargement SQL|   | Cluster Spark     |       |         |               |
  | transactions  |   | prix/m² par dept  |       |         |               |
  +-------+-------+   +---------+---------+       |         |               |
          |                     |                 |         |               |
          +----------+----------+-----------------+---------+               |
                     v                                                    |
             +-------------------+       miroir batch optionnel             |
             | SQLite            | ----------------------------------------+
             | homepedia.db      |      drop + insert par collection
             | faits/indicateurs |
             | faits/indicateurs |
             +---------+---------+
                       |
          SQL / pandas | + Parquet / GeoJSON / TinyDB
                       v
             +-------------------+          +-------------------+
             | Streamlit         |          | Metabase          |
             | dashboard + page  |          | BI optionnelle    |
             | temps réel        |          | lit /data en RO   |
             +---------+---------+          +-------------------+
                       |
                       v
                  Navigateur :8501
```

Preuves principales : `infra/docker-compose.yml`, `src/backend/`,
`src/realtime_price/`, `src/app/`.

## 2. Frontières des composants

### Zone batch

Les scripts `src/backend/ingest_*.py` sont des programmes autonomes pilotés par
Make/PowerShell. Il n'existe pas de DAG ni de scheduler versionné. Les fichiers
dans `data/raw/` forment la zone brute ; `data/processed/` est la zone nettoyée ;
SQLite et les Parquet sont les couches de service
(`infra/Makefile`, `infra/make.ps1`).

### Zone analytique

Le job Spark est un batch indépendant. Il lit un CSV complet, calcule les
agrégats distribuables, convertit la sortie agrégée en pandas, puis écrit dans
SQLite. La frontière Spark/SQLite passe donc par le driver Python
(`src/backend/spark_dvf_analysis.py`).

### Zone de collecte périodique

Le worker est un service long vivant. Il encapsule :

- l'adaptateur source : `insee_scraper.py` ;
- les règles DQ et l'orchestration : `worker.py` ;
- les adaptateurs de stockage : `sqlite_store.py` et `mongo_store.py`.

Cette séparation est plus nette que dans le pipeline batch.

### Zone de présentation

Le dashboard principal lit `homepedia.db` en mode lecture seule avec retry. La
page temps réel lit `realtime_price.db`, une petite base séparée du fichier
analytique de 1,8 Go ; le worker écrit aussi dans Mongo. Cette séparation évite
qu'une collecte périodique perturbe les lectures DVF sous Docker Desktop.
Metabase monte `data/` en lecture seule, mais aucun dashboard Metabase n'est
versionné (`src/app/streamlit_app.py`,
`src/app/pages/06_Temps_reel_prix.py`, `infra/docker-compose.yml`).

## 3. Flux de données

### Flux A — DVF batch

```text
ValeursFoncieres-2024.txt
  -> pandas : sélection/nettoyage/dédoublonnage
  -> transactions_2024.csv
  -> SQLite.transactions
  -> Streamlit : SQL filtré, LIMIT, calcul prix_m2
  -> KPI / CSV / histogrammes / carte
```

Un flux parallèle relit le CSV avec Spark :

```text
transactions_2024.csv
  -> Spark : cast, surface > 0, prix_m2, groupBy(dept)
  -> toPandas()
  -> table agrégée SQLite
  -> vue Spark Streamlit
```

Le job écrit la table canonique `analyse_departementale`, conforme au contrat
partagé par SQLite, MongoDB, Streamlit et Metabase.

### Flux B — INSEE socio-économique

```text
CSV/XLS INSEE
  -> filtres métier + normalisation code département
  -> agrégation départementale
  -> CSV / Parquet / tables SQLite
  -> jointure code département ou département->région
  -> cartes, distributions, corrélations
```

Il n'existe aucune clé étrangère SQL : les relations sont conventionnelles sur
`code`, `dept` et `code_region` (`docs/homepedia_erd.dot`,
`data/homepedia.db`).

### Flux C — temps réel par polling

```text
tick worker
  -> GET HTTPS page INSEE
  -> parse HTML + score des tables
  -> PricePoint[]
  -> contrôle DQ
  -> realtime_price.db : latest upsert
  -> realtime_price.db : history si période/valeur change
  -> Mongo raw/observations/latest
  -> journal du run SQLite même si erreur
  -> sleep(max(5, intervalle))
```

Le mot juste à l'oral est « ingestion périodique en micro-batch ».

### Flux D — miroir Mongo

`sqlite_to_mongo_all_tables.py` découvre les tables SQLite, supprime les
collections orphelines, remplace chaque collection et insère par lots de 5 000.
Ce flux est un snapshot complet, pas une CDC
(`src/etl/sqlite_to_mongo_all_tables.py`).

## 4. Protocoles et interfaces

| Liaison | Protocole/interface |
|---|---|
| Worker → INSEE | HTTPS GET, timeout configurable |
| App/worker → SQLite | API locale `sqlite3`, fichier monté |
| Worker/app → Mongo | protocole MongoDB via PyMongo |
| Navigateur → Streamlit | HTTP sur port 8501 |
| Navigateur → Metabase | HTTP sur port 3000 |
| Outils → conteneurs | Docker Compose CLI |
| ETL → fichiers | filesystem local, CSV/Excel/Parquet/GeoJSON |

Kafka, Hadoop HDFS, RPC, RabbitMQ, REST métier et gRPC ne sont pas utilisés.

## 5. Stockage et modèle

### SQLite

État constaté et consolidé les 21–22/08/2026 :

| Table locale | Lignes | Rôle |
|---|---:|---|
| `transactions` | 1 884 593 | faits DVF nettoyés et dédoublonnés |
| `analyse_departementale` | 94 | agrégats prix départementaux |
| `analyse_regionale` | 13 | agrégats multi-sources régionaux |
| `population` | 100 | population départementale |
| `revenus` | 103 | revenu médian |
| `chomage` | 100 | chômage |
| `pauvrete` | 97 | pauvreté |
| `realtime_price_latest` | 7 | dernier état par métrique |
| `realtime_price_history` | 16 765 | changements de métriques |
| `realtime_price_runs` | 5 400 | exécutions et erreurs |

Sources : `data/homepedia.db` pour l'analytique et
`data/realtime_price.db` pour le flux périodique. Streamlit ouvre les deux en
lecture seule avec six tentatives et un `busy_timeout` de 30 secondes.

Indexes constatés : date, code postal, commune et index composite
date/type/prix sur les transactions ; indexes temps réel sur métrique et
horodatage. L'absence de clés étrangères rend les jointures souples, mais ne
garantit pas l'intégrité référentielle (`data/homepedia.db`,
`src/backend/setup_indexes.py`).

### MongoDB

- `realtime_price_raw_runs` : debug brut par run ;
- `realtime_price_observations` : observation par métrique et scrape ;
- `realtime_price_latest` : dernier état ;
- collections issues du miroir SQLite.

Les index uniques rendent les upserts idempotents sur un identifiant logique
(`src/realtime_price/mongo_store.py`).

### Fichiers

- CSV : interchange simple mais lecture colonne/scan coûteux ;
- Parquet Snappy : colonnes typées, compressées et adaptées à l'analytique ;
- GeoJSON : géométrie pour Folium/GeoPandas ;
- TinyDB JSON : démonstration documentaire locale ;
- PNG/HTML : sorties d'analyse.

## 6. Traitement

### Batch pandas

Approprié aux indicateurs de quelques dizaines de Mo et au prototypage. La lecture
du DVF brut entier en pandas peut saturer la RAM car aucun `chunksize` n'est
utilisé (`src/backend/ingest_valeursfoncieres.py`).

### Batch Spark

L'agrégation `groupBy` s'exécute sur un cluster Spark standalone Docker composé
d'un master et de deux workers. Le job normalise les codes postaux, traite la
Corse en `2A`/`2B`, exclut les départements invalides ainsi que les prix au m²
hors plage, puis produit 94 agrégats départementaux. La source reste un CSV
local non partitionné et la sortie est collectée avec `toPandas()`.

`toPandas()` est acceptable ici puisque la sortie attendue est d'environ cent
lignes, mais ne le serait pas sur une sortie de grande cardinalité
(`src/backend/spark_dvf_analysis.py`).

### Temps réel

Chaque run est indépendant, mais la séquence de double écriture ne forme pas une
transaction atomique. Le contrôle DQ empêche certains enregistrements invalides,
et la séparation `latest/history/runs` équilibre état courant, audit et volume
(`src/realtime_price/worker.py`).

## 7. Orchestration

L'orchestration est locale et impérative :

- `etl-all` enchaîne INSEE, DVF, Spark, agrégation et export Mongo ;
- `rt-up` démarre le worker ;
- `rt-scrape-now` lance un run unique ;
- Docker redémarre les services avec `restart: unless-stopped`.

Il n'y a ni Airflow, ni Dagster, ni cron versionné, ni gestion formelle de
lineage/dependencies/retry (`infra/Makefile`, `infra/make.ps1`).

## 8. Déploiement

Le `Dockerfile` multi-stage construit une image Python 3.11 et lance Streamlit.
Compose déploie :

```text
app             image Homepedia, port 8501, data/src/outputs montés
realtime-price  même image, commande worker, dépend de mongo
mongo           mongo:7, volume local, port 27017 exposé
metabase        metabase:latest, port 3000, data en lecture seule
spark-master    coordinateur Spark standalone, ports 7077/8080
spark-worker-1  exécuteur Spark, 1 cœur et 1 Gio
spark-worker-2  exécuteur Spark, 1 cœur et 1 Gio
spark-submit    job ponctuel via le profil tools
```

Limites :

- l'image `homepedia-app:latest` doit être construite avant le worker ;
- `latest` rend Metabase non reproductible ;
- `src` est monté en écriture, ce qui masque la copie faite dans l'image ;
- le healthcheck `/healthz` semble incohérent avec `/_stcore/health`, et son
  binaire `wget` n'est installé que dans l'étage de build, pas le runtime ;
- aucun déploiement cloud ou secret manager n'est versionné.

Preuves : `Dockerfile`, `infra/docker-compose.yml`.

## 9. Dépendances externes

- disponibilité et stabilité des fichiers/schémas DGFiP-INSEE ;
- structure HTML de la page INSEE temps réel ;
- images Docker Python, MongoDB et Metabase ;
- bibliothèques géospatiales natives GDAL/Fiona ;
- Java requis par PySpark ;
- espace disque pour SQLite, Mongo et fichiers ;
- connexion réseau lors du scraping.

## 10. Points de panne et mécanismes de reprise

| Panne | Effet | Reprise actuelle | Manque |
|---|---|---|---|
| fichier brut absent/schema modifié | batch échoue | relance manuelle après correction | validation de contrat en amont |
| mémoire insuffisante pandas | ingestion DVF échoue | aucune automatique | lecture par chunks/Parquet |
| job Spark échoue | agrégat absent/ancien | relance de cible Make | checkpoint/monitoring |
| SQLite momentanément indisponible | message utilisateur sans traceback | six retries, lecture seule, `busy_timeout` | supervision Docker |
| concurrence analytique/temps réel | lectures DVF ralenties | bases SQLite séparées, WAL sur la base temps réel | DB serveur en production |
| page INSEE change | zéro point ou erreur | run `error`, prochain poll | alerte, tests de contrat, fallback |
| Mongo indisponible après SQLite | stores divergents | prochain run, upserts partiels | outbox/réconciliation |
| export snapshot interrompu | collections Mongo partielles | relancer export complet | collection temporaire + swap |
| conteneur worker tombe | plus de collecte | restart Docker | alerte de fraîcheur |
| base/fichiers corrompus | service indisponible | aucune procédure documentée | backups/restauration testée |
| noms de tables divergent | vues/ETL échouent | aucune automatique | migration/version de schéma |

## 11. Performance et scalabilité

### Points positifs

- filtres SQL paramétrés et `LIMIT` côté Streamlit ;
- cache Streamlit ;
- index transactionnels ;
- agrégat Spark pré-calculé ;
- Parquet pour les petits indicateurs ;
- insertions Mongo par lots de 5 000 ;
- historique temps réel dédupliqué.

### Goulots d'étranglement

- SQLite avec 1 884 593 transactions uniques : bon en lecture locale, faible
  concurrence d'écriture et pas de scale-out ;
- calcul `valeur_fonciere / surface` à la volée, sans colonne matérialisée ;
- `date(date_mutation)` peut réduire l'utilisation d'un index ;
- `SELECT *` charge plus de colonnes que nécessaire ;
- CSV DVF non partitionné ;
- pandas charge des fichiers entiers ;
- volumes Docker Windows peuvent être plus lents ;
- géométrie régionale d'environ 30 Mo lue par l'UI puis simplifiée.

### Évolution possible

```text
Brut immuable -> object storage / Parquet partitionné
               -> Spark cluster ou moteur SQL colonne
               -> tables curated versionnées
               -> PostgreSQL/ClickHouse pour serving
               -> API/cache pour l'UI
               -> orchestrateur + catalogue + monitoring
```

Pour un vrai streaming, utiliser une source événementielle/CDC et Kafka n'aurait
de sens que si la source produit des événements plus fréquents que les
publications trimestrielles INSEE. Dans le cas actuel, le polling est plus simple
et économiquement cohérent.

## 12. Cohérence et garanties

- **SQLite latest** : upsert par `metric_uid`.
- **SQLite history** : ajout seulement si période ou valeur change.
- **Mongo observations** : unicité `(metric_uid, scraped_at)`.
- **Mongo latest** : unicité `metric_uid`.
- **Runs** : UUID par tentative.
- **Batch DVF** : chargement `replace` contrôlé après nettoyage et dédoublonnage ;
  une relance du même jeu ne recrée pas les doublons.
- **Double stockage** : cohérence éventuelle, aucune transaction distribuée.

## 13. Explication orale en deux minutes

« Homepedia est une chaîne analytique immobilière française en deux parties.

La première est un pipeline batch. Je pars des transactions DVF 2024 et de
plusieurs sources INSEE — population, revenu, chômage et pauvreté. Des scripts
pandas normalisent les colonnes et les codes géographiques, filtrent les valeurs
invalides et produisent des CSV ou Parquet. Les transactions sont chargées dans
SQLite. J'utilise aussi Spark pour calculer le nombre de transactions et le prix
moyen au mètre carré par département sur le volume DVF. La petite sortie agrégée
est ensuite enregistrée dans SQLite.

La deuxième partie est une collecte périodique. Un worker interroge une page
INSEE toutes les cinq minutes par défaut, extrait les indices trimestriels depuis
les tableaux HTML, applique des contrôles de qualité, puis écrit le dernier état,
l'historique des changements et les métriques de chaque run. SQLite sert
l'interface locale ; MongoDB conserve également le brut, les observations et le
dernier état avec des index uniques pour l'idempotence.

Streamlit constitue la couche de présentation. Il combine SQL, Parquet et
GeoJSON pour afficher des filtres, cartes, distributions, corrélations et une
page de suivi du worker. Docker Compose lance l'application, le worker, MongoDB,
Metabase et le cluster Spark standalone composé d'un master et de deux workers.

La solution est volontairement portable et adaptée à un prototype. Ses limites
principales sont SQLite pour la montée en charge, le polling qui n'est pas du vrai
streaming et la double écriture non atomique. Le contrat de données canonique
réduit désormais les dérives de noms entre les composants. En production,
j'ajouterais orchestration et monitoring, et déplacerais les
faits vers un stockage plus scalable. »
# Mise à jour technique du 21 août 2026

Le traitement DVF utilise désormais un cluster Spark standalone conteneurisé :

```text
spark-master:7077
├── spark-worker-1 (1 cœur, 1 Gio)
└── spark-worker-2 (1 cœur, 1 Gio)
```

Le service `spark-submit` exécute `src/backend/spark_dvf_analysis.py` et écrit
la table canonique `analyse_departementale`. L'agrégation régionale consomme
ensuite cette pré-agrégation au lieu de relire toutes les transactions.

Le contrat physique partagé avec Metabase est décrit dans `docs/DATA_CONTRACT.md`.
Les tables canoniques sont en français pour préserver les collections MongoDB
et les analyses Metabase existantes.
