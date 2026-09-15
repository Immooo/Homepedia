# Homepedia

Homepedia est une application d'analyse du marché immobilier français. Elle transforme les données publiques **DVF** et **INSEE** en indicateurs départementaux, cartes interactives et tableaux de bord explorables.

> Projet de démonstration et d'analyse : les indicateurs affichés dépendent des jeux de données et de leurs millésimes.

## Fonctionnalités

- Exploration des transactions immobilières par département et type de bien.
- Calcul et visualisation du prix moyen au m², de distributions et de corrélations.
- Croisement avec des indicateurs socio-économiques : revenu médian, chômage, pauvreté et population.
- Cartes choroplèthes départementales et filtres de période, prix et surface.
- Collecte périodique d'un indicateur de prix INSEE, avec historique et contrôles de qualité des données.
- Stockage local dans SQLite et tampon temps réel dans MongoDB.

## Aperçu technique

```text
DVF + INSEE ──> ingestion / normalisation ──> SQLite ──> Streamlit
                                                    │
INSEE (temps réel) ──> worker de collecte ──> MongoDB
```

| Composant | Technologies |
| --- | --- |
| Application | Python 3.11, Streamlit |
| Analyse | pandas, PySpark, GeoPandas, Matplotlib, Folium |
| Données | DVF, INSEE |
| Stockage | SQLite, MongoDB |
| Qualité | pytest, Ruff, Black, mypy, pre-commit |
| Conteneurs | Docker Compose |

## Prérequis

- Python 3.11+
- `pip`
- Optionnel : Docker et Docker Compose pour démarrer l'ensemble des services
- Optionnel : Java, requis par certains traitements PySpark

## Démarrage rapide

Clonez le dépôt, créez un environnement virtuel et installez les dépendances :

```bash
python -m venv .venv

# Windows PowerShell
.\.venv\Scripts\Activate.ps1

# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

Copiez ensuite la configuration d'exemple. Les valeurs par défaut permettent de travailler en local ; ne versionnez jamais votre fichier `.env`.

```bash
cp .env.example .env
```

Sous Windows PowerShell :

```powershell
Copy-Item .env.example .env
```

Lancez l'application :

```bash
streamlit run src/app/streamlit_app.py
```

L'interface est alors disponible sur [http://localhost:8501](http://localhost:8501).

## Préparer les données

Les fichiers de données brutes et la base SQLite ne sont pas inclus dans le dépôt. Exécutez les scripts d'ingestion appropriés après avoir récupéré les sources publiques :

```bash
python src/backend/ingest_valeursfoncieres.py
python src/backend/ingest_insee_population.py
python src/backend/ingest_insee_poverty.py
python src/backend/ingest_insee_unemployment.py
python src/backend/ingest_insee_income.py
python src/backend/spark_dvf_analysis.py
```

Les scripts produisent les données nécessaires dans les répertoires locaux ignorés par Git (`data/` et `outputs/`).

## Exécution avec Docker

Docker Compose démarre l'application Streamlit, le worker temps réel, MongoDB et Metabase :

```bash
docker compose -f infra/docker-compose.yml up --build
```

| Service | Adresse |
| --- | --- |
| Homepedia | [http://localhost:8501](http://localhost:8501) |
| Metabase | [http://localhost:3000](http://localhost:3000) |
| MongoDB | `localhost:27017` |

Les dossiers `data/`, `outputs/` et `mongo_data/` sont montés localement. Ils peuvent contenir des données volumineuses et restent exclus du dépôt.

## Configuration

Les variables principales sont documentées dans [`.env.example`](.env.example). Les paramètres utiles incluent :

| Variable | Rôle | Valeur par défaut |
| --- | --- | --- |
| `DB_PATH` | Chemin de la base SQLite | `/app/data/homepedia.db` (Docker) |
| `MONGO_URI` | URL de connexion MongoDB | `mongodb://mongo:27017` |
| `MONGO_DB` | Base MongoDB utilisée | `homepedia` / `homepedia_buffer` |
| `POLL_INTERVAL_SECONDS` | Fréquence du worker temps réel | `300` |
| `INSEE_SOURCE_URL` | Source INSEE suivie par le worker | statistique INSEE configurée |

Pour un environnement partagé ou de production, fournissez les secrets via le gestionnaire de secrets de votre plateforme plutôt que dans un fichier ou une image Docker.

## Développement et qualité

```bash
pytest
ruff check src scripts
black --check .
mypy src
pre-commit run --all-files
```

## Structure du dépôt

```text
src/
  app/                 # Application Streamlit
  backend/             # Ingestion et préparation des jeux de données
  etl/                 # Chargements SQLite et MongoDB
  realtime_price/      # Collecte périodique et contrôles qualité
scripts/               # Outils de préparation des données
infra/                 # Docker Compose et scripts d'infrastructure
tests/                 # Tests automatisés
docs/                  # Documentation du projet et schémas
```

## Sources de données

- [Demandes de valeurs foncières (DVF)](https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/)
- [INSEE](https://www.insee.fr/)

Les données sont soumises à leurs licences, conditions d'utilisation et millésimes respectifs. Homepedia ne constitue pas un conseil immobilier ou financier.

## Sécurité

Les fichiers de configuration locale, bases de données, archives de données et répertoires de travail sont ignorés par Git. Avant toute contribution, vérifiez que les clés, mots de passe, jetons d'accès et exports de données ne sont pas ajoutés à un commit.

## Licence

Copyright © 2026 Adrien Troise. Tous droits réservés.

Le code source, la documentation, les éléments graphiques et les autres contenus propres au projet **Homepedia** sont protégés par le droit d'auteur.

Sauf autorisation écrite préalable de l'auteur, il est interdit de :

- copier ou reproduire tout ou partie du projet ;
- modifier ou créer une œuvre dérivée à partir du code ;
- redistribuer, publier ou mettre à disposition le code source ;
- utiliser tout ou partie du projet dans un autre projet public ou privé ;
- utiliser le projet ou une partie de celui-ci à des fins commerciales.

La consultation du dépôt et l'exécution du projet à des fins d'évaluation ou de démonstration sont autorisées.

Les jeux de données provenant de **DVF**, de **l'INSEE** ou d'autres sources externes restent soumis à leurs propres licences et conditions d'utilisation.

Pour toute demande de réutilisation, contactez l'auteur.
