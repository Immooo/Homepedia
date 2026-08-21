# Rapport de conformité — sujet T-DAT-902

Date de recette : 21 août 2026.

## Résultat

| Exigence du sujet | Implémentation Homepedia | État |
|---|---|---|
| Données immobilières volumineuses | 1 884 593 transactions DVF uniques et nettoyées dans SQLite | Conforme |
| Indicateurs diversifiés | prix, surface, population, revenus, chômage, pauvreté | Conforme |
| Base relationnelle | SQLite | Conforme |
| Base non relationnelle | MongoDB `homepedia_buffer` | Conforme |
| Standardisation des données | contrat commun documenté dans `docs/DATA_CONTRACT.md` | Conforme |
| Traitement distribué | Spark standalone, un master et deux workers Docker | Conforme |
| Analyse tabulaire et graphique | vues Streamlit et exploration Metabase | Conforme |
| Cartographie | GeoJSON départements et régions dans Streamlit | Conforme |
| Analyse textuelle | jeu annexe Hotel Reviews, sentiments et nuage de mots, clairement séparé du domaine DVF | Partiel / démonstrateur |
| Niveaux géographiques | commune, département et région | Conforme |
| Temps réel (bonus) | polling INSEE toutes les 300 s, contrôle qualité et déduplication | Conforme (bonus) |
| Schéma et nettoyage documentés | architecture, contrat et vue Méthodologie | Conforme |

## Preuves de recette

- `pytest` : 16 tests réussis.
- Streamlit : endpoint `/healthz` valide et conteneur `healthy`.
- Temps réel : 7 points reçus, 7 valides, 0 erreur lors du run contrôlé.
- Spark : master `ALIVE`, 2 workers, 2 cœurs ; application distribuée terminée avec code 0.
- Agrégat Spark : départements métropolitains et ultramarins, avec gestion `2A`/`2B`.
- Agrégat régional : 13 régions métropolitaines dans `analyse_regionale`, Corse incluse.
- MongoDB : les transactions nettoyées et les agrégats ont été synchronisés vers `homepedia_buffer`.
- Metabase : connexion valide, schéma synchronisé et valeurs réanalysées.

## Commandes de démonstration

```powershell
docker compose -f infra/docker-compose.yml up -d
powershell -File infra/make.ps1 etl-spark
python -m pytest -q
```

Interfaces :

- Streamlit : `http://localhost:8501`
- Metabase : `http://localhost:3000`
- Spark master : `http://localhost:8080`

## Limites à annoncer honnêtement

- Le cluster Spark est un cluster multi-conteneurs exécuté sur une seule machine physique.
- SQLite convient à la démonstration analytique locale, mais pas à de nombreuses écritures concurrentes.
- Le flux INSEE utilise du polling micro-batch ; ce n'est pas du streaming événementiel natif.
- Les données DVF sont historiques ; seul le sous-système d'indices INSEE est actualisé périodiquement.
- Le jeu d'analyse textuelle est un démonstrateur NLP hôtelier et ne doit pas être présenté comme une source immobilière DVF.
