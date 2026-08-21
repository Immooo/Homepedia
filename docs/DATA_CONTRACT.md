# Contrat de données canonique Homepedia

Ce contrat est la référence commune à SQLite, MongoDB, Streamlit, Metabase et aux tests.
Les noms physiques restent en français afin de préserver les collections et tableaux de bord
Metabase existants. Les libellés d'interface peuvent être reformulés librement.

| Jeu de données | Table / collection | Clé géographique | Mesures principales |
|---|---|---|---|
| Transactions DVF | `transactions` | `code_postal`, `commune` | `valeur_fonciere`, `surface_reelle_bati` |
| Population | `population` | `code` | `population` |
| Revenus | `revenus` | `code` | `revenu_median` |
| Chômage | `chomage` | `code` | `taux_chomage` |
| Pauvreté | `pauvrete` | `code` | `taux_pauvrete` |
| Agrégat département | `analyse_departementale` | `dept` | `nb_transactions`, `prix_m2_moyen` |
| Agrégat région | `analyse_regionale` | `code_region` | `nb_transactions`, `prix_m2_moyen`, `population`, `revenu_median`, `taux_chomage`, `taux_pauvrete` |

Les codes départementaux et régionaux sont des chaînes afin de conserver les zéros initiaux
et les codes corses. Les scripts ETL remplacent atomiquement les petites tables de référence.
L'export SQLite vers MongoDB doit conserver exactement ces noms et ces champs.

## Règles qualité DVF

- suppression des doublons exacts avant chargement ;
- chargement SQLite idempotent (`replace`, jamais `append`) ;
- valeur foncière minimale de 1 000 € pour exclure les mutations symboliques ;
- code postal numérique normalisé sur cinq caractères ;
- surface strictement positive pour calculer le prix au m² ;
- prix au m² conservé entre 0 et 20 000 € pour les agrégats de marché ;
- traitement spécifique des départements corses `2A` et `2B`.
