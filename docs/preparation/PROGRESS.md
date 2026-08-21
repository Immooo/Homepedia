# Homepedia — Suivi de progression

> Initialisé le 30/07/2026. Les niveaux sont à `0` car aucune réponse de l'apprenant
> n'a encore été évaluée. Ils ne représentent pas une faiblesse constatée.

## Échelle

| Niveau | Interprétation | Prochaine révision |
|---:|---|---|
| 0 | non évalué | au premier jour prévu |
| 1 | raté malgré un indice | lendemain |
| 2 | fragile, réponse très incomplète | +2 jours |
| 3 | correct avec oublis mineurs | +4 jours |
| 4 | solide et justifié | +7 jours |
| 5 | maîtrisé sous question difficile | +7 jours, puis entretien final |

## Tableau de progression

| Notion | Importance | Niveau 0–5 | Dernière révision | Bonnes réponses | Erreurs | Prochaine révision | Commentaire |
|---|---|---:|---|---:|---:|---|---|
| Problème métier et pitch | Critique | 3 | 21/08/2026 | 2 | 2 | 25/08/2026 | Chaîne complète et Spark cités ; rendre le pitch plus concis et distinguer les sources INSEE fichiers du scraping temps réel |
| Sources DVF et INSEE | Critique | 4 | 21/08/2026 | 2 | 2 | 28/08/2026 | Distingue correctement les fichiers socio-économiques INSEE des indices de prix collectés périodiquement sur la page HTML |
| Entrées, sorties et utilisateurs | Haute | 4 | 21/08/2026 | 5 | 1 | 28/08/2026 | Explique le diagnostic, la correction et la vérification anti-régression du rôle de mainteneur |
| Architecture globale | Critique | 3 | 21/08/2026 | 3 | 1 | 25/08/2026 | Chaîne globale et Spark correctement replacés ; préciser MongoDB comme complément du temps réel et SQLite comme lecture principale de Streamlit |
| Parcours d'une transaction DVF | Critique | 4 | 21/08/2026 | 2 | 1 | 28/08/2026 | Distingue donnée brute et traitée ; consolider les niveaux CSV détaillé, table SQLite et agrégat Spark |
| Parcours d'un indicateur INSEE | Haute | 3 | 14/08/2026 | 1 | 0 | 18/08/2026 | Parcours général correct ; retenir FILOSOFI, code commune→département et Parquet |
| Parcours d'un indice temps réel | Critique | 3 | 20/08/2026 | 1 | 1 | 24/08/2026 | Parcours complet globalement correct : page INSEE → polling/scraper → DQ → SQLite/MongoDB → Streamlit ; dire « périodique », pas « ponctuel » |
| Fichiers et composants centraux | Haute | 3 | 20/08/2026 | 1 | 6 | 24/08/2026 | `worker.py` correctement placé sur le temps réel ; priorité actuelle : maîtriser les rôles avant les noms de fichiers |
| État local et preuves chiffrées | Haute | 2 | 21/08/2026 | 3 | 2 | 23/08/2026 | Identifie `history` comme preuve mais oublie le chiffre ; retenir 16 765 changements ou 5 113 runs |
| Divergence de schéma/branches | Critique | 4 | 21/08/2026 | 5 | 3 | 28/08/2026 | Propose correctement un nommage canonique ; ajouter migrations versionnées et tests de contrat pour l'imposer |
| Ingestion et nettoyage DVF | Critique | 3 | 21/08/2026 | 4 | 0 | 25/08/2026 | Comprend normalisation et dédoublonnage ; relier les doublons au biais sur nombre de ventes et prix moyen |
| Idempotence des batchs | Critique | 4 | 21/08/2026 | 1 | 0 | 28/08/2026 | Comprend qu'un second `append` peut dupliquer les faits et fausser les agrégats |
| Ingestion des indicateurs INSEE | Haute | 0 | Jamais | 0 | 0 | J7 | Non évalué |
| Codes géographiques Corse/DOM | Haute | 0 | Jamais | 0 | 0 | J7 | Plusieurs implémentations incohérentes |
| Agrégations et biais statistiques | Haute | 3 | 21/08/2026 | 4 | 0 | 25/08/2026 | Comprend l'agrégat comme résumé rapide ; dire groupBy département et préciser les mesures calculées plutôt que « données similaires » |
| Jointures et clés géographiques | Haute | 2 | 14/08/2026 | 0 | 1 | 16/08/2026 | Idée générale correcte ; jointure sur code, pas simplement sur fichiers |
| Usage réel de Spark | Critique | 4 | 20/08/2026 | 1 | 1 | 27/08/2026 | Spark correctement associé à la pré-agrégation des transactions DVF ; revoir ensuite session locale et absence de cluster démontré |
| `toPandas()` et frontière driver | Haute | 0 | Jamais | 0 | 0 | J8 | Non évalué |
| CSV, Parquet et partitionnement | Haute | 0 | Jamais | 0 | 0 | J8 | Non évalué |
| SQLite : rôle, index, limites | Critique | 3 | 21/08/2026 | 3 | 0 | 25/08/2026 | Connaît la limite de concurrence ; expliquer les verrous d'écriture et l'absence de scale-out pour un passage à l'échelle |
| MongoDB : collections et index | Haute | 4 | 20/08/2026 | 2 | 2 | 27/08/2026 | Distingue MongoDB de SQLite : Streamlit lit principalement SQLite ; MongoDB complète le temps réel |
| Docker Compose et déploiement | Haute | 3 | 14/08/2026 | 1 | 0 | 18/08/2026 | Comprend conteneurs isolés et services |
| Orchestration Make/PowerShell | Moyenne | 4 | 20/08/2026 | 1 | 0 | 27/08/2026 | Batch correctement identifié comme lancé ponctuellement ; préciser ensuite Make/PowerShell et l'absence de scheduler |
| Justification SQLite | Critique | 3 | 12/08/2026 | 1 | 0 | 16/08/2026 | Justification locale correcte ; ajouter absence de serveur et limite concurrence |
| Justification MongoDB | Haute | 0 | Jamais | 0 | 0 | J11 | Non évalué |
| Double écriture et cohérence | Critique | 4 | 20/08/2026 | 2 | 1 | 27/08/2026 | Propose une réconciliation qui détecte les divergences et rejoue les écritures manquantes |
| Justification Spark/pandas | Critique | 2 | 12/08/2026 | 0 | 1 | 13/08/2026 | Comprend le principe, doit distinguer pandas en mémoire et Spark distribué |
| Polling versus vrai streaming | Critique | 3 | 21/08/2026 | 4 | 3 | 25/08/2026 | Kafka jugé à juste titre disproportionné ; préciser qu'il n'apporte pas de source événementielle à une page HTML consultée périodiquement |
| DQ des `PricePoint` | Haute | 4 | 20/08/2026 | 1 | 0 | 27/08/2026 | Contrôles DQ correctement associés à la présence, au format et à la validité des données avant stockage |
| Latest/history/runs | Critique | 4 | 21/08/2026 | 7 | 2 | 28/08/2026 | Distingue les trois rôles ; préciser que `runs` journalise chaque tentative avec son succès ou son erreur |
| Upsert, unicité, exactly-once | Critique | 0 | Jamais | 0 | 0 | J13 | Garanties partielles |
| Performance et goulots | Critique | 0 | Jamais | 0 | 0 | J14 | Non évalué |
| Scalabilité et architecture cible | Haute | 0 | Jamais | 0 | 0 | J14 | Non évalué |
| Reprise après panne partielle | Critique | 0 | Jamais | 0 | 0 | J16 | Non évalué |
| Qualité des données et tests | Critique | 0 | Jamais | 0 | 0 | J17 | Couverture actuelle faible |
| Observabilité et alertes | Haute | 2 | 21/08/2026 | 0 | 1 | 23/08/2026 | Sait chercher une trace dans MongoDB ; retenir aussi `realtime_price_runs` dans SQLite comme preuve directe, et l'absence d'alertes externes |
| Sécurité et secrets | Haute | 0 | Jamais | 0 | 0 | J18 | Mongo exposé sans auth Compose |
| Corrélation versus causalité | Haute | 0 | Jamais | 0 | 0 | J18 | Non évalué |
| Limites et améliorations | Critique | 0 | Jamais | 0 | 0 | J18 | Non évalué |
| Démonstration et plan B | Critique | 0 | Jamais | 0 | 0 | J19 | Non évalué |
| Présentation complète | Critique | 0 | Jamais | 0 | 0 | J10 | Premier essai à J10 |
| Questions pièges | Haute | 0 | Jamais | 0 | 0 | J20 | Non évalué |

## Règle de mise à jour après chaque réponse

1. Incrémenter **Bonnes réponses** si la note est au moins 7/10, sinon
   incrémenter **Erreurs**.
2. Mettre à jour le niveau :
   - 0–3/10 → niveau 1 ;
   - 4–6/10 → niveau 2 ;
   - 7–8/10 → niveau 3 ;
   - 9/10 → niveau 4 ;
   - 10/10 sur une question difficile, sans indice → niveau 5.
3. Inscrire la date réelle dans **Dernière révision**.
4. Calculer **Prochaine révision** à +1, +2, +4 ou +7 jours.
5. Écrire un commentaire très court et actionnable.

## Indicateurs globaux

| Indicateur | Valeur initiale |
|---|---:|
| Séances terminées | 5 / 21 |
| Questions évaluées | 101 |
| Moyenne des notes | 7,6 / 10 |
| Notions critiques ≥ 4/5 | 8 |
| Présentations complètes réalisées | 0 |
| Dernière séance | J5 — 21/08/2026 |
