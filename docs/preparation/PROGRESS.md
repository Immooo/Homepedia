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
| Architecture globale | Critique | 3 | 21/08/2026 | 3 | 1 | 25/08/2026 | Réapprendre l'état consolidé : cluster Spark, contrat commun et deux bases SQLite |
| Parcours d'une transaction DVF | Critique | 4 | 21/08/2026 | 2 | 1 | 28/08/2026 | Distingue donnée brute et traitée ; consolider les niveaux CSV détaillé, table SQLite et agrégat Spark |
| Parcours d'un indicateur INSEE | Haute | 3 | 21/08/2026 | 2 | 1 | 25/08/2026 | Parcours fichiers→pandas→codes→SQLite compris après confusion initiale avec le worker |
| Parcours d'un indice temps réel | Critique | 3 | 20/08/2026 | 1 | 1 | 24/08/2026 | Parcours complet globalement correct : page INSEE → polling/scraper → DQ → SQLite/MongoDB → Streamlit ; dire « périodique », pas « ponctuel » |
| Fichiers et composants centraux | Haute | 3 | 20/08/2026 | 1 | 6 | 24/08/2026 | `worker.py` correctement placé sur le temps réel ; priorité actuelle : maîtriser les rôles avant les noms de fichiers |
| État local et preuves chiffrées | Haute | 2 | 21/08/2026 | 3 | 2 | 23/08/2026 | Preuves prioritaires : 1 884 593 transactions uniques, 94 départements, 13 régions, 1 master + 2 workers |
| Divergence de schéma/branches | Critique | 4 | 21/08/2026 | 5 | 3 | 28/08/2026 | Propose correctement un nommage canonique ; ajouter migrations versionnées et tests de contrat pour l'imposer |
| Ingestion et nettoyage DVF | Critique | 3 | 21/08/2026 | 12 | 4 | 25/08/2026 | Nettoyage, contrôles, dédoublonnage et prix/m² compris ; revoir parsing date et contrôles finaux |
| Idempotence des batchs | Critique | 4 | 21/08/2026 | 4 | 0 | 28/08/2026 | Définition et risque `append` maîtrisés ; nouveau chargement DVF `replace` contrôlé |
| Ingestion des indicateurs INSEE | Haute | 3 | 21/08/2026 | 2 | 1 | 25/08/2026 | Chaîne batch comprise ; ne pas la confondre avec le scraping périodique |
| Codes géographiques Corse/DOM | Haute | 4 | 21/08/2026 | 3 | 0 | 28/08/2026 | Sait justifier chaînes, `2A`/`2B`, DOM à trois chiffres et zéros initiaux |
| Agrégations et biais statistiques | Haute | 3 | 21/08/2026 | 4 | 0 | 25/08/2026 | Comprend l'agrégat comme résumé rapide ; dire groupBy département et préciser les mesures calculées plutôt que « données similaires » |
| Jointures et clés géographiques | Haute | 2 | 21/08/2026 | 2 | 2 | 23/08/2026 | Jointure sur code normalisé comprise ; revoir le comptage des lignes non appariées |
| Usage réel de Spark | Critique | 4 | 22/08/2026 | 5 | 1 | 29/08/2026 | Explique le cluster Docker 1 master + 2 workers, la pré-agrégation et la limite mono-machine |
| `toPandas()` et frontière driver | Haute | 4 | 22/08/2026 | 1 | 0 | 29/08/2026 | Comprend que seule la sortie agrégée (~94 lignes) revient au driver |
| CSV, Parquet et partitionnement | Haute | 3 | 22/08/2026 | 1 | 1 | 26/08/2026 | Retenir colonnaire/typé/compressé et lecture sélective ; ne pas réduire Parquet au découpage |
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
| Cohérence des filtres UI | Haute | 0 | Jamais | 0 | 0 | J10 | Même périmètre filtré pour KPI, tables, exports, cartes et graphiques |
| Simulation temps réel non persistée | Haute | 0 | Jamais | 0 | 0 | J13 | 30–180 points ; scénarios et bruit ; aucune donnée fictive en base |
| Séparation des bases SQLite | Critique | 0 | Jamais | 0 | 0 | J9 | Analytique et temps réel isolés ; RO UI, retries, WAL et busy_timeout |
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
| Séances terminées | 8 / 21 |
| Questions évaluées | 133 |
| Moyenne des notes | 7,6 / 10 |
| Notions critiques ≥ 4/5 | 8 |
| Présentations complètes réalisées | 0 |
| Dernière séance | J8 — 22/08/2026 |
