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
| Problème métier et pitch | Critique | 2 | 20/08/2026 | 1 | 1 | 22/08/2026 | Valeur immobilière par lieu comprise ; citer aussi les indicateurs INSEE et la visualisation comparative |
| Sources DVF et INSEE | Critique | 3 | 12/08/2026 | 1 | 0 | 16/08/2026 | DVF immobilier, INSEE socio-économique ; préciser les variables |
| Entrées, sorties et utilisateurs | Haute | 4 | 20/08/2026 | 3 | 1 | 27/08/2026 | Distingue clairement pandas, SQLite et Streamlit ; revoir aussi exports et rôles de maintenance |
| Architecture globale | Critique | 3 | 20/08/2026 | 2 | 0 | 24/08/2026 | Chaîne globale correcte ; préciser Spark comme flux DVF parallèle et MongoDB surtout pour le temps réel/mirroring |
| Parcours d'une transaction DVF | Critique | 3 | 14/08/2026 | 1 | 1 | 18/08/2026 | Parcours compris ; distinguer CSV, table détaillée et agrégat |
| Parcours d'un indicateur INSEE | Haute | 3 | 14/08/2026 | 1 | 0 | 18/08/2026 | Parcours général correct ; retenir FILOSOFI, code commune→département et Parquet |
| Parcours d'un indice temps réel | Critique | 0 | Jamais | 0 | 0 | J3 | Non évalué |
| Fichiers et composants centraux | Haute | 2 | 20/08/2026 | 0 | 4 | 22/08/2026 | Fichiers DVF/Spark/realtime repérés avec indice ; mémoriser leurs rôles et latest/history/runs |
| État local et preuves chiffrées | Haute | 4 | 20/08/2026 | 2 | 0 | 27/08/2026 | Retient environ 5,8 millions de transactions et `data/homepedia.db` comme preuve d'exécution locale |
| Divergence de schéma/branches | Critique | 4 | 20/08/2026 | 3 | 2 | 27/08/2026 | Retient contrat de données canonique ; consolider migrations versionnées et tests de contrat |
| Ingestion et nettoyage DVF | Critique | 4 | 20/08/2026 | 2 | 0 | 27/08/2026 | pandas correctement associé aux scripts ETL : extraction, transformation et chargement des données |
| Idempotence des batchs | Critique | 0 | Jamais | 0 | 0 | J6 | `append` à discuter |
| Ingestion des indicateurs INSEE | Haute | 0 | Jamais | 0 | 0 | J7 | Non évalué |
| Codes géographiques Corse/DOM | Haute | 0 | Jamais | 0 | 0 | J7 | Plusieurs implémentations incohérentes |
| Agrégations et biais statistiques | Haute | 4 | 20/08/2026 | 3 | 0 | 27/08/2026 | Comprend le gain de performance et la perte du détail transactionnel après agrégation |
| Jointures et clés géographiques | Haute | 2 | 14/08/2026 | 0 | 1 | 16/08/2026 | Idée générale correcte ; jointure sur code, pas simplement sur fichiers |
| Usage réel de Spark | Critique | 4 | 20/08/2026 | 1 | 1 | 27/08/2026 | Spark correctement associé à la pré-agrégation des transactions DVF ; revoir ensuite session locale et absence de cluster démontré |
| `toPandas()` et frontière driver | Haute | 0 | Jamais | 0 | 0 | J8 | Non évalué |
| CSV, Parquet et partitionnement | Haute | 0 | Jamais | 0 | 0 | J8 | Non évalué |
| SQLite : rôle, index, limites | Critique | 3 | 20/08/2026 | 2 | 0 | 24/08/2026 | Stockage après ETL correctement identifié ; préciser faits, indicateurs, agrégats, temps réel et service direct de l'UI |
| MongoDB : collections et index | Haute | 3 | 20/08/2026 | 1 | 2 | 24/08/2026 | Comprend qu'une panne Mongo n'empêche pas le dashboard SQLite ; consolider les collections raw/observations/latest |
| Docker Compose et déploiement | Haute | 3 | 14/08/2026 | 1 | 0 | 18/08/2026 | Comprend conteneurs isolés et services |
| Orchestration Make/PowerShell | Moyenne | 4 | 20/08/2026 | 1 | 0 | 27/08/2026 | Batch correctement identifié comme lancé ponctuellement ; préciser ensuite Make/PowerShell et l'absence de scheduler |
| Justification SQLite | Critique | 3 | 12/08/2026 | 1 | 0 | 16/08/2026 | Justification locale correcte ; ajouter absence de serveur et limite concurrence |
| Justification MongoDB | Haute | 0 | Jamais | 0 | 0 | J11 | Non évalué |
| Double écriture et cohérence | Critique | 0 | Jamais | 0 | 0 | J11 | Non évalué |
| Justification Spark/pandas | Critique | 2 | 12/08/2026 | 0 | 1 | 13/08/2026 | Comprend le principe, doit distinguer pandas en mémoire et Spark distribué |
| Polling versus vrai streaming | Critique | 2 | 20/08/2026 | 1 | 1 | 22/08/2026 | Distingue local/internet, mais doit nommer batch et polling micro-batch |
| DQ des `PricePoint` | Haute | 0 | Jamais | 0 | 0 | J13 | Non évalué |
| Latest/history/runs | Critique | 4 | 20/08/2026 | 1 | 0 | 27/08/2026 | `latest` correctement associé à la dernière valeur connue ; consolider les trois rôles |
| Upsert, unicité, exactly-once | Critique | 0 | Jamais | 0 | 0 | J13 | Garanties partielles |
| Performance et goulots | Critique | 0 | Jamais | 0 | 0 | J14 | Non évalué |
| Scalabilité et architecture cible | Haute | 0 | Jamais | 0 | 0 | J14 | Non évalué |
| Reprise après panne partielle | Critique | 0 | Jamais | 0 | 0 | J16 | Non évalué |
| Qualité des données et tests | Critique | 0 | Jamais | 0 | 0 | J17 | Couverture actuelle faible |
| Observabilité et alertes | Haute | 0 | Jamais | 0 | 0 | J17 | Logs/runs présents, alertes absentes |
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
| Séances terminées | 4 / 21 |
| Questions évaluées | 60 |
| Moyenne des notes | 7,5 / 10 |
| Notions critiques ≥ 4/5 | 3 |
| Présentations complètes réalisées | 0 |
| Dernière séance | J4 — 20/08/2026 |
