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
| Problème métier et pitch | Critique | 3 | 12/08/2026 | 1 | 0 | 16/08/2026 | Réponse correcte mais encore à enrichir avec les indicateurs INSEE et les utilisateurs |
| Sources DVF et INSEE | Critique | 3 | 12/08/2026 | 1 | 0 | 16/08/2026 | DVF immobilier, INSEE socio-économique ; préciser les variables |
| Entrées, sorties et utilisateurs | Haute | 3 | 12/08/2026 | 1 | 1 | 16/08/2026 | Bonne distinction utilisateur/développeur ; préciser aussi export, monitoring et maintenance |
| Architecture globale | Critique | 3 | 14/08/2026 | 1 | 0 | 18/08/2026 | Bonne synthèse des couches source, traitement, stockage et présentation |
| Parcours d'une transaction DVF | Critique | 3 | 14/08/2026 | 1 | 1 | 18/08/2026 | Parcours compris ; distinguer CSV, table détaillée et agrégat |
| Parcours d'un indicateur INSEE | Haute | 3 | 14/08/2026 | 1 | 0 | 18/08/2026 | Parcours général correct ; retenir FILOSOFI, code commune→département et Parquet |
| Parcours d'un indice temps réel | Critique | 0 | Jamais | 0 | 0 | J3 | Non évalué |
| Fichiers et composants centraux | Haute | 2 | 14/08/2026 | 0 | 1 | 16/08/2026 | Composants cités, rôles encore à consolider |
| État local et preuves chiffrées | Haute | 0 | Jamais | 0 | 0 | J4 | 5 814 960 transactions constatées |
| Divergence de schéma/branches | Critique | 0 | Jamais | 0 | 0 | J4 | Anglais dans le code, français dans la base |
| Ingestion et nettoyage DVF | Critique | 3 | 12/08/2026 | 1 | 0 | 16/08/2026 | Comprend sélection, transformation et suppression des doublons |
| Idempotence des batchs | Critique | 0 | Jamais | 0 | 0 | J6 | `append` à discuter |
| Ingestion des indicateurs INSEE | Haute | 0 | Jamais | 0 | 0 | J7 | Non évalué |
| Codes géographiques Corse/DOM | Haute | 0 | Jamais | 0 | 0 | J7 | Plusieurs implémentations incohérentes |
| Agrégations et biais statistiques | Haute | 3 | 14/08/2026 | 1 | 0 | 18/08/2026 | Gain de volume compris ; perte du détail communal à préciser |
| Jointures et clés géographiques | Haute | 2 | 14/08/2026 | 0 | 1 | 16/08/2026 | Idée générale correcte ; jointure sur code, pas simplement sur fichiers |
| Usage réel de Spark | Critique | 2 | 12/08/2026 | 0 | 1 | 13/08/2026 | Bonne intuition volume/parallélisation ; préciser qu'aucun cluster n'est démontré |
| `toPandas()` et frontière driver | Haute | 0 | Jamais | 0 | 0 | J8 | Non évalué |
| CSV, Parquet et partitionnement | Haute | 0 | Jamais | 0 | 0 | J8 | Non évalué |
| SQLite : rôle, index, limites | Critique | 3 | 12/08/2026 | 1 | 0 | 16/08/2026 | `homepedia.db`, local, centralisé et portable |
| MongoDB : collections et index | Haute | 2 | 14/08/2026 | 0 | 1 | 16/08/2026 | Rôle général compris, latest/history/raw à préciser |
| Docker Compose et déploiement | Haute | 3 | 14/08/2026 | 1 | 0 | 18/08/2026 | Comprend conteneurs isolés et services |
| Orchestration Make/PowerShell | Moyenne | 0 | Jamais | 0 | 0 | J9 | Pas d'orchestrateur de données |
| Justification SQLite | Critique | 3 | 12/08/2026 | 1 | 0 | 16/08/2026 | Justification locale correcte ; ajouter absence de serveur et limite concurrence |
| Justification MongoDB | Haute | 0 | Jamais | 0 | 0 | J11 | Non évalué |
| Double écriture et cohérence | Critique | 0 | Jamais | 0 | 0 | J11 | Non évalué |
| Justification Spark/pandas | Critique | 2 | 12/08/2026 | 0 | 1 | 13/08/2026 | Comprend le principe, doit distinguer pandas en mémoire et Spark distribué |
| Polling versus vrai streaming | Critique | 3 | 14/08/2026 | 1 | 0 | 18/08/2026 | Batch = données disponibles ; polling = interrogation périodique |
| DQ des `PricePoint` | Haute | 0 | Jamais | 0 | 0 | J13 | Non évalué |
| Latest/history/runs | Critique | 0 | Jamais | 0 | 0 | J13 | Non évalué |
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
| Séances terminées | 3 / 21 |
| Questions évaluées | 36 |
| Moyenne des notes | 7,7 / 10 |
| Notions critiques ≥ 4/5 | 0 |
| Présentations complètes réalisées | 0 |
| Dernière séance | J3 — 14/08/2026 |
