# Homepedia — Programme de préparation sur 21 jours

## Calendrier réorganisé — du 20 août au 3 septembre 2026

Le programme a été recalibré le 20 août après trois séances terminées. Le temps
de travail disponible est réduit du lundi au mercredi à cause de l'entreprise
(9 h–17 h 30), tandis que le jeudi et le vendredi permettent deux blocs de
travail. Homepedia doit aussi partager la période avec un second projet plus
petit.

Rythme retenu :

- priorité à Homepedia jusqu'à la répétition générale, avant de basculer sur le
  second projet ;
- lundi à mercredi : deux heures par jour, pour une séance principale et un
  rappel ciblé, ou exceptionnellement deux blocs courts ;
- jeudi et vendredi : deux blocs possibles, le matin et l'après-midi, en tenant
  compte de la pause déjeuner à domicile de 12 h 30 à 14 h ;
- samedi : une séance légère facultative servant de tampon ;
- dimanche : libre, sans séance indispensable au calendrier ;
- 3 septembre : répétition finale, puis aucune nouvelle notion.

| Date | Séance Homepedia | Charge prévue |
|---|---:|---|
| 20 août, jeudi | J4 vers 14 h, J5 vers 16 h 15 si énergie suffisante | Composants puis révision cumulative |
| 21 août, vendredi | J6 matin, J7 après 14 h | Ingestion DVF puis INSEE/géographie |
| 22 août, samedi | J8 léger, facultatif | Spark et formats ; premier tampon |
| 23 août, dimanche | — | Repos, aucune séance requise |
| 24 août, lundi | J9 | Stockage et déploiement ; rappel J4–J8 dans les deux heures disponibles |
| 25 août, mardi | J10 | Présentation courte et corrections |
| 26 août, mercredi | J11 puis rappel préparatoire J12 | SQLite, MongoDB et double persistance |
| 27 août, jeudi | J12 matin, J13 après 14 h | Spark/pandas/Parquet puis temps réel |
| 28 août, vendredi | J14 matin, J15 après 14 h | Performance puis entretien intermédiaire |
| 29 août, samedi | Tampon facultatif | Rattrapage léger seulement si J8 a été manqué |
| 30 août, dimanche | — | Repos, aucune séance requise |
| 31 août, lundi | J16 | Incidents et reprise |
| 1 septembre, mardi | J17 | Qualité et observabilité |
| 2 septembre, mercredi | J18 et préparation J19 | Sécurité/limites puis construction du récit de démonstration |
| 3 septembre, jeudi | J19 matin, J20 début d'après-midi, J21 fin d'après-midi | Démonstration, entretien blanc et répétition générale |
| 4 septembre, vendredi | Filet de sécurité | Rappel léger seulement si la soutenance a lieu ce jour-là |

Les journées doubles ne fusionnent pas les séances : chaque bloc garde son bilan.
Le jeudi et le vendredi, la pause déjeuner sépare naturellement les blocs. Le
20 août, faute de matinée disponible, une pause d'au moins une heure est prévue
entre J4 et J5. Si la fatigue fait baisser nettement la qualité, le second bloc
devient une révision courte et son contenu est déplacé sur le tampon du samedi ou
sur l'une des plages de deux heures du lundi au mercredi.

## Calendrier accéléré initial — du 12 août au 3 septembre 2026

Le retard est absorbable : il reste 23 jours calendaires pour 21 séances. Le
programme conserve donc toutes ses étapes, avec deux journées de rattrapage. La
date du 4 septembre reste un ultime filet de sécurité si l'oral a lieu ce jour-là.

| Date | Séance | Priorité |
|---|---:|---|
| 12 août | J1 | Mission et périmètre |
| 13 août | J2 | Architecture globale |
| 14 août | J3 | Parcours de la donnée |
| 15 août | J4 | Composants et état réel |
| 16 août | J5 | Révision cumulative 1 |
| 17 août | J6 | Ingestion DVF et qualité batch |
| 18 août | Rattrapage 1 | Refaire une séance manquée ou rappel oral de 20 min |
| 19 août | J7 | INSEE et géographie |
| 20 août | J8 | Spark et formats |
| 21 août | J9 | Stockage, orchestration et déploiement |
| 22 août | J10 | Présentation courte |
| 23 août | J11 | SQLite, MongoDB et double persistance |
| 24 août | J12 | Spark, pandas et Parquet |
| 25 août | Rattrapage 2 | Refaire une séance ou corriger les fragilités J1–J12 |
| 26 août | J13 | Temps réel, idempotence et cohérence |
| 27 août | J14 | Performance, scalabilité et coûts |
| 28 août | J15 | Entretien intermédiaire |
| 29 août | J16 | Incidents et reprise |
| 30 août | J17 | Qualité et observabilité |
| 31 août | J18 | Sécurité, limites et amélioration |
| 1 septembre | J19 | Démonstration et narration finale |
| 2 septembre | J20 | Questions pièges et entretien blanc |
| 3 septembre | J21 | Répétition générale |
| 4 septembre | Filet de sécurité | Rappel léger uniquement, aucun nouveau sujet |

Si une journée est manquée, elle est déplacée sur le prochain rattrapage. Après
le 25 août, deux séances peuvent exceptionnellement être réunies sur un week-end,
mais en deux blocs séparés d'au moins trois heures. On ne supprime jamais J10,
J15, J20 ou J21, car les simulations sont indispensables.

## Principes

Chaque séance dure 45 à 60 minutes et suit ce rythme :

1. **5 min** — rappel libre sans document ;
2. **10 min** — révision des notions arrivées à échéance dans `PROGRESS.md` ;
3. **20 à 30 min** — questions une par une et correction ;
4. **10 min** — exercice oral ;
5. **5 min** — bilan et mise à jour de `PROGRESS.md` et `DAILY_SESSION.md`.

Règles de répétition espacée :

- ratée : revoir le lendemain ;
- fragile : revoir après 2 jours ;
- correcte : revoir après 4 jours ;
- maîtrisée : revoir après 7 jours.

Une « nouvelle notion » n'est ajoutée qu'après le rappel des anciennes. Les jours
5, 10 et 15 n'introduisent pas ou presque pas de nouveau contenu.

## Vue d'ensemble

| Phase | Jours | But |
|---|---:|---|
| Fondations | 1–4 | raconter le projet et son flux principal |
| Révision 1 | 5 | consolider sans nouveau sujet |
| Technique | 6–9 | expliquer code, données, stockage, exécution |
| Révision 2 | 10 | première présentation cohérente |
| Choix | 11–14 | défendre les compromis et alternatives |
| Entretien intermédiaire | 15 | répondre sous pression |
| Incidents/expertise | 16–18 | raisonner sur pannes, DQ, sécurité, scale |
| Finalisation | 19–21 | soutenance et entretien blanc |

## Jour 1 — Mission et périmètre

- **Durée** : 50 min
- **Objectifs** : formuler le problème, les utilisateurs, les sources et les
  résultats sans regarder les notes.
- **Notions révisées** : aucune ; diagnostic initial.
- **Nouvelles notions** : DVF, INSEE, transaction, indicateur, prix au m²,
  département/région.
- **Questions** : 8 à 10, principalement L1.
- **Exercice oral** : pitch de 60 secondes : « Homepedia en une phrase, puis en
  trois idées ».
- **Résultat attendu** : citer les sources et la valeur du projet sans confondre
  données immobilières et indicateurs socio-économiques.

## Jour 2 — Architecture globale

- **Durée** : 55 min
- **Objectifs** : redessiner les composants et leurs frontières.
- **Notions révisées** : mission, sources, sorties (J1).
- **Nouvelles notions** : ETL, stockage, serving, Streamlit, Docker Compose,
  batch versus polling.
- **Questions** : 9 à 11, L1 puis L2 facile.
- **Exercice oral** : reconstruire le schéma ASCII de mémoire en 5 minutes, puis
  l'expliquer.
- **Résultat attendu** : suivre une flèche de la source à l'utilisateur et dire
  où se trouvent pandas, Spark, SQLite et MongoDB.

## Jour 3 — Parcours de la donnée

- **Durée** : 55 min
- **Objectifs** : raconter deux parcours complets.
- **Notions révisées** : architecture et vocabulaire (J2).
- **Nouvelles notions** : zone brute/traitée, normalisation, agrégation, jointure,
  index, cache.
- **Questions** : 10 à 12.
- **Exercice oral** : 2 minutes sur une transaction DVF, puis 90 secondes sur un
  indice INSEE périodique.
- **Résultat attendu** : ne sauter aucune étape entre ingestion, transformation,
  stockage et visualisation.

## Jour 4 — Composants et état réel

- **Durée** : 50 min
- **Objectifs** : associer chaque fichier majeur à son rôle et distinguer
  intention, code courant et base locale.
- **Notions révisées** : J1 à J3 selon échéances.
- **Nouvelles notions** : branches, contrat de données, schéma anglais/français,
  preuve d'exécution locale.
- **Questions** : 10 à 12.
- **Exercice oral** : présenter cinq fichiers centraux et une preuve chiffrée de
  l'exécution.
- **Résultat attendu** : expliquer la divergence de schéma sans inventer sa cause.

## Jour 5 — Révision cumulative 1

- **Durée** : 50 min
- **Objectifs** : consolider les jours 1 à 4.
- **Notions révisées** : toutes les notions dues ; priorité aux erreurs.
- **Nouvelles notions** : aucune.
- **Questions** : 15 courtes, mélangées.
- **Exercice oral** : pitch de 2 minutes sans notes, puis auto-correction avec
  `ARCHITECTURE.md`.
- **Résultat attendu** : au moins 12/15 réponses correctes et un récit fluide.

## Jour 6 — Ingestion DVF et qualité batch

- **Durée** : 55 min
- **Objectifs** : expliquer précisément le code DVF.
- **Notions révisées** : parcours DVF et définitions ETL.
- **Nouvelles notions** : normalisation NFKD, parsing date, dédoublonnage,
  valeurs manquantes, `append`/`replace`, idempotence.
- **Questions** : 10 à 12, surtout L2.
- **Exercice oral** : revue de code sans écran : énumérer entrées, transformations,
  sorties et trois risques.
- **Résultat attendu** : expliquer pourquoi le batch n'est pas globalement
  idempotent.

## Jour 7 — INSEE et géographie

- **Durée** : 55 min
- **Objectifs** : maîtriser revenu, chômage, pauvreté, population et jointures
  géographiques.
- **Notions révisées** : qualité et idempotence J6.
- **Nouvelles notions** : codes INSEE, Corse/DOM, agrégation départementale,
  médiane des médianes, correspondance département-région.
- **Questions** : 10 à 12.
- **Exercice oral** : défendre puis critiquer l'agrégation du revenu.
- **Résultat attendu** : citer les cas géographiques fragiles et distinguer
  moyenne, médiane et somme.

## Jour 8 — Spark et formats

- **Durée** : 55 min
- **Objectifs** : expliquer l'usage réel de Spark et les formats.
- **Notions révisées** : flux DVF et agrégations.
- **Nouvelles notions** : transformations/actions Spark, `groupBy`, master,
  workers, `spark-submit`, driver, `toPandas`, CSV versus Parquet, partitionnement.
- **Questions** : 10 à 12.
- **Exercice oral** : répondre à « Pourquoi Spark ? » en 90 secondes, avec une
  limite et une alternative.
- **Résultat attendu** : décrire exactement le cluster Docker (1 master,
  2 workers) sans le confondre avec trois machines physiques ; savoir quand
  `toPandas()` est sûr et citer les sorties 94 départements/13 régions.

## Jour 9 — Stockage, orchestration et déploiement

- **Durée** : 60 min
- **Objectifs** : relier SQLite, Mongo, volumes et services Docker.
- **Notions révisées** : Spark/formats et idempotence.
- **Nouvelles notions** : séparation `homepedia.db`/`realtime_price.db`, lecture
  seule Streamlit, WAL, `busy_timeout`, retries, collections Mongo, miroir,
  services Spark, Dockerfile multi-stage, Compose, healthcheck et Make.
- **Questions** : 12 à 14.
- **Exercice oral** : expliquer le démarrage de l'application, du worker, de
  MongoDB, Metabase et du cluster Spark, ainsi que le chemin des volumes montés.
- **Résultat attendu** : décrire l'environnement sans confondre orchestration
  Docker et orchestration de données.

## Jour 10 — Révision cumulative 2 et présentation courte

- **Durée** : 60 min
- **Objectifs** : vérifier la chaîne complète et la cohérence du discours.
- **Notions révisées** : J1 à J9 ; aucune nouvelle notion.
- **Nouvelles notions** : aucune.
- **Questions** : 12 ciblées sur les notions dues.
- **Exercice oral** : présentation de 5 minutes chronométrée + 5 minutes de
  questions.
- **Résultat attendu** : structure problème → données → architecture → résultat →
  limites, avec moins de deux oublis majeurs.

## Jour 11 — Justifier SQLite, MongoDB et la double persistance

- **Durée** : 55 min
- **Objectifs** : défendre les stockages par les besoins.
- **Notions révisées** : stockage et index.
- **Nouvelles notions** : source de vérité, cohérence éventuelle, double écriture,
  outbox, réconciliation.
- **Questions** : 10 à 12, L3.
- **Exercice oral** : débat : « MongoDB est-il nécessaire ? » avec pour/contre et
  décision finale.
- **Résultat attendu** : présenter un compromis, pas une liste de slogans.

## Jour 12 — Justifier Spark, pandas et Parquet

- **Durée** : 55 min
- **Objectifs** : défendre le moteur et les formats selon volume et accès.
- **Notions révisées** : Spark J8.
- **Nouvelles notions** : coût de sérialisation, projection, predicate pushdown,
  partition pruning, petites partitions, DuckDB/Polars.
- **Questions** : 10 à 12.
- **Exercice oral** : comparer Spark, pandas chunké et DuckDB pour Homepedia.
- **Résultat attendu** : proposer une alternative crédible sans dévaloriser le
  choix pédagogique Spark.

## Jour 13 — Temps réel, idempotence et cohérence

- **Durée** : 60 min
- **Objectifs** : maîtriser le worker de bout en bout.
- **Notions révisées** : double écriture et DQ.
- **Nouvelles notions** : polling, heure de Paris, fréquence de publication,
  latest/history/runs, upsert, clés uniques, exactly-once versus idempotence,
  simulation de 30 à 180 points et séparation stricte réel/simulé.
- **Questions** : 12 à 14.
- **Exercice oral** : expliquer pourquoi Kafka n'est pas utilisé, puis présenter
  la simulation sans la faire passer pour une donnée persistée ou publiée par
  l'INSEE.
- **Résultat attendu** : employer précisément « micro-batch périodique » et
  décrire les garanties réelles.

## Jour 14 — Performance, scalabilité et coûts

- **Durée** : 55 min
- **Objectifs** : identifier les goulots et une trajectoire d'évolution.
- **Notions révisées** : moteurs, formats, stockage.
- **Nouvelles notions** : scan complet, indexabilité, matérialisation, concurrence,
  scale-up/scale-out, coût opérationnel.
- **Questions** : 10 à 12.
- **Exercice oral** : proposer une architecture pour 10 puis 1 000 utilisateurs.
- **Résultat attendu** : prioriser les changements selon un besoin mesuré.

## Jour 15 — Révision cumulative et entretien intermédiaire

- **Durée** : 60 min
- **Objectifs** : soutenir un entretien semi-directif.
- **Notions révisées** : toutes les échéances J1–J14.
- **Nouvelles notions** : aucune.
- **Questions** : 15 à 18, niveaux 1 à 3, ordre aléatoire.
- **Exercice oral** : 8 minutes de présentation + 12 minutes de questions.
- **Résultat attendu** : moyenne ≥ 7/10, aucune erreur structurante sur
  architecture, Spark ou temps réel.

## Jour 16 — Incidents et reprise

- **Durée** : 55 min
- **Objectifs** : raisonner à partir de symptômes.
- **Notions révisées** : idempotence, cohérence, orchestration.
- **Nouvelles notions** : panne partielle, retry/backoff, reprise batch, collection
  temporaire, outbox.
- **Questions** : 10 scénarios L4.
- **Exercice oral** : incident « SQLite réussi, Mongo en panne » : diagnostic,
  impact, reprise, prévention.
- **Résultat attendu** : séparer détection, mitigation, correction et prévention.

## Jour 17 — Qualité des données et observabilité

- **Durée** : 55 min
- **Objectifs** : construire des contrôles et alertes utiles.
- **Notions révisées** : DQ du worker et géographie.
- **Nouvelles notions** : contrat commun, cohérence des filtres entre vues,
  résultat vide, fraîcheur, complétude, unicité, distribution,
  métriques/alertes et lineage.
- **Questions** : 10 à 12.
- **Exercice oral** : concevoir un tableau de bord d'exploitation avec cinq
  métriques et deux alertes.
- **Résultat attendu** : relier chaque métrique à un risque concret.

## Jour 18 — Sécurité, limites scientifiques et architecture améliorée

- **Durée** : 60 min
- **Objectifs** : traiter les angles morts.
- **Notions révisées** : stockage, déploiement, corrélations.
- **Nouvelles notions** : moindre privilège, réseau Docker, secrets, sauvegarde,
  corrélation/causalité, biais d'agrégation.
- **Questions** : 12 à 14.
- **Exercice oral** : présenter trois faiblesses sans dénigrer le projet, puis
  trois améliorations priorisées.
- **Résultat attendu** : discours honnête, technique et orienté solution.

## Jour 19 — Démonstration et narration finale

- **Durée** : 60 min
- **Objectifs** : construire le fil de soutenance et le plan B.
- **Notions révisées** : toutes les notions fragiles.
- **Nouvelles notions** : conduite de démonstration, transitions, preuve,
  fallback hors ligne.
- **Questions** : 8 à 10 après la présentation.
- **Exercice oral** : soutenance de 10 minutes avec démonstration simulée ;
  annoncer explicitement le mode mock s'il est utilisé.
- **Résultat attendu** : scénario reproductible, données préchargées, captures ou
  résultats de secours, aucune dépendance à un recalcul lourd.

## Jour 20 — Questions pièges et entretien blanc

- **Durée** : 60 min
- **Objectifs** : résister aux formulations agressives et aux faux présupposés.
- **Notions révisées** : erreurs des jours 15–19.
- **Nouvelles notions** : technique de réponse « fait → limite → choix →
  amélioration ».
- **Questions** : 18 à 22, dont les 10 questions pièges.
- **Exercice oral** : entretien blanc de 25 minutes, sans document.
- **Résultat attendu** : demander/préciser quand une prémisse est fausse, ne rien
  inventer, notes ≥ 8/10 sur les questions prioritaires.

## Jour 21 — Répétition générale

- **Durée** : 60 min
- **Objectifs** : valider l'autonomie complète.
- **Notions révisées** : uniquement les dernières fragilités et questions ★.
- **Nouvelles notions** : aucune.
- **Questions** : 12 à 15 après la présentation.
- **Exercice oral** : soutenance complète chronométrée, démonstration, questions,
  conclusion en 30 secondes.
- **Résultat attendu** :
  - architecture expliquée en 2 minutes sans erreur ;
  - soutenance complète fluide ;
  - moyenne ≥ 8/10 ;
  - aucune notion critique sous 4/5 dans `PROGRESS.md` ;
  - plan B de démonstration prêt.

## Critères de passage

| Jalon | Minimum |
|---|---|
| Fin J5 | expliquer mission, sources, composants et deux flux |
| Fin J10 | présentation 5 min cohérente, moyenne ≥ 6,5/10 |
| Fin J15 | défendre trois choix et trois limites, moyenne ≥ 7/10 |
| Fin J18 | résoudre un incident et proposer une architecture améliorée |
| Fin J21 | soutenance complète, moyenne ≥ 8/10, critiques ≥ 4/5 |

## Commande de démarrage

Dire exactement : `Commence le jour N`.

Le coach doit alors lire `STUDY_PLAN.md`, `PROGRESS.md` et
`PROJECT_ANALYSIS.md`, sélectionner les notions dues, puis poser une seule
question à la fois sans révéler la réponse.
