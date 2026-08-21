# Homepedia — Journal des séances quotidiennes

> Les séances sont consignées ci-dessous. Ce fichier est complété à la fin de chaque
> séance ; les entrées les plus récentes sont ajoutées en haut de la section
> « Historique ».

## État courant

| Champ | Valeur |
|---|---|
| Programme | En cours |
| Prochaine séance | Jour 6 — Ingestion DVF et qualité batch |
| Commande | `Commence le jour 6` |
| Dernière séance | J5 — 21/08/2026 |
| Calendrier | Réorganisé du 20/08/2026 au 03/09/2026 : 2 h lun.–mer., doubles blocs jeu.–ven. |
| Jours tampon | Samedi léger facultatif ; dimanche libre ; 04/09 en filet de sécurité |
| Point prioritaire | Consolider SQLite/MongoDB, les fichiers temps réel et la divergence de schéma avant d'ajouter du contenu |

## Format d'une entrée

```markdown
## Séance JN — AAAA-MM-JJ

- Durée :
- Questions posées :
- Note moyenne :
- Exercice oral :
- Résultat :

### Résumé très court

Deux ou trois phrases maximum.

### Notions maîtrisées

- ...

### Notions fragiles

- ...

### Erreurs à corriger

- Affirmation :
  - Pourquoi elle est incorrecte :
  - Formulation attendue :

### Révisions programmées

| Notion | Niveau | Prochaine révision | Motif |
|---|---:|---|---|
| ... | ... | ... | ... |

### Objectifs de la séance suivante

- ...
```

## Historique

## Séance J5 — 2026-08-21

- Durée : bloc du matin + rappel court, séance interactive d'environ 1 h 20 au total
- Suivi temporel : début 10 h 07 ; pause à 10 h 43 ; reprise à 11 h 39 ; environ 50 minutes actives
- Règle pour la suite : annoncer début, pause, reprise et durée active à chaque bloc
- Questions posées : 23 évaluées (une question chiffrée annulée à la demande de l'apprenant)
- Note moyenne : 8,0/10
- Exercice oral : présentation spontanée de la chaîne complète Homepedia
- Résultat : révision cumulative réussie ; les notions batch, sources INSEE, schéma divergent et idempotence progressent nettement

### Résumé très court

Tu as retrouvé un récit global cohérent et tu réponds correctement aux questions
sur DVF, INSEE, Spark, SQLite, polling et agrégation. Les points à surveiller
restent la précision du pitch, la distinction entre collecte et publication, et
les trois catégories `latest/history/runs` lorsque la question est posée rapidement.

### Notions maîtrisées

- séparation des sources INSEE fichiers et indices HTML périodiques ;
- intérêt de la pré-agrégation Spark et perte du détail transactionnel ;
- divergence code/base et contrat de données canonique ;
- rôle de SQLite pour la concurrence locale et rôle du mainteneur ;
- `runs` à chaque tentative et `history` seulement si période ou valeur change ;
- risque de duplication avec `append` et principe d'idempotence.

### Notions fragiles

- formuler un pitch concis sans dire que le projet « gère » l'immobilier ;
- expliquer pourquoi cinq minutes de polling ne signifient pas une publication toutes les cinq minutes ;
- citer deux preuves chiffrées complètes sans aide ;
- détailler `latest/history/runs` sous pression.

### Révisions programmées

| Notion | Niveau | Prochaine révision | Motif |
|---|---:|---|---|
| Problème métier et pitch | 3 | 25/08/2026 | Rendre la présentation plus concise |
| Sources DVF et INSEE | 4 | 28/08/2026 | Distinction fichiers/HTML acquise |
| Polling versus vrai streaming | 3 | 25/08/2026 | Revoir collecte versus publication |
| Latest/history/runs | 2 | 23/08/2026 | Réactivation nécessaire sous question rapide |
| Idempotence des batchs | 4 | 28/08/2026 | Risque `append` compris |

### Objectifs de la séance suivante

- expliquer le nettoyage DVF étape par étape ;
- distinguer doublon, valeur manquante, valeur invalide et non-idempotence ;
- défendre `append`, `replace` et une clé naturelle avec leurs compromis.

## Séance J4 — 2026-08-20

- Durée : deux blocs interactifs, environ 2 h 15
- Questions posées : 42
- Note moyenne : 7,1/10
- Exercice oral : reconstruction guidée de la chaîne globale, des preuves d'exécution locale et présentation de 60 à 90 secondes
- Résultat : architecture globale retrouvée ; distinguer plus précisément Spark, SQLite, MongoDB et les sources INSEE dans le récit oral

### Résumé très court

Après un démarrage trop directement centré sur les fichiers, la séance a été
reprise depuis l'architecture globale. Tu sais à nouveau raconter le flux
sources → ETL/pandas → stockage → Streamlit, positionner Spark sur DVF, suivre
le polling INSEE et citer la preuve d'environ 5,8 millions de transactions. Les
confusions SQLite/MongoDB, sources INSEE et formulation orale restent prioritaires.

### Notions maîtrisées

- rôle de Streamlit comme interface utilisateur ;
- rôle de pandas dans les scripts ETL ;
- Spark appliqué aux transactions DVF et intérêt des agrégats ;
- perte du détail après agrégation ;
- `data/homepedia.db` comme preuve de l'exécution locale ;
- noms anglais dans le code courant, français dans la base locale ;
- principe d'un contrat de données canonique.
- polling micro-batch périodique, distinct du vrai streaming ;
- séparation `latest` / `history` / `runs` du temps réel.
- compromis de la double écriture SQLite/MongoDB et principe de réconciliation.

### Notions fragiles

- associer les noms précis des fichiers à leurs rôles ;
- distinguer `insee_scraper.py`, `worker.py` et `sqlite_store.py` ;
- rôle de MongoDB versus SQLite ;
- batch versus polling micro-batch ;
- migrations versionnées et tests de contrat.
- présenter explicitement une limite réelle à l'oral.
- expliquer l'impact d'une panne partielle entre SQLite et MongoDB.

### Erreurs à corriger

- Affirmation : « Le dashboard lit MongoDB car les données sont pré-agrégées. »
  - Pourquoi elle est incorrecte : Streamlit lit principalement SQLite, avec
    certains fichiers Parquet et GeoJSON ; MongoDB complète surtout le temps réel.
  - Formulation attendue : « SQLite sert directement l'interface ; MongoDB garde
    notamment le brut, les observations et le dernier état du sous-système temps réel. »
- Affirmation : « Les noms de tables sont normalement les mêmes. »
  - Pourquoi elle est incorrecte : une divergence est constatée entre le code
    courant en anglais et la base locale surtout en français.
  - Formulation attendue : « Je constate une dérive de contrat à corriger par un
    schéma canonique, des migrations versionnées et des tests de contrat. »
- Affirmation : « Homepedia gère l'immobilier. »
  - Pourquoi elle est incorrecte : le projet analyse et visualise des données ;
    il ne gère ni annonces, ni biens, ni transactions opérationnelles.
  - Formulation attendue : « Homepedia est un prototype analytique qui permet de
    comparer le marché immobilier français à partir de DVF et d'indicateurs INSEE. »

### Révisions programmées

| Notion | Niveau | Prochaine révision | Motif |
|---|---:|---|---|
| Problème métier et pitch | 2 | 22/08/2026 | Citer systématiquement DVF et INSEE |
| Fichiers et composants centraux | 2 | 22/08/2026 | Noms et rôles à mémoriser |
| Polling versus vrai streaming | 2 | 22/08/2026 | Employer batch et micro-batch périodique |
| Divergence de schéma/branches | 4 | 27/08/2026 | Réponse correcte après reformulation |
| MongoDB : collections et index | 3 | 24/08/2026 | Distinguer MongoDB et SQLite |
| Sources DVF et INSEE | 2 | 22/08/2026 | Citer toutes les familles de sources dans le pitch |
| Architecture globale | 2 | 22/08/2026 | Replacer précisément Spark et MongoDB |

### Objectifs de la séance suivante

- réviser d'abord les erreurs J1–J4 ;
- raconter le projet en deux minutes sans notes, avec DVF, INSEE et une limite ;
- distinguer tous les flux batch, Spark et temps réel ;
- renforcer les fichiers centraux sans perdre la vision globale.

## Séance J1 — 2026-08-12

- Durée : séance interactive, 14 questions
- Questions posées : 14
- Note moyenne : 7,8/10
- Exercice oral : pitch de 60 secondes + distinction utilisateur/développeur
- Résultat : bases présentes ; besoin de structurer les flux et de préciser les rôles techniques

### Résumé très court

Tu comprends le but général : comparer les prix immobiliers par territoire avec DVF
et INSEE. Tu identifies correctement SQLite, Streamlit, pandas et l'intérêt général
de Spark. Tu sais maintenant distinguer le besoin de l'utilisateur du travail du
développeur et raconter le chemin général de la donnée. Les réponses doivent encore
devenir plus précises et éviter les formulations absolues comme « tout savoir ».

### Notions maîtrisées

- valeur foncière et surface pour calculer le prix au m² ;
- intérêt de supprimer les doublons ;
- différence générale entre donnée brute et donnée nettoyée ;
- rôle de `homepedia.db` et de Streamlit ;
- différence générale entre pandas et Streamlit ;
- intuition du rôle de Spark sur de gros volumes.
- différence entre utilisateur du dashboard et mainteneur du pipeline ;
- idée générale du chemin brut → nettoyage → stockage → calcul → affichage.

### Notions fragiles

- parcours complet d'une transaction DVF ;
- détails des sources et sorties ;
- différence pandas en mémoire / Spark distribué ;
- utilisateurs et indicateurs socio-économiques à citer dans le pitch.
- rôle du développeur à compléter avec tests, déploiement, erreurs et monitoring.

### Erreurs à corriger

- « Spark est utilisé parce que la consigne le demande » : il faut d'abord donner
  sa justification technique — traitement distribuable d'un volume important —
  puis reconnaître que le code actuel montre surtout une session locale.
- « les données sont affichées » : préciser CSV traité, SQLite, requête filtrée,
  puis tableau/carte/graphique Streamlit.
- « on peut tout savoir » : remplacer par le périmètre réel des données disponibles
  et reconnaître les limites de couverture.

### Révisions programmées

| Notion | Niveau | Prochaine révision | Motif |
|---|---:|---|---|
| Entrées, sorties et utilisateurs | 2 | 13/08/2026 | Réponse trop générale |
| Parcours d'une transaction DVF | 2 | 13/08/2026 | Flux à réciter dans l'ordre |
| Usage réel de Spark | 2 | 13/08/2026 | Ajouter la nuance session locale |
| Justification Spark/pandas | 2 | 13/08/2026 | Distinguer les moteurs |
| Problème métier et pitch | 3 | 16/08/2026 | Enrichir avec INSEE et utilisateurs |
| Sources DVF et INSEE | 3 | 16/08/2026 | Citer les variables |
| Ingestion et nettoyage DVF | 3 | 16/08/2026 | Consolider les étapes |
| SQLite : rôle, index, limites | 3 | 16/08/2026 | Ajouter serveur/concurrence |
| Entrées, sorties et utilisateurs | 3 | 16/08/2026 | Ajouter export, tests et monitoring au rôle du développeur |

### Objectifs de la séance suivante

- dessiner l'architecture globale sans notes ;
- placer pandas, Spark, SQLite, MongoDB, Streamlit et Docker ;
- refaire le parcours DVF en moins de 90 secondes ;
- répondre à « batch ou streaming ? ».

## Séance J2 — 2026-08-14

- Durée : séance interactive, 10 questions
- Questions posées : 10
- Note moyenne : 7,9/10
- Exercice oral : explication de l'architecture en environ une minute
- Résultat : architecture globale comprise ; rôles MongoDB et contrats entre composants à renforcer

### Résumé très court

Tu sais maintenant reconstruire la chaîne `sources → ETL/pandas → Spark éventuel
→ stockage → Streamlit`, et tu comprends le rôle de Docker Compose. Tu distingues
également batch et polling. La prochaine étape est de suivre précisément une donnée
DVF et un indicateur INSEE à travers les transformations.

### Notions maîtrisées

- rôle général de SQLite, Spark, Streamlit et Docker Compose ;
- chaîne ETL : extraire, transformer, charger ;
- différence batch/polling ;
- architecture globale et couches de présentation/stockage.

### Notions fragiles

- détail des collections MongoDB (`raw`, `observations`, `latest`) ;
- distinction entre le chemin batch DVF et le chemin temps réel ;
- composants concrets à associer aux fichiers du dépôt.

### Erreurs à corriger

- Dire « SQLite ou MongoDB » : pour le worker temps réel, SQLite et MongoDB sont
  alimentés séquentiellement ; ils ont des rôles complémentaires.
- Dire « projet Media » ou « requête NC » : employer Homepedia et page INSEE.

### Révisions programmées

| Notion | Niveau | Prochaine révision | Motif |
|---|---:|---|---|
| Architecture globale | 3 | 18/08/2026 | Bonne synthèse à stabiliser |
| Fichiers et composants centraux | 2 | 16/08/2026 | Associer noms et rôles |
| MongoDB : collections et index | 2 | 16/08/2026 | Préciser raw/observations/latest |
| Polling versus vrai streaming | 3 | 18/08/2026 | Bonne distinction à réutiliser |

### Objectifs de la séance suivante

- suivre une transaction DVF étape par étape ;
- suivre un indicateur INSEE jusqu'à son affichage ;
- distinguer donnée brute, CSV traité, Parquet, SQLite et agrégat ;
- expliquer où interviennent normalisation, jointure, index et cache.

## Séance J3 — 2026-08-14

- Durée : séance interactive, 12 questions
- Questions posées : 12
- Note moyenne : 7,4/10
- Exercice oral : réponses détaillées sur le parcours DVF et la jointure INSEE
- Résultat : flux général compris ; précision à renforcer sur les niveaux de détail et les clés de jointure

### Résumé très court

Tu sais raconter la chaîne DVF et le parcours général d'un indicateur INSEE. Tu
comprends pourquoi Spark produit des agrégats plus rapides à consulter. Tu dois
encore distinguer clairement les données détaillées des données agrégées et
expliquer qu'une jointure s'effectue sur une clé géographique commune.

### Notions maîtrisées

- colonnes principales d'une transaction DVF ;
- formule `valeur_fonciere / surface_reelle_bati` et conditions de validité ;
- rôle du code postal pour localiser/regrouper ;
- fichier `transactions_2024.csv` ;
- intérêt d'un agrégat départemental ;
- parcours général d'un indicateur INSEE.

### Notions fragiles

- différence exacte entre CSV détaillé, table SQLite détaillée et table Spark agrégée ;
- jointure sur clé (`code`, `dept`, `code_region`) ;
- perte du détail communal après agrégation ;
- traitement spécifique Corse/DOM.

### Erreurs à corriger

- Le nombre de pièces et le type de bien ne sont pas nécessaires au calcul du prix
  au m² ; ils servent aux filtres et regroupements.
- Une agrégation départementale ne permet plus de retrouver le détail de chaque
  commune.
- Une jointure ne consiste pas seulement à prendre deux fichiers : elle relie des
  lignes grâce à une clé commune, par exemple le code département.
- Dire **FILOSOFI**, pas « Philosophie ».

### Révisions programmées

| Notion | Niveau | Prochaine révision | Motif |
|---|---:|---|---|
| Parcours d'une transaction DVF | 3 | 18/08/2026 | Stabiliser les étapes et niveaux de détail |
| Parcours d'un indicateur INSEE | 3 | 18/08/2026 | Ajouter les noms de fichiers et formats |
| Agrégations et biais statistiques | 3 | 18/08/2026 | Retenir la perte du détail communal |
| Jointures et clés géographiques | 2 | 16/08/2026 | Revoir clé et direction de jointure |

### Objectifs de la séance suivante

- associer cinq fichiers du dépôt à leurs rôles ;
- expliquer l'écart entre code courant, base locale et ERD ;
- citer des preuves chiffrées de l'état local ;
- distinguer fait constaté, hypothèse et information manquante.

## Consignes au coach

- Lire `STUDY_PLAN.md`, `PROGRESS.md` et `PROJECT_ANALYSIS.md` avant la séance.
- Sélectionner d'abord les notions dont la révision est arrivée à échéance.
- Poser une seule question et attendre la réponse.
- Ne pas révéler la réponse avant la tentative.
- Si la réponse est « je ne sais pas », donner un indice ; ne donner la réponse
  complète qu'après un second échec ou sur demande.
- Après chaque réponse : correct, manques, erreurs, réponse améliorée, note /10,
  puis adaptation de la difficulté.
- En fin de séance, mettre à jour ce journal et `PROGRESS.md`.
