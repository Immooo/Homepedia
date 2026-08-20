# Homepedia — Journal des séances quotidiennes

> Les séances sont consignées ci-dessous. Ce fichier est complété à la fin de chaque
> séance ; les entrées les plus récentes sont ajoutées en haut de la section
> « Historique ».

## État courant

| Champ | Valeur |
|---|---|
| Programme | En cours |
| Prochaine séance | Jour 4 — Composants et état réel |
| Commande | `Commence le jour 4` |
| Dernière séance | J3 — 14/08/2026 |
| Calendrier | Accéléré du 12/08/2026 au 03/09/2026 |
| Jours tampon | 18/08 et 25/08 |
| Point prioritaire | Diagnostic initial sans consultation des réponses |

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
