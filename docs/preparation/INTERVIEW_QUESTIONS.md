# Homepedia — Banque de questions d'entretien

## Mode d'emploi

- Pendant une séance, poser **une seule question à la fois**.
- Ne pas ouvrir la grille attendue avant la réponse.
- Noter sur 10 : exactitude 4, preuves projet 2, clarté 2, recul critique 2.
- Une réponse qui survend le projet comme « temps réel Kafka » ou « Spark
  distribué en production » doit être corrigée.
- Les questions marquées ★ sont prioritaires pour la soutenance.

Les grilles sont placées dans des blocs repliables pour servir à l'examinateur
après la tentative.

## Niveau 1 — Compréhension générale

### Questions

1. **L1-Q01 ★** Quel problème Homepedia résout-il en une phrase ?
2. **L1-Q02 ★** Quelles sont les principales sources de données ?
3. **L1-Q03 ★** Décris le parcours d'une transaction DVF jusqu'au dashboard.
4. **L1-Q04** Quelles sorties l'utilisateur peut-il consulter ou exporter ?
5. **L1-Q05 ★** Quels sont les composants majeurs de l'architecture ?
6. **L1-Q06** Quel est le rôle de SQLite ?
7. **L1-Q07** Quel est le rôle de MongoDB, et l'interface principale le lit-elle ?
8. **L1-Q08 ★** Pourquoi le projet utilise-t-il Spark ?
9. **L1-Q09** Quelles vues analytiques trouve-t-on dans Streamlit ?
10. **L1-Q10 ★** Le projet fait-il du batch, du streaming, ou les deux ?
11. **L1-Q11** Quels grains géographiques sont utilisés ?
12. **L1-Q12** Quels éléments prouvent que le projet a réellement traité un
    volume significatif ?

<details>
<summary>Grille examinateur — niveau 1</summary>

- **Q01** : transformer DVF + INSEE en indicateurs et visualisations du marché
  immobilier français.
- **Q02** : DVF 2024, INSEE population/revenu/chômage/pauvreté, page INSEE des
  indices ; GeoJSON ; avis hôteliers comme extension annexe.
- **Q03** : brut `|` → normalisation/filtre/dédoublonnage pandas → CSV traité →
  SQLite → SQL filtré et calcul prix/m² → visualisations/export.
- **Q04** : KPI, tables, cartes, distributions, boxplots, corrélations, historique
  temps réel, export CSV.
- **Q05** : ETL batch, Spark, SQLite, worker polling, MongoDB, Streamlit,
  Metabase/Docker Compose.
- **Q06** : stockage local portable ; `homepedia.db` pour l'analytique et
  `realtime_price.db` pour isoler les écritures périodiques.
- **Q07** : brut/observations/latest et miroir ; la page temps réel lit SQLite,
  le dashboard ne consulte Mongo que pour un test explicite.
- **Q08** : pré-agrégation départementale du DVF sur un cluster Spark standalone
  Docker de 1 master et 2 workers ; cluster réel mais mono-machine, pas production.
- **Q09** : Standard, Spark, Text Analysis, socio-éco, région, méthodologie, plus
  page temps réel.
- **Q10** : batch + polling micro-batch ; aucun vrai bus de streaming.
- **Q11** : transaction/commune/code postal, département, région, France/IDF/
  province pour le temps réel.
- **Q12** : 1 884 593 transactions uniques, 94 départements, 13 régions,
  cluster 1 master + 2 workers et runs tracés.

</details>

## Niveau 2 — Fonctionnement technique

### Questions

1. **L2-Q01 ★** Comment `prix_m2` est-il calculé et quelles lignes sont exclues ?
2. **L2-Q02** Comment les noms de colonnes DVF sont-ils normalisés ?
3. **L2-Q03 ★** Comment le job Spark transforme-t-il le CSV et où se trouve sa
   frontière avec pandas ?
4. **L2-Q04** Pourquoi `toPandas()` est-il acceptable ici, et quand deviendrait-il
   dangereux ?
5. **L2-Q05 ★** Comment les codes départements sont-ils dérivés pour le revenu,
   et quels cas particuliers sont traités ?
6. **L2-Q06** Comment les indicateurs départementaux sont-ils transformés en
   indicateurs régionaux ?
7. **L2-Q07 ★** Comment le scraper temps réel choisit-il les tableaux HTML ?
8. **L2-Q08 ★** Quels contrôles DQ sont appliqués à un `PricePoint` ?
9. **L2-Q09 ★** Quelle différence entre `latest`, `history` et `runs` ?
10. **L2-Q10** Comment l'idempotence est-elle assurée dans MongoDB ?
11. **L2-Q11** Pourquoi l'historique SQLite ne reçoit-il pas une ligne à chaque
    scraping ?
12. **L2-Q12** Comment l'UI évite-t-elle de charger toute la table de transactions ?
13. **L2-Q13** Quels formats de fichiers sont utilisés et pour quels usages ?
14. **L2-Q14** Quels index SQLite sont importants pour le projet ?
15. **L2-Q15** Comment le miroir complet SQLite vers Mongo fonctionne-t-il ?
16. **L2-Q16 ★** Comment le cluster Spark Docker est-il composé et comment le job
    lui est-il soumis ?
17. **L2-Q17** Comment les filtres Streamlit restent-ils cohérents entre KPI,
    tableaux, exports, cartes et graphiques ?
18. **L2-Q18** Comment fonctionne la simulation temps réel et quelles données
    sont volontairement absentes de la base ?

<details>
<summary>Grille examinateur — niveau 2</summary>

- **Q01** : valeur foncière / surface bâtie ; surface > 0, valeur non nulle,
  conversion numérique ; filtres de prix UI.
- **Q02** : strip, lowercase, espaces→underscore, NFKD, ASCII.
- **Q03** : lecture CSV strings, nettoyage/cast, calcul, code postal, groupBy ;
  agrégat collecté via `toPandas()` puis écrit SQLite.
- **Q04** : seulement ~100 lignes en sortie ; dangereux si forte cardinalité car
  collecte sur le driver.
- **Q05** : code commune sur 5 caractères ; DOM 97/98 sur 3 caractères, Corse
  2A/2B, sinon deux premiers.
- **Q06** : table de correspondance département→région, normalisation, groupBy
  avec somme/médiane/moyenne, puis jointures externes/gauches.
- **Q07** : score heuristique selon mots clés, présence période trimestrielle,
  choix du meilleur tableau, seuil minimal.
- **Q08** : UID/période/URL non vides, regex `YYYY-T[1-4]`, valeur finie, plages
  index `(0,2000]`, pourcentage `[-200,200]`.
- **Q09** : état courant / changements / audit de chaque tentative.
- **Q10** : index uniques `run_id`, `(metric_uid,scraped_at)` et `metric_uid`,
  puis upserts.
- **Q11** : déduplication si période et valeur inchangées ; les runs gardent la
  preuve de collecte.
- **Q12** : SQL paramétré, filtres appliqués en amont, échantillons/LIMIT et cache
  Streamlit ; message propre si aucun résultat.
- **Q13** : CSV brut/interchange, Parquet analytique colonne, GeoJSON carte,
  SQLite service, JSON TinyDB, HTML/PNG sorties.
- **Q14** : dates/code postal/commune/composite transaction ; métrique + timestamp
  et timestamps des runs.
- **Q15** : découvre tables, supprime orphelines, `drop()` chaque collection,
  insère par lots de 5 000.
- **Q16** : `spark-master`, deux workers d'un cœur/1 Gio, puis `spark-submit` via
  le profil tools et `spark://spark-master:7077`.
- **Q17** : un même masque ou ensemble de départements filtrés alimente toutes les
  vues ; éviter de recalculer chaque graphique sur un périmètre différent.
- **Q18** : 30 à 180 points, scénarios volatil/stable/hausse/baisse, tendance,
  oscillation et bruit ; comparaison brut/simulé ; aucune donnée fictive persistée.

</details>

## Niveau 3 — Justification des choix

### Questions

1. **L3-Q01 ★** Pourquoi SQLite est-il un choix raisonnable ici ?
2. **L3-Q02 ★** Quelles limites de SQLite apparaîtraient en production ?
3. **L3-Q03 ★** Pourquoi utiliser Spark alors que pandas est déjà présent ?
4. **L3-Q04** Le choix Spark est-il réellement nécessaire pour ce projet ?
5. **L3-Q05 ★** Pourquoi utiliser Parquet pour les indicateurs ?
6. **L3-Q06** Pourquoi garder MongoDB en plus de SQLite ?
7. **L3-Q07 ★** Pourquoi le polling est-il plus pertinent que Kafka pour cette
   source INSEE ?
8. **L3-Q08** Pourquoi séparer latest, history et runs plutôt que stocker une seule
   table ?
9. **L3-Q09** Pourquoi utiliser une médiane pour revenu/pauvreté et une moyenne
   pour chômage/prix ?
10. **L3-Q10 ★** Quelle est la différence entre reproductibilité et portabilité
    dans ce projet ?
11. **L3-Q11** Pourquoi pré-calculer un agrégat départemental ?
12. **L3-Q12 ★** Quels compromis introduit la double écriture SQLite/Mongo ?
13. **L3-Q13** Pourquoi monter `data/` en volume Docker ?
14. **L3-Q14** Quelles alternatives proposerais-tu à Streamlit ?
15. **L3-Q15** Quels choix réduisent le coût d'une démonstration ?
16. **L3-Q16 ★** Pourquoi séparer la base analytique de la base temps réel ?
17. **L3-Q17** Pourquoi les coefficients d'une matrice de corrélation ne doivent-ils
    pas totaliser 1 ?

<details>
<summary>Grille examinateur — niveau 3</summary>

- **Q01** : zéro administration, fichier portable, lecture locale, bon pour
  prototype mono-machine.
- **Q02** : concurrence d'écriture, verrouillage, HA, sauvegarde, scale-out,
  contrôle d'accès.
- **Q03** : volume DVF, API distribuable et valeur pédagogique ; pandas reste
  utilisé sur petits jeux/sorties.
- **Q04** : non strictement ; DuckDB/Polars/pandas chunké suffiraient probablement
  sur une machine. Spark se justifie surtout par apprentissage et évolution.
- **Q05** : compression, typage, projection de colonnes, scan analytique ; ici
  gain limité car fichiers petits.
- **Q06** : représentation documentaire, brut/debug, upserts/index uniques,
  démonstration polyglotte ; coût : complexité et cohérence.
- **Q07** : publication trimestrielle et quelques métriques ; Kafka ajouterait
  exploitation/coût sans flux événementiel réel.
- **Q08** : requêtes simples pour état, audit des changements et supervision
  séparés ; rétention différente.
- **Q09** : robustesse de la médiane aux extrêmes, somme population, moyenne taux/
  prix ; discuter pondération et « médiane des médianes ».
- **Q10** : portable = tourne local/Docker ; reproductible = mêmes dépendances,
  sources, versions, schémas et résultats. Le projet vise la première mais la
  dérive des dépendances/schémas nuit à la seconde.
- **Q11** : latence UI, moins de scans des faits, taille de sortie minime.
- **Q12** : disponibilité de deux usages mais absence d'atomicité, divergence,
  réconciliation nécessaire.
- **Q13** : persistance et accès aux gros jeux sans les intégrer à l'image ;
  contrepartie : dépendance à l'état hôte.
- **Q14** : Dash/Panel, API FastAPI + frontend, Superset/Metabase ; arbitrer
  vitesse de prototypage vs contrôle/performance.
- **Q15** : SQLite, Docker Compose, cache/LIMIT, agrégats, données déjà calculées,
  polling simple.
- **Q16** : éviter que les écritures périodiques et verrous du worker perturbent
  les lectures analytiques DVF ; lecture seule UI, WAL et busy timeout.
- **Q17** : chaque coefficient mesure indépendamment la relation entre deux
  variables ; ce ne sont ni des parts d'un total ni des probabilités.

</details>

## Niveau 4 — Expertise, limites, incidents et alternatives

### Questions

1. **L4-Q01 ★** Comment le chargement DVF est-il devenu idempotent, et quelle
   limite conserve un remplacement complet ?
2. **L4-Q02 ★** Que se passe-t-il si SQLite réussit puis MongoDB échoue dans un
   run temps réel ?
3. **L4-Q03** Comment garantirais-tu la cohérence entre SQLite et MongoDB ?
4. **L4-Q04 ★** Comment diagnostiques-tu un worker qui renvoie soudain zéro point ?
5. **L4-Q05** Comment reprends-tu un export SQLite→Mongo interrompu après trois
   collections ?
6. **L4-Q06 ★** Quels problèmes vois-tu dans la normalisation des codes
   géographiques ?
7. **L4-Q07** Comment prouves-tu que les 1 884 593 transactions finales sont
   nettoyées et que le chargement est rejouable ?
8. **L4-Q08 ★** Comment le contrat de données commun empêche-t-il les divergences
   SQLite/MongoDB/Streamlit/Metabase ?
9. **L4-Q09** Comment versionnerais-tu le schéma et les contrats de données ?
10. **L4-Q10** Comment partitionnerais-tu les transactions en data lake ?
11. **L4-Q11** Comment éviter un scan ou un chargement mémoire complet du DVF ?
12. **L4-Q12 ★** Quels tests prioritaires ajouterais-tu ?
13. **L4-Q13** Quelles métriques et alertes d'observabilité mettrais-tu ?
14. **L4-Q14** Comment sécuriserais-tu MongoDB et l'application ?
15. **L4-Q15** Comment gérerais-tu une modification silencieuse du HTML INSEE ?
16. **L4-Q16** Le mode mock de l'UI est-il acceptable ? Sous quelles conditions ?
17. **L4-Q17** Comment ferais-tu évoluer l'architecture pour dix utilisateurs
    concurrents ? Pour mille ?
18. **L4-Q18** Dans quel cas Kafka deviendrait-il pertinent ?
19. **L4-Q19** Comment distinguer corrélation et causalité dans les graphiques du
    projet ?
20. **L4-Q20 ★** Si tu avais une semaine, quelles trois améliorations livrerais-tu
    avant la soutenance ?

<details>
<summary>Grille examinateur — niveau 4</summary>

- **Q01** : nettoyage/dédoublonnage puis `replace` contrôlé ; le rejeu du même
  fichier conserve 1 884 593 faits, mais une interruption pendant le remplacement
  demande staging/transaction/swap pour une garantie plus forte.
- **Q02** : SQLite est à jour, Mongo en retard, run journalisé `error` ; prochain
  run peut corriger latest mais pas forcément le brut manquant avec le même run.
- **Q03** : store canonique + outbox, CDC, file de messages, saga/retry ou job de
  réconciliation ; éviter transaction distribuée si inutile.
- **Q04** : regarder statut/error/raw_debug, nombre de tables, scores/headers,
  HTTP/status, snapshot HTML ; vérifier période/structure/seuils et tests fixtures.
- **Q05** : relance complète possible mais fenêtre vide/partielle ; mieux écrire
  dans collections temporaires versionnées puis swap/alias.
- **Q06** : Corse `2A/2B`, DOM à trois chiffres, zéros initiaux, types Excel ;
  référentiel et normalisation centralisée.
- **Q07** : règles explicites, doublons exacts ou clé métier, mutations
  symboliques, plages de prix, tests de contrat et second lancement donnant le
  même nombre de lignes.
- **Q08** : dictionnaire canonique français, clés et mesures documentées, tests de
  contrat, migrations versionnées et miroir qui préserve les mêmes noms/champs.
- **Q09** : migrations Alembic ou SQL versionnées, version de dataset, schéma
  canonique, tests de contrat, data dictionary, compatibilité ascendante.
- **Q10** : année/mois ou département selon requêtes, éviter petites partitions,
  Parquet, statistiques/catalogue ; mesurer avant.
- **Q11** : chunks pandas, projection colonnes, conversion directe Parquet,
  DuckDB/Polars lazy, Spark et partition pruning.
- **Q12** : parseur avec HTML figé, normalisation Corse/DOM, DQ limites,
  idempotence/rejeu, agrégats attendus, migration de schéma, smoke UI.
- **Q13** : fraîcheur, durée, points collectés/valides/rejetés, erreurs, volume,
  changements, disponibilité source/stores, verrous SQLite, latence UI ; alertes
  sur fraîcheur et échecs consécutifs.
- **Q14** : ne pas exposer 27017, auth/TLS, secrets manager, réseau interne,
  moindre privilège, auth Streamlit/reverse proxy, chiffrement/backups.
- **Q15** : contrat de parsing, fixtures, canary, seuil de métriques, conservation
  HTML/debug, alerte et fallback versionné.
- **Q16** : acceptable comme simulation explicitement étiquetée, jamais confondue
  avec mesure réelle, désactivée par défaut hors démo.
- **Q17** : 10 : WAL/read replicas ou PostgreSQL, cache ; 1000 : service API,
  DB analytique, pré-agrégats, CDN/cache, workers séparés et HA.
- **Q18** : nombreuses sources événementielles, débit/latence, plusieurs
  consommateurs, replay, découplage ; pas pour sept valeurs trimestrielles.
- **Q19** : corrélation descriptive, facteurs confondants, agrégation écologique,
  temporalités différentes, pas de causalité sans design causal.
- **Q20** : aligner schéma/branche ; environnement et dépendances reproductibles ;
  tests/smoke de démonstration et script de lancement. Accepter variantes bien
  justifiées.

</details>

## Questions pièges courtes

1. « Combien de nœuds contient ton cluster Spark ? »
2. « Quel topic Kafka porte les transactions ? »
3. « Pourquoi appelles-tu trimestrielle une donnée scrapée toutes les cinq
   minutes ? »
4. « MongoDB est-il la source de vérité ? »
5. « Quelle garantie exactly-once offres-tu ? »
6. « Ton prix moyen au m² est-il robuste aux valeurs aberrantes ? »
7. « Une corrélation revenu-prix prouve-t-elle un effet causal ? »
8. « Ton cluster Spark possède-t-il réellement trois machines physiques ? »
9. « Pourquoi obtiens-tu 94 départements et 13 régions, pas 101 et 18 ? »
10. « Un `LIMIT` sans `ORDER BY` produit-il un échantillon représentatif ? »
11. « Pourquoi les corrélations de la matrice ne totalisent-elles pas 1 ? »
12. « Les points simulés de la démo polluent-ils l'historique réel ? »

<details>
<summary>Réflexes attendus</summary>

Ne pas inventer. Répondre respectivement : 1 master + 2 workers conteneurisés sur
une seule machine physique, aucun Kafka, fréquence
de collecte ≠ fréquence de publication, rôle dépendant du flux, idempotence
partielle et non exactly-once, moyenne sensible aux extrêmes, non-causalité,
cluster Docker mono-machine, couverture liée aux données valides et au périmètre
métropolitain/Corse, `LIMIT` non ordonné non représentatif, coefficients
indépendants et simulation jamais persistée.

</details>
