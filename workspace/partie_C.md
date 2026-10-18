# Partie C – Cassandra : réponses pour le rapport

Script : `workspace/03_cassandra_publication.py` · Sortie complète : `workspace/log_C.txt`

```bash
docker compose --profile nosql up -d --build
# attendre que le service cassandra soit "healthy" (~2 min)
docker compose exec notebook python /home/jovyan/work/03_cassandra_publication.py
```

Entrée : `output/audience_cible.jsonl` produit par MongoDB (partie B3), 34 855 documents.
Le script est rejouable : il supprime et recrée ses tables à chaque exécution.

---

## C1. Modèle orienté requête

### Requête à servir
« Top N des utilisateurs d'un pays et d'un segment, triés par score décroissant. »

### Table
```sql
CREATE KEYSPACE IF NOT EXISTS inf6303_devoir1
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE audience_par_pays_segment (
    country   text,
    segment   text,
    score     int,      -- = analytics.total_watch_sec
    user_id   text,
    nb_events int,      -- = analytics.nb_events
    plan      text,
    PRIMARY KEY ((country, segment), score, user_id)
) WITH CLUSTERING ORDER BY (score DESC, user_id ASC);
```

### Choix du score
`score = analytics.total_watch_sec`, la durée totale de visionnement calculée par Hive
(`SUM(watch_seconds)`) et transmise par MongoDB. C'est la métrique qui définit le segment
(intensif ≥ 55 000 s, actif ≥ 30 000 s). Elle est traçable de bout en bout (Hive → MongoDB →
Cassandra) et aucune valeur n'est inventée entre les étapes.

### Clé de partition `(country, segment)`
- La requête fournit toujours un pays **et** un segment : ce couple est donc la clé de partition.
- Toutes les lignes d'un couple sont stockées ensemble, sur le même nœud (et ses réplicas).
  Une requête de service lit **une seule partition**, sans balayer le cluster.
- Une clé `country` seule obligerait à filtrer le segment. Une clé `segment` seule ne donnerait
  que 2 partitions énormes.

### Clustering `(score DESC, user_id ASC)`
- Dans une partition, Cassandra garde les lignes triées sur disque selon les colonnes de
  clustering. Avec `score DESC`, le tri demandé est déjà fait à l'écriture : la lecture se limite
  aux N premières lignes (`LIMIT 10`), sans aucun tri ni `ORDER BY` à l'exécution.
- **Égalités de score réglées par `user_id`.** En Cassandra, un `INSERT` est un *upsert* :
  deux lignes qui ont la même clé primaire s'écrasent. Or plusieurs utilisateurs d'une même
  partition ont exactement le même `total_watch_sec`. Mesuré sur `audience_cible.jsonl` :
  - 2 548 combinaisons `(country, segment, score)` sont partagées par au moins deux utilisateurs ;
  - jusqu'à 5 ex aequo, par exemple Canada/actif/38 158 s : u001872, u003108, u004232,
    u005460, u027798 ;
  - avec la clé `((country, segment), score)`, **2 772 lignes auraient été perdues en silence**
    (32 083 lignes chargées au lieu de 34 855) et le contrôle C2 aurait échoué.

  Ajouter `user_id`, déjà unique, comme dernière colonne de clustering rend la clé primaire
  unique. Le tri secondaire par `user_id ASC` rend aussi l'ordre des ex aequo déterministe.

### Dénormalisation
- Cassandra ne fait pas de jointure. La table contient donc **tout ce que la réponse affiche** :
  `user_id`, `country`, `segment`, `score`, `nb_events`, `plan`. Une lecture suffit, sans aller
  chercher le plan dans MongoDB ni la durée dans Hive.
- La table est une **vue de service** dérivée de MongoDB : `plan` y est copié, et `country`
  et `segment` font partie de la clé. Si un besoin de lecture change, on crée une autre table avec
  les mêmes données et une autre clé (voir `audience_par_plan` en C3). Le stockage dupliqué est
  le prix d'une latence prévisible.
- Seul le sous-ensemble utile est transféré : 34 855 profils consentants intensif/actif.
  Le million d'événements reste dans HDFS.
- Contrepartie : les copies doivent être tenues cohérentes. Ici, elles sont reconstruites à
  chaque exécution à partir de `audience_cible.jsonl`, qui est le point de reprise.

### Risque de partition chaude
Les 16 partitions (8 pays × 2 segments) sont déséquilibrées (comptages tirés de `log_C.txt`) :

| Pays | actif | intensif |
|---|---:|---:|
| Belgique | 1 978 | 474 |
| Cameroun | 2 536 | 604 |
| **Canada** | **8 792** | 2 077 |
| Cote_d_Ivoire | 3 370 | 811 |
| France | 3 305 | 820 |
| Maroc | 2 752 | 684 |
| Senegal | 3 666 | 932 |
| Tunisie | 1 672 | **382** |
| **Total** | 28 071 | 6 784 |

- Canada/actif = **8 792** lignes contre **382** pour Tunisie/intensif, soit environ **23 fois plus**.
  Le Canada représente à lui seul 31 % des lignes (10 869 / 34 855), comme dans les données
  sources (311 247 événements sur 1 000 000).
- Le nœud qui détient Canada/actif reçoit une part disproportionnée des lectures et des
  écritures (partition chaude). Avec seulement 16 partitions, la répartition par hachage est
  aussi grossière : quelques nœuds porteraient tout.
- **À ce volume, le risque reste théorique.** 8 792 lignes de quelques dizaines d'octets font
  moins de 1 Mo, très loin de la limite pratique d'environ 100 Mo par partition. De plus, la
  requête ne lit que les 10 premières lignes, déjà triées.
- **Si le volume augmentait**, on ajouterait un *bucket* à la clé de partition,
  `((country, segment, bucket), score, user_id)` avec `bucket = hash(user_id) % N`. Le top 10
  global s'obtient alors en lisant les N buckets et en fusionnant N petites listes déjà triées.
  Autre option : remplacer le segment par une granularité plus fine (une tranche de score).

---

## C2. Chargement

- Lecture ligne à ligne de `/home/jovyan/work/output/audience_cible.jsonl`.
  Correspondance des champs : `country`, `analytics.segment`, `analytics.total_watch_sec → score`,
  `user_id`, `analytics.nb_events → nb_events`, `plan`.
- **Requête préparée** (`session.prepare("INSERT ... VALUES (?, ?, ?, ?, ?, ?)")`). Le serveur
  analyse le CQL une seule fois, puis seules les valeurs circulent pour chaque ligne. C'est plus
  rapide et il n'y a aucune concaténation de chaînes, donc aucun risque d'injection CQL.
  Les insertions sont envoyées en parallèle (`execute_concurrent_with_args`, concurrence 64) :
  34 855 insertions en 13,8 s, 0 échec.
- **Comptage par partition** : un `SELECT COUNT(*) ... WHERE country = ? AND segment = ?` pour
  chacun des 16 couples, c'est-à-dire la même méthode d'accès que le service. On évite ainsi un
  `COUNT(*)` global qui balaierait tout le cluster.

| Contrôle | Valeur |
|---|---:|
| Documents exportés par MongoDB (`audience_cible.jsonl`) | **34 855** |
| Lignes chargées dans `audience_par_pays_segment` | **34 855** |
| Partitions | 16 |
| Écart | 0 (`assert nb_charges == nb_exportes` réussi) |

Chaque partition contient exactement le nombre de documents attendu pour son couple.

---

## C3. Requêtes de service

Requête préparée commune (une seule partition lue, ordre déjà sur disque) :
```sql
SELECT user_id, score, nb_events, plan
FROM audience_par_pays_segment
WHERE country = ? AND segment = ?
LIMIT 10;
```

### Top 10 Canada / intensif

| # | user_id | score (s) | nb_events | plan |
|---:|---|---:|---:|---|
| 1 | u012192 | 99 744 | 36 | standard |
| 2 | u047861 | 96 938 | 28 | standard |
| 3 | u025691 | 95 497 | 34 | standard |
| 4 | u049062 | 95 407 | 42 | premium |
| 5 | u042077 | 94 314 | 30 | standard |
| 6 | u001658 | 94 019 | 24 | standard |
| 7 | u007230 | 93 381 | 26 | gratuit |
| 8 | u013563 | 93 250 | 37 | gratuit |
| 9 | u025640 | 90 301 | 31 | gratuit |
| 10 | u029858 | 90 175 | 33 | premium |

Vérification indépendante : un tri Python du fichier `audience_cible.jsonl` (Canada, intensif,
score décroissant) donne exactement la même liste.

### Autre couple pertinent : France / actif
Choix : la France est le premier marché européen du jeu de données (118 790 événements contre
69 962 pour la Belgique), et le segment actif est le plus
nombreux. Les premiers actifs sont juste sous le seuil intensif (55 000 s) : c'est l'audience la
plus facile à faire passer à l'intensif par une campagne ciblée.

| # | user_id | score (s) | nb_events | plan |
|---:|---|---:|---:|---|
| 1 | u048934 | 54 994 | 25 | premium |
| 2 | u009679 | 54 993 | 22 | gratuit |
| 3 | u015351 | 54 982 | 24 | standard |
| 4 | u017027 | 54 967 | 21 | standard |
| 5 | u020613 | 54 964 | 24 | gratuit |
| 6 | u010041 | 54 951 | 22 | standard |
| 7 | u045319 | 54 948 | 22 | gratuit |
| 8 | u001195 | 54 941 | 27 | gratuit |
| 9 | u044077 | 54 938 | 23 | gratuit |
| 10 | u036149 | 54 929 | 26 | standard |

Tous les scores sont < 55 000, ce qui confirme que la bonne frontière de segment est respectée
dans Cassandra.

### Recherche globale par plan : pas naturelle pour cette table
```sql
SELECT user_id, country, segment, score
FROM audience_par_pays_segment WHERE plan = 'premium' LIMIT 10;
```
Réponse de Cassandra :
```
code=2200 [Invalid query] message="Cannot execute this query as it might involve data
filtering and thus may have unpredictable performance. If you want to execute this query
despite the performance unpredictability, use ALLOW FILTERING"
```
`plan` ne fait ni partie de la clé de partition ni du clustering. Cassandra ne sait donc pas
quelles partitions lire : il faudrait les ouvrir toutes. Il refuse plutôt que de lancer
en silence un balayage complet.
Avec `ALLOW FILTERING`, la requête passe et trouve **6 352** lignes premium, mais seulement après
avoir lu les **34 855** lignes des 16 partitions. Le coût est proportionnel à la taille de la
table, pas à celle de la réponse, et le résultat n'est même pas trié globalement par score
(chaque partition est triée séparément). C'est l'inverse du principe « une requête = une
partition ».

### Table supplémentaire proposée : `audience_par_plan`
```sql
CREATE TABLE audience_par_plan (
    plan      text,
    score     int,
    user_id   text,
    country   text,
    segment   text,
    nb_events int,
    PRIMARY KEY ((plan), score, user_id)
) WITH CLUSTERING ORDER BY (score DESC, user_id ASC);
```
- Clé de partition `plan` : la nouvelle requête fournit le plan, donc une requête = une partition.
- Même clustering `(score DESC, user_id ASC)` : top N par score sans tri, et ex aequo conservés.
- Mêmes données dupliquées depuis `audience_cible.jsonl` (dénormalisation : une table par requête).

Résultats (chargement par requête préparée, comptage par partition) :

| plan | lignes chargées | attendu (fichier) |
|---|---:|---:|
| gratuit | 16 831 | 16 831 |
| premium | 6 352 | 6 352 |
| standard | 11 672 | 11 672 |
| **Total** | **34 855** | **34 855** |

Le nombre de premium (6 352) est identique à celui obtenu par `ALLOW FILTERING` : les deux
tables sont cohérentes.

Top 10 premium (`WHERE plan = 'premium' LIMIT 10`, une seule partition) :

| # | user_id | score (s) | country | segment |
|---:|---|---:|---|---|
| 1 | u041488 | 103 073 | Cameroun | intensif |
| 2 | u040743 | 96 078 | Cote_d_Ivoire | intensif |
| 3 | u049062 | 95 407 | Canada | intensif |
| 4 | u009818 | 95 404 | Senegal | intensif |
| 5 | u049503 | 92 788 | France | intensif |
| 6 | u025368 | 92 400 | Cameroun | intensif |
| 7 | u024406 | 92 099 | Belgique | intensif |
| 8 | u002854 | 90 899 | Senegal | intensif |
| 9 | u029858 | 90 175 | Canada | intensif |
| 10 | u004019 | 89 320 | Canada | intensif |

u049062 et u029858 apparaissent dans les deux tops (Canada/intensif et premium) avec le même
score : c'est la même donnée vue par deux clés différentes.

**Limite de cette clé** : elle ne donne que 3 partitions, dont `gratuit` qui regroupe 48 % des
lignes. C'est acceptable à 34 855 lignes. À plus grande échelle, on utiliserait
`((plan, country), score, user_id)` si le pays est connu à la requête, ou un *bucket*
`((plan, bucket), score, user_id)` pour garder une recherche globale par plan.
