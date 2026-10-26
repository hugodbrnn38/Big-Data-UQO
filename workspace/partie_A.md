# Partie A – Hive/HiveQL : réponses pour le rapport

Script : `workspace/01_hive_analyse.sql` · Sortie complète : `workspace/output/log_A.txt`

```bash
docker compose --profile hadoop up -d --build
# Terminal JupyterLab
bash /home/jovyan/work/00_deposer_dans_hdfs.sh
beeline -u jdbc:hive2://hive:10000/default -n jovyan \
  -f /home/jovyan/work/01_hive_analyse.sql
hdfs dfs -getmerge /inf6303/devoir1/sorties/segments /tmp/segments_utilisateurs.csv
cp /tmp/segments_utilisateurs.csv /home/jovyan/work/output/segments_utilisateurs.csv
```

Entrées : `data/evenements/*.csv.gz` (8 fichiers) et `data/catalogue_contenus.csv`, déposés dans HDFS.
Le script est rejouable : il recrée ses tables externes (`DROP` ne supprime que la définition,
jamais les fichiers HDFS) et réécrit le répertoire de sortie (`INSERT OVERWRITE DIRECTORY`).
Les résultats A2 sont **comparés dans la requête elle-même** aux valeurs de
`data/MANIFESTE_DONNEES.json` (colonne `controle`) : tous les contrôles valent `OK`.

---

## Section 3. Démarrage et contrôle du volume

### Nombre de fichiers et volume HDFS
```
$ hdfs dfs -ls /inf6303/devoir1/evenements
Found 8 items
-rw-r--r--   1 jovyan supergroup    3064906 ... evenements_part_01.csv.gz
...                                (8 fichiers de 3 064 220 à 3 065 291 octets)
$ hdfs dfs -du -s -h /inf6303/devoir1/evenements
23.4 M  23.4 M  /inf6303/devoir1/evenements
$ hdfs dfs -count -h /inf6303/devoir1/evenements
           1            8             23.4 M /inf6303/devoir1/evenements
```

| Mesure | Valeur |
|---|---:|
| Fichiers dans `/inf6303/devoir1/evenements` | **8** |
| Volume HDFS (compressé) | **24 517 828 octets (23,4 Mio)**, environ 2,9 Mio par fichier |
| Espace disque consommé (2e colonne de `du`) | 23,4 Mio (facteur de réplication 1, un seul DataNode) |
| Taille de bloc HDFS (`dfs.blocksize`) | 128 Mio |
| Blocs (`hdfs fsck`) | 8 : **un bloc par fichier** |
| Lignes physiques (`zcat \| wc -l`) | 1 000 008 = 1 000 000 événements + 8 en-têtes |
| Volume décompressé | 89 405 327 octets (85,3 Mio) |

Le catalogue est déposé dans `/inf6303/devoir1/catalogue` (1 fichier, 83 080 octets).

### Pourquoi 8 fichiers gzip donnent plus de parallélisme qu'un seul
- Dans MapReduce, une tâche *map* traite un *split*. Pour un fichier texte non compressé, Hadoop
  coupe le fichier en splits, en général un par bloc, et chaque mapper commence à lire au milieu
  du fichier, à la première fin de ligne.
- **gzip n'est pas découpable.** Un flux deflate ne peut être décodé qu'à partir de son début :
  on ne peut pas commencer la lecture au milieu du fichier. Hadoop le sait (`GzipCodec` n'est pas
  un `SplittableCompressionCodec`) et crée **un seul split par fichier .gz**, quelle que soit sa
  taille ou son nombre de blocs.
- Avec un seul fichier gzip, un seul mapper lirait donc le million de lignes, même sur un cluster
  de 100 nœuds. S'il dépassait 128 Mio, ce mapper devrait en plus aller lire les blocs suivants
  sur d'autres nœuds (perte de localité des données).
- Avec 8 fichiers, on obtient **8 splits, donc 8 mappers** qui lisent chacun 125 000 lignes en
  parallèle. On le constate dans `log_A.txt` : chaque requête sur `evenements_massifs` affiche
  `number of splits:8`. La jointure avec le catalogue affiche `number of splits:9` (8 + 1).
- Le découpage en amont, au moment de la production des fichiers, remplace le découpage que gzip
  interdit. Le degré de parallélisme maximal de la lecture est le nombre de fichiers. Des formats
  découpables (bzip2, ou ORC/Parquet compressés par blocs internes) lèveraient cette limite.
- Remarque sur cet environnement : YARN est instable (voir le README), donc les jobs tournent en
  MapReduce local. Les 8 tâches map s'exécutent alors dans la JVM de HiveServer2. Le découpage
  en 8 splits est le même que sur un cluster, mais le gain réel en temps n'apparaît qu'avec
  plusieurs nœuds.

### Écart entre le volume compressé et le nombre logique de lignes
- HDFS mesure des **octets stockés**, pas des lignes. `du` donne 23,4 Mio, qui sont des octets
  compressés. Ce chiffre ne permet pas de déduire le nombre d'événements.
- Le contenu logique est 3,65 fois plus gros : 89,4 Mo décompressés pour 24,5 Mo stockés. Une
  ligne CSV fait en moyenne 89 octets décompressés, mais seulement environ 24,5 octets
  compressés. gzip est très efficace ici, car les colonnes répètent peu de valeurs (8 pays,
  5 types, 4 appareils, 4 qualités) et les identifiants ont des préfixes communs.
- Le nombre logique de lignes n'est connu qu'après décompression et lecture : `zcat | wc -l`
  donne 1 000 008 lignes physiques. Il faut retirer les 8 en-têtes, un par fichier, ignorés par
  `skip.header.line.count`, pour obtenir **1 000 000 événements**, confirmé par `COUNT(*)` en A1.
- Conséquence pour Hive : le planificateur estime les volumes à partir des tailles de fichiers
  compressés. L'`EXPLAIN` ci-dessous annonce environ 224 522 lignes pour la table des événements,
  alors qu'il y en a 1 000 000. Sans `ANALYZE TABLE`, les statistiques (`Column stats: NONE`)
  sous-estiment fortement la donnée réelle.

---

## A1. Tables et qualité

### Tables externes typées
```sql
CREATE EXTERNAL TABLE evenements_massifs (          -- Franklin, inchangée
  event_id STRING, user_id STRING, content_id STRING, event_type STRING,
  event_ts STRING, country STRING, device STRING, watch_seconds INT,
  quality STRING, session_id STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '/inf6303/devoir1/evenements'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE catalogue_contenus (
  content_id   STRING,   -- clé de jointure avec evenements_massifs.content_id
  title        STRING,
  genre        STRING,
  release_year INT,
  duration_sec INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '/inf6303/devoir1/catalogue'
TBLPROPERTIES ('skip.header.line.count'='1');
```
- **EXTERNAL** : Hive ne possède pas les fichiers. `DROP TABLE` ne supprime que la définition,
  et les données déposées restent dans HDFS. Le schéma est appliqué à la lecture.
- **Typage** : les mesures sont en `INT` (`watch_seconds`, `duration_sec`, `release_year`).
  Une valeur non numérique devient `NULL` à la lecture, ce qui la rend détectable (contrôles
  ci-dessous). L'ordre des colonnes suit l'en-tête réel des fichiers.
- **Fins de ligne du catalogue** : `catalogue_contenus.csv` est fourni en CRLF, donc le `\r`
  reste collé à `duration_sec`. Ce n'est pas un problème : Hive convertit bien la valeur en `INT`
  (0 `NULL` sur 2 000, contrôle ci-dessous). Le fichier source n'a pas été modifié, il doit rester
  conforme à `SHA256SUMS.txt`.
- **Paramètres d'exécution** en tête du script (voir README) : MapReduce local, pas de map-join
  automatique (mémoire), et `HiveInputFormat`. Avec le format par défaut
  `CombineHiveInputFormat`, les 8 en-têtes étaient comptés comme des données (1 000 008).

### Volumes confirmés

| Contrôle | Obtenu | Attendu |
|---|---:|---:|
| `COUNT(*)` `evenements_massifs` | **1 000 000** | 1 000 000 |
| `COUNT(*)` `catalogue_contenus` | **2 000** | 2 000 |
| `COUNT(DISTINCT content_id)` catalogue | 2 000 | 2 000 |

### Contrôles de qualité et explication

| Contrôle | Règle | Résultat | Pourquoi ce contrôle |
|---|---|---:|---|
| `user_id` invalide | `NULL` ou vide | 0 | `user_id` est la clé d'interconnexion avec MongoDB et Cassandra. Une valeur vide créerait un faux utilisateur en A3. |
| Format `user_id` | `RLIKE '^u[0-9]{6}$'` | 0 | Une valeur tronquée ou un décalage de colonnes (virgule dans un champ) passe le test « non vide » mais pas celui du format. |
| Format `event_id` | `RLIKE '^e[0-9]{9}$'` | 0 | Même raison, pour la clé de l'événement. |
| `event_id` en double | `COUNT(*) - COUNT(DISTINCT event_id)` | 0 | Un doublon (fichier déposé deux fois, par exemple) gonflerait les comptages et les durées. |
| Format `content_id` | `RLIKE '^c[0-9]{5}$'` | 0 | `content_id` est la clé de jointure du top 10. |
| `content_id` absent du catalogue | `LEFT JOIN`, `c.content_id IS NULL` | 0 | La jointure interne du top 10 perdrait ces événements sans le signaler. Les 2 000 contenus du catalogue sont tous utilisés. |
| Unicité de la clé du catalogue | `COUNT(DISTINCT content_id)` = 2 000 | OK | Un `content_id` en double dans la dimension dupliquerait des lignes à la jointure et fausserait les sommes. |
| Durée `NULL` ou non numérique | `watch_seconds IS NULL` | 0 | Le typage `INT` transforme une valeur illisible en `NULL`. `SUM` l'ignorerait en silence. |
| Durée négative | `watch_seconds < 0` | 0 | Une durée négative est impossible et réduirait la durée totale d'un utilisateur. |
| Durée nulle (0 s) | `watch_seconds = 0` | 2 566 | Ces durées sont **valides** : toutes concernent des événements sans lecture (pause, recherche, abandon, favori). Il y a 0 lecture à 0 s. |
| Plage des durées | `MIN` et `MAX` | 0 à 7 200 s | Aucune valeur aberrante (au plus 2 h par événement). |
| Durée > durée du contenu | `watch_seconds > duration_sec` | 224 988 | Signalée mais **non exclue** (voir ci-dessous). |
| Catalogue | `duration_sec` `NULL` ou ≤ 0, `release_year` `NULL` | 0 / 0 | Durées de 308 à 8 395 s, années de 1990 à 2026. |

Durées par type d'événement (elles expliquent les 2 566 durées nulles) :

| event_type | événements | min (s) | max (s) | moyenne (s) |
|---|---:|---:|---:|---:|
| lecture | 549 714 | 20 | 7 200 | 3 611,1 |
| pause | 139 884 | 0 | 180 | 90,0 |
| recherche | 119 987 | 0 | 180 | 90,0 |
| abandon | 110 611 | 0 | 180 | 89,8 |
| ajout_favori | 79 804 | 0 | 180 | 90,0 |

**Durée regardée supérieure à la durée du contenu (224 988 événements, 22,5 %).** C'est un
contrôle de cohérence métier, pas une erreur de format. On le signale sans exclure ces lignes,
pour trois raisons :
- `watch_seconds` peut cumuler le temps d'une session (revisionnage, retour en arrière).
- Les exclure modifierait la sortie A3 transmise à MongoDB, qui doit couvrir tous les événements.
- En les gardant, le top 10 et les durées totales correspondent exactement au manifeste : ses
  valeurs attendues ont donc été calculées sur l'ensemble des événements.

**Bilan A1** : aucune ligne invalide. Le filtre de la requête A3 (`user_id` non vide, durée non
`NULL` et ≥ 0) ne retire rien : la somme des `event_count` de la sortie vaut bien 1 000 000.

---

## A2. Analyse distribuée

### Répartition des événements par pays et par type
Un seul passage sur le million de lignes produit le croisement pays × type (40 lignes, table
temporaire `repartition_pays_type`). Les deux répartitions simples sont ensuite dérivées de ces
40 lignes au lieu de relire le million d'événements deux fois.

**Par pays**, comparée à `events_by_country` du manifeste :

| Pays | Événements | % | Manifeste | Contrôle |
|---|---:|---:|---:|---|
| Canada | 311 247 | 31,12 | 311 247 | OK |
| Senegal | 132 617 | 13,26 | 132 617 | OK |
| Cote_d_Ivoire | 119 916 | 11,99 | 119 916 | OK |
| France | 118 790 | 11,88 | 118 790 | OK |
| Maroc | 98 758 | 9,88 | 98 758 | OK |
| Cameroun | 89 530 | 8,95 | 89 530 | OK |
| Belgique | 69 962 | 7,00 | 69 962 | OK |
| Tunisie | 59 180 | 5,92 | 59 180 | OK |

**Par type**, comparée à `events_by_type` :

| Type | Événements | % | Manifeste | Contrôle |
|---|---:|---:|---:|---|
| lecture | 549 714 | 54,97 | 549 714 | OK |
| pause | 139 884 | 13,99 | 139 884 | OK |
| recherche | 119 987 | 12,00 | 119 987 | OK |
| abandon | 110 611 | 11,06 | 110 611 | OK |
| ajout_favori | 79 804 | 7,98 | 79 804 | OK |

**Croisement pays × type** :

| Pays | lecture | pause | recherche | abandon | ajout_favori |
|---|---:|---:|---:|---:|---:|
| Belgique | 38 404 | 9 814 | 8 424 | 7 716 | 5 604 |
| Cameroun | 49 451 | 12 475 | 10 779 | 9 781 | 7 044 |
| Canada | 170 961 | 43 623 | 37 242 | 34 537 | 24 884 |
| Cote_d_Ivoire | 65 889 | 16 723 | 14 413 | 13 356 | 9 535 |
| France | 65 322 | 16 582 | 14 231 | 13 177 | 9 478 |
| Maroc | 54 558 | 13 786 | 11 692 | 10 798 | 7 924 |
| Senegal | 72 561 | 18 608 | 16 146 | 14 722 | 10 580 |
| Tunisie | 32 568 | 8 273 | 7 060 | 6 524 | 4 755 |

Lecture : le Canada représente près du tiers des événements, ce qui explique la partition chaude
Canada/actif discutée en partie C. Le mélange des types est presque identique d'un pays à l'autre
(environ 55 % de lectures partout) : les pays diffèrent par leur volume, pas par leur usage.

### Top 10 des contenus par durée totale (après jointure avec le catalogue)
```sql
SELECT e.content_id, c.title, c.genre, c.duration_sec,
       SUM(e.watch_seconds) AS duree_totale_sec, COUNT(*) AS nb_evenements
FROM evenements_massifs e
JOIN catalogue_contenus c ON e.content_id = c.content_id
GROUP BY e.content_id, c.title, c.genre, c.duration_sec
ORDER BY duree_totale_sec DESC
LIMIT 10;
```

| # | content_id | titre | genre | durée totale (s) | événements | Manifeste | Contrôle |
|---:|---|---|---|---:|---:|---|---|
| 1 | c00545 | Contenu_00545 | jeunesse | 1 226 712 | 564 | c00545 · 1 226 712 | OK |
| 2 | c01844 | Contenu_01844 | actualite | 1 221 503 | 517 | c01844 · 1 221 503 | OK |
| 3 | c00686 | Contenu_00686 | comedie | 1 216 833 | 526 | c00686 · 1 216 833 | OK |
| 4 | c00277 | Contenu_00277 | sport | 1 210 971 | 568 | c00277 · 1 210 971 | OK |
| 5 | c00553 | Contenu_00553 | science_fiction | 1 204 604 | 529 | c00553 · 1 204 604 | OK |
| 6 | c01825 | Contenu_01825 | drame | 1 201 793 | 524 | c01825 · 1 201 793 | OK |
| 7 | c00882 | Contenu_00882 | drame | 1 200 490 | 510 | c00882 · 1 200 490 | OK |
| 8 | c00912 | Contenu_00912 | comedie | 1 199 344 | 494 | c00912 · 1 199 344 | OK |
| 9 | c01263 | Contenu_01263 | jeunesse | 1 198 489 | 567 | c01263 · 1 198 489 | OK |
| 10 | c01769 | Contenu_01769 | jeunesse | 1 198 083 | 519 | c01769 · 1 198 083 | OK |

Le rang, le `content_id` et la durée totale sont identiques au manifeste pour les 10 lignes. La
jointure apporte le titre et le genre, qui n'existent pas dans les événements. Les écarts sont
faibles (1,23 M contre 1,20 M de secondes) : aucun contenu ne domine nettement, et trois des dix
sont des contenus jeunesse.

### Durée moyenne par appareil et qualité
`duree_moy_sec` porte sur tous les événements. `duree_moy_lecture_sec` ne garde que les lectures,
car les autres types ne durent que de 0 à 180 s et tirent la moyenne vers le bas.

| Appareil | Qualité | Événements | Durée moy. (s) | Durée moy. lectures (s) |
|---|---|---:|---:|---:|
| mobile | 4K | 52 292 | 2 028,6 | 3 606,9 |
| mobile | HD | 155 778 | 2 026,3 | 3 623,4 |
| mobile | FHD | 143 906 | 2 023,3 | 3 609,5 |
| mobile | SD | 47 933 | 2 012,9 | 3 595,9 |
| ordinateur | 4K | 27 203 | 2 032,4 | 3 596,4 |
| ordinateur | FHD | 75 425 | 2 028,9 | 3 608,2 |
| ordinateur | HD | 82 003 | 2 022,4 | 3 597,7 |
| ordinateur | SD | 25 236 | 2 014,5 | 3 599,4 |
| tablette | SD | 9 767 | 2 065,3 | 3 642,0 |
| tablette | 4K | 10 327 | 2 025,3 | 3 605,2 |
| tablette | FHD | 28 825 | 2 019,9 | 3 634,5 |
| tablette | HD | 31 232 | 2 010,6 | 3 591,6 |
| television | FHD | 111 535 | 2 034,5 | 3 621,5 |
| television | SD | 37 343 | 2 032,1 | 3 618,5 |
| television | HD | 121 069 | 2 028,1 | 3 608,8 |
| television | 4K | 40 126 | 2 011,3 | 3 605,9 |

Lecture des résultats :
- Les moyennes sont très proches : de 2 010 à 2 065 s sur tous les événements, et de 3 592 à
  3 642 s sur les lectures. L'appareil et la qualité ne changent pratiquement pas la durée d'une
  session dans ce jeu de données.
- Ce qui varie, c'est le **volume** : le mobile (399 909 événements) et la télévision (310 073)
  dominent, et les qualités HD et FHD sont les plus fréquentes.
- La plus forte moyenne (tablette/SD) est aussi le plus petit groupe (9 767 événements), donc le
  plus sensible au hasard. Aucun manifeste ne donne de valeur attendue pour cette analyse.

---

## EXPLAIN de la requête du top 10

Plan complet dans `log_A.txt`. Avec `hive.auto.convert.join=false`, Hive produit **3 jobs
MapReduce en chaîne** et une étape de lecture finale :

| Étape | Map | Shuffle (clé de partition) | Reduce |
|---|---|---|---|
| **Stage-1** | `TableScan e` (événements) et `TableScan c` (catalogue), filtre `content_id is not null`, projection | **`Reduce Output Operator` sur `content_id`, pour les deux tables** | `Join Operator` (`Inner Join 0 to 1`), puis `Group By` partiel (`mode: hash`) |
| Stage-2 | relecture du résultat intermédiaire | `content_id, title, genre, duration_sec` | `Group By` final (`mode: mergepartial`) |
| Stage-3 | relecture, `TopN Hash Memory Usage: 0.1` | **tri global** `sort order: -` sur `duree_totale_sec`, sans colonne de partition, donc un seul reducer | `Limit 10` |
| Stage-0 | `Fetch Operator`, `limit: 10` | – | – |

### Opération coûteuse : la jointure en phase reduce (shuffle) du Stage-1
- C'est une **jointure côté reduce** (*common join*). Pour réunir les lignes de même
  `content_id`, Hive envoie **les deux tables** dans le shuffle (`Reduce Output Operator`, clé
  `content_id`). Les 1 000 000 événements sont donc sérialisés, triés, transférés et fusionnés,
  alors que la table jointe ne fait que 2 000 lignes (83 Ko). C'est l'étape qui déplace le plus
  de données. C'est aussi le seul passage sur le million de lignes : les étapes suivantes ne
  traitent qu'environ 2 000 groupes.
- Sur un vrai cluster, ce shuffle traverse le réseau. Le reducer qui reçoit un `content_id`
  populaire reçoit aussi toutes ses lignes (risque de déséquilibre).
- **Amélioration** : une **map-join** (*broadcast*). Le catalogue de 83 Ko tient en mémoire. Avec
  `hive.auto.convert.join=true`, Hive le diffuse à chaque mapper et fait la jointure côté map :
  le Stage-1 n'a plus de shuffle d'événements. Elle est désactivée ici uniquement parce que la
  tâche locale qui prépare la table de hachage dépasse la mémoire du conteneur Hive (`-Xmx1G`,
  voir README). Autre option : agréger d'abord les événements par `content_id`, puis joindre les
  2 000 lignes agrégées.

### Deuxième point coûteux : le tri global (Stage-3)
- `ORDER BY` impose un ordre total : le plan n'a **pas de colonne de partition**, et tout passe
  par **un seul reducer**. Sur un grand résultat, c'est un goulot d'étranglement qu'on ne peut
  pas paralléliser.
- Ici, le coût est limité pour deux raisons : le tri ne porte que sur environ 2 000 lignes
  agrégées, et Hive applique le `LIMIT` dès la phase map (`TopN Hash Memory Usage: 0.1`). Chaque
  mapper n'envoie que ses 10 meilleures lignes au reducer unique.

### Statistiques du plan
Les estimations (`Num rows: 224522` pour les événements, `1675` pour le catalogue) sont fausses,
car `Column stats: NONE` : Hive les déduit de la taille des fichiers, compressés pour les
événements. Un `ANALYZE TABLE ... COMPUTE STATISTICS` donnerait de vraies cardinalités. Le choix
automatique d'une map-join repose justement sur ces tailles estimées.

---

## A3. Sortie transmise à MongoDB (résumé)

Zone TODO A4 du gabarit, réalisée par Franklin et **non modifiée**.

- Agrégation par `user_id` : `MAX(country)` (le contrôle préalable donne 0 utilisateur avec
  plusieurs pays, donc le pays est unique), `COUNT(*)` pour `event_count` et
  `SUM(watch_seconds)` pour `total_watch_sec`.
- Segment selon la durée totale (seuils imposés) : **intensif ≥ 55 000 s**, **actif ≥ 30 000 s**,
  sinon **occasionnel**.
- Écriture dans `/inf6303/devoir1/sorties/segments` (`INSERT OVERWRITE DIRECTORY`, CSV sans
  en-tête : `user_id,country,event_count,total_watch_sec,segment`), puis relecture par une table
  externe `segments_sortie` pour les contrôles.

| Contrôle | Obtenu | Attendu |
|---|---:|---:|
| Utilisateurs avec plusieurs pays | 0 | 0 |
| Lignes de sortie / `user_id` distincts | 50 000 / 50 000 | 50 000 |
| Somme des `event_count` | 1 000 000 | 1 000 000 |
| actif (30 003 – 54 999 s) | 30 901 | 30 901 (manifeste) |
| occasionnel (1 574 – 29 999 s) | 11 669 | 11 669 (manifeste) |
| intensif (55 000 – 106 412 s) | 7 430 | 7 430 (manifeste) |

Les bornes min et max de chaque segment respectent les seuils. Les trois effectifs sont
identiques au manifeste.

**Rapatriement** : `hdfs dfs -getmerge` directement vers `/home/jovyan/work/output` échoue sous
Windows (`getmerge: Operation not permitted`). Le client Hadoop tente de fixer les permissions
POSIX du fichier local, ce que refuse le dossier monté depuis Windows. On fusionne donc dans
`/tmp` du conteneur, puis on copie : le contenu est le même.

| Contrôle du fichier | Valeur |
|---|---|
| `wc -l segments_utilisateurs.csv` | 50 000 |
| SHA-256 | `cd53b4ffcda7fb4847aeca73f265b0a0ce2cdf76b6afe168702904ebd6f59aab` |
| Comparaison avec la version utilisée par MongoDB (parties B, C, D1) | **identique** |

La réexécution complète de la partie A redonne exactement le fichier consommé en aval.
L'enrichissement MongoDB et la table Cassandra restent donc valides sans être rejoués.
