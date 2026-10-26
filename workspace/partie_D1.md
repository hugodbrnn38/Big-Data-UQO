# Partie D1 – Traçabilité : réponses pour le rapport

Script : `workspace/04_validation_pipeline.py` · Sortie complète : `workspace/output/log_D.txt`

```bash
docker compose --profile nosql up -d --build
# attendre que le service cassandra soit "healthy" (~2 min)
docker compose exec notebook python /home/jovyan/work/04_validation_pipeline.py
```

Prérequis : les parties A, B et C ont été exécutées (CSV Hive présent, collection MongoDB
`profils` enrichie, table Cassandra `audience_par_pays_segment` chargée).
Le script ne modifie rien : il lit les quatre étapes et échoue (code de sortie non nul) au moindre
écart. D'autres `user_id` peuvent être suivis en argument :
`python 04_validation_pipeline.py u000004 u000006`.

---

## D1.1 Comptages à chaque étape

Chaque étape est relue dans son propre système, sans réutiliser les compteurs des scripts
précédents :

- **Hive** : lecture de `output/segments_utilisateurs.csv` (résultat du `getmerge`).
- **MongoDB** : `count_documents` sur la collection `inf6303_devoir1.profils`.
- **Fichier de passage** : lecture de `output/audience_cible.jsonl`.
- **Cassandra** : lecture des partitions par leur clé `(country, segment)`, comme le fait le
  service. On lit les 8 pays × 3 segments, soit 24 partitions, et non un `COUNT(*)` global qui
  balaierait tout le cluster. Les 8 partitions `occasionnel` sont lues pour prouver qu'elles sont
  vides.

| Étape | Système | Lignes |
|---|---|---:|
| 1 | Hive : `segments_utilisateurs.csv` | **50 000** |
| 2 | MongoDB : profils | **50 000** |
| | MongoDB : profils enrichis (`analytics` présent) | **50 000** |
| | MongoDB : consentants intensif/actif (filtre B3) | 34 855 |
| 3 | `audience_cible.jsonl` | **34 855** |
| 4 | Cassandra : `audience_par_pays_segment` (16 partitions non vides) | **34 855** |

### Pourquoi on passe de 50 000 à 34 855
Le filtre B3 (`consent_analytics = true` et `analytics.segment ∈ {intensif, actif}`) est la seule
réduction du pipeline. Les 15 145 profils exclus se décomposent ainsi :

| Segment Hive | Consentants | Non consentants |
|---|---:|---:|
| intensif | 6 784 → exportés | 646 → exclus |
| actif | 28 071 → exportés | 2 830 → exclus |
| occasionnel | 10 642 → exclus | 1 027 → exclus |

- Exclus car occasionnel : 10 642 + 1 027 = **11 669** (le même nombre que la sortie Hive A3).
- Exclus car intensif ou actif mais non consentants : 646 + 2 830 = **3 476**.
- 34 855 + 11 669 + 3 476 = **50 000** : aucun profil ne disparaît sans raison.

### Contrôles de cohérence (tous réussis)
Les contrôles comparent les **valeurs**, pas seulement les nombres de lignes, et portent sur
**tous** les utilisateurs, pas seulement sur les trois suivis.

| Contrôle | Obtenu | Attendu |
|---|---:|---:|
| Hive : `user_id` distincts | 50 000 | 50 000 |
| Hive : somme des `nb_events` | 1 000 000 | 1 000 000 |
| MongoDB : profils = lignes Hive | 50 000 | 50 000 |
| MongoDB : profils enrichis = profils | 50 000 | 50 000 |
| JSONL : lignes = admissibles MongoDB | 34 855 | 34 855 |
| JSONL + exclus = profils | 50 000 | 50 000 |
| Cassandra : lignes = lignes JSONL | 34 855 | 34 855 |
| Cassandra : lignes `occasionnel` | 0 | 0 |
| Hive → MongoDB : profils dont pays ou `analytics` diffère | 0 | 0 |
| MongoDB → JSONL : documents dont pays, plan ou `analytics` diffère | 0 | 0 |
| JSONL → Cassandra : lignes dont une colonne diffère | 0 | 0 |
| Cassandra : `user_id` absents du JSONL | 0 | 0 |

La somme des `nb_events` vaut 1 000 000 : chaque événement Hive est compté une fois, et seul cet
agrégat quitte HDFS. Le million d'événements n'est chargé ni dans MongoDB ni dans Cassandra.

---

## D1.2 Traçabilité de trois `user_id`

Les trois utilisateurs sont réels et pris dans les fichiers produits, sans aucun identifiant
fabriqué. Ils couvrent les trois cas du pipeline :

| `user_id` | Cas |
|---|---|
| u000001 | intensif, consentant → publié |
| u000003 | actif, consentant → publié |
| u000002 | occasionnel **et** non consentant → arrêté dans MongoDB |

### Tableau comparatif

`duree/score` = `total_watch_sec` dans Hive, MongoDB et le JSONL ; `score` dans Cassandra.

| `user_id` | Étape | pays | nb_events | durée / score (s) | segment | plan | consentement |
|---|---|---|---:|---:|---|---|---|
| **u000001** | Hive (CSV) | Senegal | 26 | 60 487 | intensif | – | – |
| | MongoDB | Senegal | 26 | 60 487 | intensif | gratuit | true |
| | `audience_cible.jsonl` | Senegal | 26 | 60 487 | intensif | gratuit | – |
| | Cassandra | Senegal | 26 | 60 487 | intensif | gratuit | – |
| **u000003** | Hive (CSV) | Cote_d_Ivoire | 27 | 45 954 | actif | – | – |
| | MongoDB | Cote_d_Ivoire | 27 | 45 954 | actif | standard | true |
| | `audience_cible.jsonl` | Cote_d_Ivoire | 27 | 45 954 | actif | standard | – |
| | Cassandra | Cote_d_Ivoire | 27 | 45 954 | actif | standard | – |
| **u000002** | Hive (CSV) | Canada | 17 | 26 137 | occasionnel | – | – |
| | MongoDB | Canada | 17 | 26 137 | occasionnel | premium | **false** |
| | `audience_cible.jsonl` | *absent* | | | | | |
| | Cassandra | *absent* | | | | | |

### Explication des différences

**u000001 (Senegal, intensif)** : les valeurs sont identiques aux quatre étapes.
- 60 487 s ≥ 55 000 s, donc segment intensif, conformément au seuil Hive.
- Dans Cassandra, la ligne se trouve dans la partition `(Senegal, intensif)` avec `score = 60 487`,
  c'est-à-dire la durée calculée par Hive, recopiée sans transformation.

**u000003 (Cote_d_Ivoire, actif)** : les valeurs sont identiques aux quatre étapes.
- 30 000 s ≤ 45 954 s < 55 000 s, donc segment actif.
- La ligne se trouve dans la partition `(Cote_d_Ivoire, actif)`.

**u000002 (Canada, occasionnel)** : présent dans Hive et MongoDB, absent ensuite. C'est le
comportement attendu, pas une perte de données.
- Il est **enrichi** dans MongoDB : le sous-document `analytics` reprend exactement les valeurs Hive.
  MongoDB garde le profil complet de tous les utilisateurs (document opérationnel).
- Il est **exclu par le filtre B3** pour deux raisons indépendantes : segment occasionnel
  (26 137 s < 30 000 s) et `consent_analytics = false`. Une seule aurait suffi.
- Il n'arrive donc ni dans `audience_cible.jsonl` ni dans Cassandra. Le script l'a cherché dans les
  24 partitions de la table, y compris `(Canada, occasionnel)`, qui est vide.
- Cas complémentaires, vérifiés avec `04_validation_pipeline.py u000004 u000006` :
  - u000004 (Canada, actif, 51 017 s) est exclu **uniquement** pour son consentement (`false`).
  - u000006 (France, occasionnel, 22 735 s) est exclu **uniquement** pour son segment, bien qu'il
    soit consentant.

### Ce qui change d'une étape à l'autre (sans être une incohérence)

| Différence | Où | Raison |
|---|---|---|
| `plan` et `consent_analytics` apparaissent | MongoDB | Ils ne viennent pas des événements mais du profil (`profils_utilisateurs.jsonl.gz`). Hive ne les connaît pas. |
| `consent_analytics` disparaît | JSONL, Cassandra | Il a servi de filtre : toutes les lignes exportées valent `true`, le transférer n'apporterait rien. |
| `total_watch_sec` devient `score` | Cassandra | Il s'agit d'un renommage : c'est la colonne de clustering qui trie le top N (voir partie C). La valeur est identique. |
| `country` et `segment` deviennent la clé de partition | Cassandra | Ils sont imposés par la requête de service « top d'un pays et d'un segment ». |
| Le pays existe deux fois en amont | Hive (événements) et MongoDB (profil) | Les deux sources concordent pour les 50 000 utilisateurs (0 écart). Le JSONL reprend le pays du profil. |

---

## Conclusion D1
- `user_id` relie bien les trois systèmes : chaque ligne Cassandra se retrouve, avec les mêmes
  valeurs, dans le JSONL, dans MongoDB et dans la sortie Hive.
- La seule réduction (50 000 → 34 855) est le filtre métier B3, et elle est entièrement expliquée.
- Aucune valeur n'est recalculée ni inventée après Hive : la durée et le nombre d'événements de
  Cassandra sont ceux de l'agrégation HiveQL.
