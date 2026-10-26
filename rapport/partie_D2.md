# Partie D2 – Architecture du pipeline

Figure : `rapport/architecture_D2.png` (source vectorielle : `rapport/architecture_D2.svg`)

## Composants et flux

| Flèche | De → vers | Ce qui circule | Étape |
|---|---|---|---|
| **déposer** | Sources → HDFS | `evenements/*.csv.gz` (8 fichiers, 1 000 000 lignes) et `catalogue_contenus.csv` (2 000 contenus) | Section 3, `00_deposer_dans_hdfs.sh` |
| **agréger** | HDFS → Hive | Tables externes typées lues sur HDFS, puis agrégation par `user_id` et attribution du segment | A1, A2, A3 |
| **exporter** | Hive → fichier de passage 1 | `segments_utilisateurs.csv` : 50 000 lignes, sans en-tête (`INSERT OVERWRITE DIRECTORY` + `hdfs dfs -getmerge`) | A3 |
| **déposer** | Sources → MongoDB | `profils_utilisateurs.jsonl.gz` : 50 000 profils importés avec leurs sous-documents | B1 |
| **enrichir** | Fichier de passage 1 → MongoDB | Mise à jour de chaque profil par `user_id` avec le sous-document `analytics` | B2 |
| **exporter** | MongoDB → fichier de passage 2 | `audience_cible.jsonl` : 34 855 profils consentants des segments intensif et actif | B3 |
| **publier** | Fichier de passage 2 → Cassandra | Chargement par requête préparée dans `audience_par_pays_segment` | C2 |
| **interroger** | Cassandra → application | Top N d'un couple (pays, segment), trié par score décroissant | C3 |

## Trois responsabilités distinctes

- **Stockage analytique (HDFS + Hive).** Il conserve le volume massif (1 000 000 événements) et l'analyse en lot. Le schéma est appliqué à la lecture par des tables externes. Les 8 fichiers gzip permettent 8 lectures parallèles, car un fichier gzip n'est pas découpable. Les événements bruts ne quittent jamais HDFS : seuls les agrégats par `user_id` sont transférés.
- **Document opérationnel (MongoDB).** Il conserve le profil complet et souple de chaque utilisateur (préférences, appareils), enrichi par les indicateurs de Hive dans un sous-document `analytics`. Les mises à jour se font par `user_id`, grâce à un index unique. L'index composé `(consent_analytics, analytics.segment)` supporte le filtre de ciblage.
- **Vue de service (Cassandra).** C'est une table dénormalisée conçue pour une seule requête : le top des utilisateurs d'un pays et d'un segment. La clé de partition `(country, segment)` fait qu'une requête lit une seule partition. Le clustering `score DESC, user_id ASC` donne des lignes déjà triées sur disque.

La clé d'interconnexion `user_id` est conservée de bout en bout. À chaque étape, on ne transfère que ce qui est utile à l'étape suivante : 1 000 000 événements → 50 000 lignes agrégées → 50 000 profils enrichis → 34 855 documents → 34 855 lignes.

## Point de reprise

Le fichier **`audience_cible.jsonl`** sert de point de reprise entre MongoDB et Cassandra.

- Si le chargement dans Cassandra échoue (Cassandra pas encore prête, panne pendant l'insertion), on relance seulement `03_cassandra_publication.py` à partir de ce fichier. Il n'est pas nécessaire de rejouer Hive (l'étape la plus coûteuse) ni MongoDB.
- La reprise est idempotente : le script supprime et recrée la table avant de charger. Comme la clé primaire contient `user_id`, rejouer une insertion écrase la même ligne au lieu de créer un doublon.
- Le contrôle « 34 855 documents exportés = 34 855 lignes chargées » confirme que la reprise est complète.

De la même façon, `segments_utilisateurs.csv` joue le rôle de point de reprise entre Hive et MongoDB : en cas d'échec de l'enrichissement, on relance `02_mongodb_enrichissement.py` sans recalculer les agrégats Hive.
