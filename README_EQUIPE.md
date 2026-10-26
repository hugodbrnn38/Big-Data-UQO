# INF6303 – Devoir 1 : Pipeline analytique polyglotte (Hive → MongoDB → Cassandra)

## Membres et répartition du travail

| Rôle | Partie | Responsable | Échéance | Statut |
|---|---|---|---|---|
| P1 | Hive section 3 (volume HDFS) + A1 + A2 → gabarit **TODO A2 + A3** | Mikael | Dim. 11 oct. | Terminé |
| P2 | Hive A3 + D2 (architecture) → gabarit **TODO A1 + A4** | Franklin | Dim. 11 oct. | Terminé |
| P3 | MongoDB B1 + B2 + B3 | Gaston | Ven. 16 oct. | Terminé |
| P4 | Cassandra C1 + C2 + C3 | Sabra | Mar. 20 oct. | Terminé |
| P5 | Validation D1 + intégration (reproductibilité, rapport) | Hugo | Sam. 24 oct. | Terminé |

Réponses rédigées pour le rapport : `workspace/partie_A.md`, `workspace/partie_C.md`,
`workspace/partie_D1.md` ; partie B dans ce fichier (contrat `audience_cible.jsonl`) ;
diagramme D2 : `rapport/architecture_D2.png`.

Revue croisée : P1 → P2 → P3 → P4 → P5 → P1.

### Règle commune : compléter les gabarits du professeur
> « Complétez les zones TODO. Ne modifiez pas les chemins imposés. »

Chacun travaille **directement dans le fichier fourni** (`01_hive_analyse.sql`, `02_mongodb_enrichissement.py`, `03_cassandra_publication.py`), uniquement dans ses zones TODO. Pas de script parallèle. Les chemins HDFS, la base `inf6303_devoir1` et les noms de tables imposés ne changent pas.

### Correspondance énoncé ↔ zones TODO de `01_hive_analyse.sql`
| Zone du gabarit | Contenu | Partie de l'énoncé | Responsable | Statut |
|---|---|---|---|---|
| TODO A1 | table externe `evenements_massifs` (10 colonnes), `/inf6303/devoir1/evenements` | A1 – tables | Franklin | Fait |
| TODO A2 | table externe `catalogue_contenus` (5 colonnes), `/inf6303/devoir1/catalogue` | A1 – tables | Mikael | Fait |
| TODO A3 | contrôles de qualité, analyses et EXPLAIN | A1 – qualité + A2 – analyses | Mikael | Fait |
| TODO A4 | segment par utilisateur, export vers `/inf6303/devoir1/sorties/segments` | **A3** – sortie vers MongoDB | Franklin | Fait |

---

## Contrats d'interface (fichiers de passage)

### `output/segments_utilisateurs.csv` (Hive → MongoDB)
- Produit par : `01_hive_analyse.sql` (zone TODO A4), tables dans la base `inf6303_devoir1`
- **Sans en-tête**, séparateur `,`, 50 000 lignes (une par `user_id`)
- Colonnes, dans cet ordre :

| # | Colonne | Type | Description |
|---|---|---|---|
| 1 | `user_id` | texte | clé d'interconnexion (ex. `u000001`) |
| 2 | `country` | texte | pays unique de l'utilisateur (ex. `Canada`, `Cote_d_Ivoire`) |
| 3 | `event_count` | entier | nombre d'événements |
| 4 | `total_watch_sec` | entier | `SUM(watch_seconds)` |
| 5 | `segment` | texte | `intensif` / `actif` / `occasionnel` |

- Lecture Python recommandée :
  ```python
  pd.read_csv("output/segments_utilisateurs.csv", header=None,
              names=["user_id","country","event_count","total_watch_sec","segment"])
  ```

### `output/audience_cible.jsonl` (MongoDB → Cassandra)

- Produit par : `02_mongodb_enrichissement.py` (B3).
- Format : JSON Lines (JSONL), encodage UTF-8.
- Nombre d'enregistrements : **34 855**.
- Taille : **4 990 244 octets** (environ 4,76 Mio).
- Une ligne correspond à un profil utilisateur.
- Chaque profil possède un identifiant `user_id` unique.

#### Critères de sélection

Seuls les profils respectant simultanément les conditions suivantes sont exportés :

1. `consent_analytics = true`
2. `analytics.segment` appartient à `["intensif", "actif"]`

#### Structure des documents

| Champ | Type | Description |
|---|---|---|
| `user_id` | string | Identifiant unique de l'utilisateur |
| `country` | string | Pays de l'utilisateur |
| `plan` | string | Type d'abonnement |
| `analytics.nb_events` | integer | Nombre d'événements |
| `analytics.total_watch_sec` | integer | Durée totale de visionnement en secondes |
| `analytics.segment` | string | Segment analytique |

Exemple de document :

```json
{"user_id":"u000001","country":"Senegal","plan":"gratuit","analytics":{"nb_events":26,"total_watch_sec":60487,"segment":"intensif"}}
```

#### Résultats de l'exportation

| Indicateur | Valeur |
|---|---:|
| Profils MongoDB initiaux | 50 000 |
| Profils admissibles | 34 855 |
| Profils exportés | 34 855 |
| Profils intensifs | 6 784 |
| Profils actifs | 28 071 |
| Profils exclus | 15 145 |
| Réduction du nombre d'enregistrements | 30,29 % |
| Erreurs de validation | 0 |

Le fichier est destiné à la partie C – Cassandra.

Score utilisé pour le classement Cassandra : `analytics.total_watch_sec` (durée totale calculée
par Hive), sans transformation (voir `workspace/partie_C.md`).

---

## Environnement : correctifs appliqués

L'environnement fourni a nécessité les ajustements suivants. Ils sont indispensables pour rejouer le pipeline.

### 1. Impersonation HiveServer2
Erreur : `User: hive is not allowed to impersonate jovyan`
Correctif, dans `hive-config/hive-site.xml`, avant `</configuration>` :
```xml
<property><name>hive.server2.enable.doAs</name><value>false</value></property>
```

### 2. Hive ne redémarre pas (`HiveServer2 running as process 7. Stop it first.`)
L'image `apache/hive:4.0.0` garde un ancien fichier PID au redémarrage. Ne pas utiliser `restart` ; **recréer** le conteneur :
```powershell
docker compose --profile hadoop rm -sf hive
docker compose --profile hadoop up -d --force-recreate hive
```

### 3. YARN instable (ResourceManager / NodeManager en `Exited 255`)
Les requêtes restaient bloquées à la soumission du job. `01_hive_analyse.sql` s'exécute donc en **MapReduce local** (dans HiveServer2). Ces lignes figurent en tête du fichier, juste après `USE inf6303_devoir1;` :
```sql
SET mapreduce.framework.name=local;
SET hive.exec.mode.local.auto=false;
SET hive.auto.convert.join=false;   -- la tâche locale de map-join manquait de mémoire
```
Le résultat est identique ; seul le lieu d'exécution change.

### 4. En-têtes des fichiers compressés comptés comme données
Avec le format d'entrée par défaut (`CombineHiveInputFormat`), `skip.header.line.count` n'était pas appliqué : `COUNT(*)` = **1 000 008** au lieu de 1 000 000.
```sql
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
```
Après correction : **1 000 000** événements.

### 5. Metastore Hive impossible à créer (`Schema initialization failed!`)
Erreur dans `docker compose logs hive` : `Failed to create database '/opt/hive/data/metastore_db'`.
Quand le volume `hive-data` est recréé (après un `down -v`, par exemple), sa racine peut appartenir
à `root`, alors que Hive tourne avec l'utilisateur `hive` (uid 1000) : il ne peut pas y écrire.
Correctif (PowerShell), puis recréer Hive comme au point 2 :
```powershell
docker run --rm -v inf6303-devoir1_hive-data:/d alpine chown 1000:1000 /d
docker compose --profile hadoop rm -sf hive
docker compose --profile hadoop up -d --force-recreate hive
```

### 6. `getmerge` refusé sous Windows (`getmerge: Operation not permitted`)
`hdfs dfs -getmerge` écrit le fichier local puis tente d'en fixer les permissions POSIX, ce que
refuse le dossier `workspace/` monté depuis Windows. Le fichier reste vide (0 ligne).
Procédure corrigée : fusionner dans `/tmp` du conteneur, puis copier (contenu identique) :
```bash
rm -f /tmp/segments_utilisateurs.csv /home/jovyan/work/output/segments_utilisateurs.csv
hdfs dfs -getmerge /inf6303/devoir1/sorties/segments /tmp/segments_utilisateurs.csv
cp /tmp/segments_utilisateurs.csv /home/jovyan/work/output/segments_utilisateurs.csv
```

### 7. `JAVA_HOME is not set` avec `docker compose exec`
L'image du notebook ne définit pas `JAVA_HOME` : `hdfs` et `beeline` échouent lorsqu'on les lance
depuis PowerShell par `docker compose exec`. Passer la variable à chaque commande :
```powershell
docker compose exec -e JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 notebook hdfs dfs -ls /inf6303/devoir1
```

### 8. Fins de ligne Windows (CRLF)
Avec `core.autocrlf=true`, un script extrait en CRLF casse bash dans le conteneur
(`set: pipefail: invalid option name`). Le fichier `.gitattributes` impose LF pour `*.sh`, `*.sql`,
`*.csv` et `*.jsonl` ; `data/` est exclu pour rester conforme à `SHA256SUMS.txt`.

---

## Procédure de reproduction (depuis un environnement remis à zéro)

> Deux types de terminal sont utilisés : **PowerShell (Windows)** dans le dossier du projet, et **Terminal JupyterLab (Linux)**.

### Étape 0 – Démarrage et dépôt HDFS
**PowerShell (Windows)**
```powershell
docker compose --profile hadoop --profile nosql down -v      # remise à zéro complète
docker compose --profile hadoop up -d --build
# JupyterLab : http://localhost:8888/lab?token=inf6303
```
**Terminal JupyterLab (Linux)**
```bash
bash /home/jovyan/work/00_deposer_dans_hdfs.sh
hdfs dfs -du -h /inf6303/devoir1/evenements
hdfs dfs -ls /inf6303/devoir1/evenements
```

### Étape 1 – Partie A : Hive
**Terminal JupyterLab (Linux)**
```bash
mkdir -p /home/jovyan/work/output
beeline -u jdbc:hive2://hive:10000/default -n jovyan \
  -f /home/jovyan/work/01_hive_analyse.sql 2>&1 | tee /home/jovyan/work/output/log_A.txt

# getmerge via /tmp : l'écriture directe dans work/output échoue sous Windows (correctif 6)
rm -f /tmp/segments_utilisateurs.csv /home/jovyan/work/output/segments_utilisateurs.csv
hdfs dfs -getmerge /inf6303/devoir1/sorties/segments /tmp/segments_utilisateurs.csv
cp /tmp/segments_utilisateurs.csv /home/jovyan/work/output/segments_utilisateurs.csv
wc -l /home/jovyan/work/output/segments_utilisateurs.csv   # attendu : 50000
sha256sum /home/jovyan/work/output/segments_utilisateurs.csv
# attendu : cd53b4ffcda7fb4847aeca73f265b0a0ce2cdf76b6afe168702904ebd6f59aab
```

Contrôles attendus (exécution du 25 oct. 2026, détail dans `workspace/partie_A.md`) :

| Contrôle | Valeur |
|---|---|
| Fichiers HDFS / volume compressé | 8 / 23,4 Mio (un split par fichier gzip) |
| Événements / contenus | 1 000 000 / 2 000 |
| Lignes invalides (user_id vide / durée NULL / durée < 0) | 0 / 0 / 0 |
| `event_id` en double / `content_id` absents du catalogue | 0 / 0 |
| Répartitions pays et type, top 10 des contenus | identiques à `MANIFESTE_DONNEES.json` |
| Utilisateurs multi-pays | 0 |
| Utilisateurs en sortie | 50 000 (distincts) |
| Somme des `event_count` | 1 000 000 |
| actif / occasionnel / intensif | 30 901 / 11 669 / 7 430 |

### Étape 2 – Partie B : MongoDB

Cette étape importe les profils utilisateurs, crée les index MongoDB,
enrichit les documents avec les résultats Hive et exporte l'audience
cible destinée à Cassandra.

#### Prérequis

Le fichier suivant doit avoir été généré par la partie A :

`workspace/output/segments_utilisateurs.csv`

Il contient 50 000 enregistrements.

Le fichier documentaire source est :

`data/profils_utilisateurs.jsonl.gz`

#### Démarrage des services

**PowerShell (Windows)**

```powershell
docker compose --profile hadoop stop
docker compose --profile nosql up -d --build
```

Vérifier les services :

```powershell
docker compose ps
```

#### Exécution de MongoDB

**PowerShell (Windows)**

```powershell
docker compose exec notebook python /home/jovyan/work/02_mongodb_enrichissement.py
```

Le script exécute successivement :

- B1 : importation des 50 000 profils documentaires ;
- B1 : création des index MongoDB ;
- B2 : enrichissement avec les segments produits par Hive ;
- B3 : sélection et exportation de l'audience cible.

#### Validation du livrable

```powershell
docker compose exec notebook python /home/jovyan/work/validation_mongodb.py
```

Résultats obtenus :

```text
=== VALIDATION DU LIVRABLE MONGODB ===
Documents analyses : 34855
Identifiants uniques : 34855
Segments intensifs : 6784
Segments actifs : 28071
Erreurs detectees : 0

VALIDATION MONGODB REUSSIE
```

#### Fichier produit

`workspace/output/audience_cible.jsonl`

Ce fichier contient 34 855 profils sélectionnés et constitue
l'entrée de la partie C – Cassandra.

### Étape 3 – Partie C : Cassandra

#### Prérequis
`workspace/output/audience_cible.jsonl` produit par la partie B (34 855 documents).
Le profil `nosql` est déjà démarré à l'étape 2.

#### Attendre Cassandra
Cassandra peut demander jusqu'à deux minutes avant d'accepter les connexions. Attendre que le
service soit `healthy` :

**PowerShell (Windows)**
```powershell
docker compose ps cassandra      # STATUS doit afficher « (healthy) »
```
Le script réessaie de toute façon la connexion toutes les 10 s pendant 3 minutes.

#### Exécution
**Terminal JupyterLab (Linux)**
```bash
python /home/jovyan/work/03_cassandra_publication.py 2>&1 | tee /home/jovyan/work/output/log_C.txt
```

Le script est rejouable (il supprime et recrée ses tables) et exécute :
- C1 : keyspace `inf6303_devoir1`, table `audience_par_pays_segment`,
  `PRIMARY KEY ((country, segment), score, user_id)`, tri `score DESC, user_id ASC` ;
- C2 : lecture de `audience_cible.jsonl`, insertion par requête préparée, comptage par partition ;
- C3 : top 10 Canada/intensif et France/actif, refus de la recherche globale par plan,
  table supplémentaire `audience_par_plan`.

Contrôles attendus (`output/log_C.txt`, détail dans `workspace/partie_C.md`) :

| Contrôle | Valeur |
|---|---|
| Documents lus dans `audience_cible.jsonl` | 34 855 |
| Lignes chargées / échecs | 34 855 / 0 |
| Partitions (8 pays × 2 segments) | 16 |
| `audience_par_plan` (gratuit / premium / standard) | 16 831 / 6 352 / 11 672 |

### Étape 4 – Partie D1 : Validation de bout en bout

#### Prérequis
Étapes 1 à 3 exécutées : CSV Hive présent, collection MongoDB `profils` enrichie, table Cassandra
chargée. Le profil `nosql` doit être démarré (MongoDB et Cassandra sont interrogés).

#### Exécution
**Terminal JupyterLab (Linux)**
```bash
python /home/jovyan/work/04_validation_pipeline.py 2>&1 | tee /home/jovyan/work/output/log_D.txt
```
Autres `user_id` à suivre en argument : `python 04_validation_pipeline.py u000004 u000006`.

Le script ne modifie rien. Il compte chaque étape, compare les valeurs utilisateur par utilisateur
entre Hive, MongoDB, le JSONL et Cassandra, et suit trois `user_id`. Il se termine avec un code non
nul au moindre écart.

Contrôles attendus (`output/log_D.txt`, détail dans `workspace/partie_D1.md`) :

| Contrôle | Valeur |
|---|---|
| Hive CSV / profils MongoDB / profils enrichis | 50 000 / 50 000 / 50 000 |
| `audience_cible.jsonl` / lignes Cassandra | 34 855 / 34 855 |
| Exclus car occasionnel / car non consentant | 11 669 / 3 476 |
| Valeurs différentes entre étapes | 0 |
| Résultat | `VALIDATION DU PIPELINE REUSSIE` |

---

## Contenu de l'archive de remise
- `rapport.pdf`
- `01_hive_analyse.sql`, `02_mongodb_enrichissement.py`, `03_cassandra_publication.py`
- `output/segments_utilisateurs.csv`, `output/audience_cible.jsonl`
- `README_EQUIPE.md`
- `rapport/` (sources du rapport) : `architecture_D2.png` (diagramme D2)

Ne pas inclure les volumes Docker ni les fichiers sources massifs (`data/`).

## Aide au diagnostic
- `docker compose --profile hadoop ps -a` : état des services
- `docker compose --profile hadoop logs --tail=100 hive` : journal HiveServer2
- `hdfs dfs -ls -R /inf6303/devoir1` : arborescence HDFS
