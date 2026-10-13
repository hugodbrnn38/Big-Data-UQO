# INF6303 – Devoir 1 : Pipeline analytique polyglotte (Hive → MongoDB → Cassandra)

## Membres et répartition du travail

| Rôle | Partie | Responsable | Échéance | Statut |
|---|---|---|---|---|
| P1 | Hive A1 + A2 + section 3 (volume HDFS) → gabarit **TODO A2 + A3** | Mikael | Dim. 11 oct. | À faire |
| P2 | Hive A3 + D2 (architecture) → gabarit **TODO A1 + A4** | Franklin | Dim. 11 oct. | A3 livré · D2 livré |
| P3 | MongoDB B1 + B2 + B3 | Gaston | Ven. 16 oct. | À faire |
| P4 | Cassandra C1 + C2 + C3 | Sabra | Mar. 20 oct. | À faire |
| P5 | Intégration D1 + reproductibilité + rapport | Hugo | Sam. 24 oct. | À faire |

Revue croisée : P1 → P2 → P3 → P4 → P5 → P1.

### Règle commune : compléter les gabarits du professeur
> « Complétez les zones TODO. Ne modifiez pas les chemins imposés. »

Chacun travaille **directement dans le fichier fourni** (`01_hive_analyse.sql`, `02_mongodb_enrichissement.py`, `03_cassandra_publication.py`), uniquement dans ses zones TODO. Pas de script parallèle. Les chemins HDFS, la base `inf6303_devoir1` et les noms de tables imposés ne changent pas.

### Correspondance énoncé ↔ zones TODO de `01_hive_analyse.sql`
| Zone du gabarit | Contenu | Partie de l'énoncé | Responsable | Statut |
|---|---|---|---|---|
| TODO A1 | table externe `evenements_massifs` (10 colonnes), `/inf6303/devoir1/evenements` | A1 – tables | Franklin | Fait |
| TODO A2 | table externe `catalogue_contenus` (5 colonnes), `/inf6303/devoir1/catalogue` | A1 – tables | Mikael | À faire |
| TODO A3 | contrôles de qualité et analyses | A1 – qualité + A2 – analyses | Mikael | Comptage et invalides faits ; analyses à faire |
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
- Produit par : `02_mongodb_enrichissement.py` (B3) — *à compléter par P3*
- Champs : `user_id`, `country`, `plan`, `analytics{...}` — *à préciser*
- Calcul du score utilisé pour le tri Cassandra : *à définir en équipe*

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

rm -f /home/jovyan/work/output/segments_utilisateurs.csv
hdfs dfs -getmerge /inf6303/devoir1/sorties/segments \
  /home/jovyan/work/output/segments_utilisateurs.csv
wc -l /home/jovyan/work/output/segments_utilisateurs.csv   # attendu : 50000
```

Contrôles attendus (exécution du 9 oct. 2026) :

| Contrôle | Valeur |
|---|---|
| Événements | 1 000 000 |
| Lignes invalides (user_id vide / durée NULL / durée < 0) | 0 / 0 / 0 |
| Utilisateurs multi-pays | 0 |
| Utilisateurs en sortie | 50 000 (distincts) |
| Somme des `event_count` | 1 000 000 |
| actif / occasionnel / intensif | 30 901 / 11 669 / 7 430 |

### Étape 2 – Partie B : MongoDB — *à compléter par P3*
**PowerShell (Windows)**
```powershell
docker compose --profile hadoop stop
docker compose --profile nosql up -d --build
```
**Terminal JupyterLab (Linux)**
```bash
python /home/jovyan/work/02_mongodb_enrichissement.py
```

### Étape 3 – Partie C : Cassandra — *à compléter par P4*
```bash
python /home/jovyan/work/03_cassandra_publication.py
```
Cassandra peut demander jusqu'à deux minutes avant d'accepter les connexions.

### Étape 4 – Partie D : Validation — *à compléter par P5*
```bash
python /home/jovyan/work/04_validation_pipeline.py
```

---

## Contenu de l'archive de remise
- `rapport.pdf`
- `01_hive_analyse.sql`, `02_mongodb_enrichissement.py`, `03_cassandra_publication.py`
- `output/segments_utilisateurs.csv`, `output/audience_cible.jsonl`
- `README_EQUIPE.md`
- `rapport/` (sources du rapport) : `architecture_D2.png`, `architecture_D2.svg`

Ne pas inclure les volumes Docker ni les fichiers sources massifs (`data/`).

## Aide au diagnostic
- `docker compose --profile hadoop ps -a` : état des services
- `docker compose --profile hadoop logs --tail=100 hive` : journal HiveServer2
- `hdfs dfs -ls -R /inf6303/devoir1` : arborescence HDFS
