-- DEVOIR 1 - Partie A : Hive/HiveQL
-- Complétez les zones TODO. Ne modifiez pas les chemins imposés.

CREATE DATABASE IF NOT EXISTS inf6303_devoir1;
USE inf6303_devoir1;

-- ---------------------------------------------------------------------
-- Paramètres d'exécution (ajoutés par l'équipe, voir README_EQUIPE.md)
-- * MapReduce local : ResourceManager/NodeManager s'arrêtent (Exited 255,
--   mémoire Docker limitée) et les jobs restaient bloqués à la soumission.
--   Le résultat est identique ; seul le lieu d'exécution change.
-- * Pas de map-join automatique : sa tâche locale (MapredLocalTask)
--   dépasse la mémoire du conteneur Hive (-Xmx1G).
-- * HiveInputFormat : avec CombineHiveInputFormat (défaut), les 8 en-têtes
--   n'étaient pas ignorés => COUNT(*) = 1 000 008 au lieu de 1 000 000.
-- ---------------------------------------------------------------------
SET mapreduce.framework.name=local;
SET hive.exec.mode.local.auto=false;
SET hive.auto.convert.join=false;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- TODO A1 : table externe evenements_massifs (10 colonnes)
-- LOCATION '/inf6303/devoir1/evenements'
-- Ordre vérifié sur l'en-tête réel des fichiers :
--   event_id,user_id,content_id,event_type,event_ts,country,device,
--   watch_seconds,quality,session_id
-- EXTERNAL : DROP supprime seulement la définition, jamais les fichiers HDFS.
-- On recrée pour garantir le schéma (IF NOT EXISTS garderait une ancienne
-- définition sans prévenir).
DROP TABLE IF EXISTS evenements_massifs;

CREATE EXTERNAL TABLE evenements_massifs (
  event_id      STRING,
  user_id       STRING,
  content_id    STRING,
  event_type    STRING,
  event_ts      STRING,   -- format ISO 2026-02-05T20:47:15 (avec 'T')
  country       STRING,
  device        STRING,
  watch_seconds INT,      -- valeur non numérique => NULL (détectée en A3)
  quality       STRING,
  session_id    STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/inf6303/devoir1/evenements'
TBLPROPERTIES ('skip.header.line.count'='1');

-- TODO A2 : table externe catalogue_contenus (5 colonnes)
-- LOCATION '/inf6303/devoir1/catalogue'
-- [Mikael] Vérifier l'en-tête : head -3 /home/jovyan/data/catalogue_contenus.csv
-- DROP TABLE IF EXISTS catalogue_contenus;
-- CREATE EXTERNAL TABLE catalogue_contenus ( ... 5 colonnes ... )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
-- STORED AS TEXTFILE
-- LOCATION '/inf6303/devoir1/catalogue'
-- TBLPROPERTIES ('skip.header.line.count'='1');

-- TODO A3 : contrôles de qualité et analyses demandées dans l'énoncé.
-- [Mikael] À compléter : COUNT(*) catalogue = 2 000 ; répartition par pays
-- et par type ; top 10 contenus par durée totale (jointure catalogue) ;
-- durée moyenne par appareil et qualité ; EXPLAIN d'une requête.

-- Volume des événements (attendu : 1 000 000)
SELECT COUNT(*) AS nb_evenements FROM evenements_massifs;

-- Identifiants et durées invalides
SELECT
  SUM(CASE WHEN user_id IS NULL OR TRIM(user_id) = '' THEN 1 ELSE 0 END) AS user_id_invalide,
  SUM(CASE WHEN watch_seconds IS NULL                  THEN 1 ELSE 0 END) AS duree_nulle_ou_non_numerique,
  SUM(CASE WHEN watch_seconds < 0                      THEN 1 ELSE 0 END) AS duree_negative
FROM evenements_massifs;

-- TODO A4 : produire puis exporter un segment par utilisateur.
-- Le répertoire HDFS imposé est /inf6303/devoir1/sorties/segments

-- A4.1 Prérequis : un utilisateur a-t-il plusieurs pays ?
-- Résultat obtenu : 0 => le pays est unique par user_id, MAX(country)
-- le récupère dans la même agrégation (pas de jointure).
SELECT COUNT(*) AS nb_users_multi_pays
FROM (
  SELECT user_id
  FROM evenements_massifs
  GROUP BY user_id
  HAVING COUNT(DISTINCT country) > 1
) t;

-- A4.2 Agrégation par user_id et segmentation
-- Seuils (complément du 6 octobre 2026), sur SUM(watch_seconds) seulement :
--   intensif >= 55000 ; actif >= 30000 ; occasionnel < 30000
-- event_count est conservé mais ne sert pas au segment.
-- Format de sortie (sans en-tête) :
--   user_id,country,event_count,total_watch_sec,segment
INSERT OVERWRITE DIRECTORY '/inf6303/devoir1/sorties/segments'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT
  user_id,
  MAX(country)       AS country,
  COUNT(*)           AS event_count,
  SUM(watch_seconds) AS total_watch_sec,
  CASE
    WHEN SUM(watch_seconds) >= 55000 THEN 'intensif'
    WHEN SUM(watch_seconds) >= 30000 THEN 'actif'
    ELSE 'occasionnel'
  END AS segment
FROM evenements_massifs
WHERE user_id IS NOT NULL AND TRIM(user_id) <> ''
  AND watch_seconds IS NOT NULL AND watch_seconds >= 0
GROUP BY user_id;

-- A4.3 Contrôles de cohérence sur la sortie
DROP TABLE IF EXISTS segments_sortie;

CREATE EXTERNAL TABLE segments_sortie (
  user_id STRING, country STRING, event_count BIGINT,
  total_watch_sec BIGINT, segment STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/inf6303/devoir1/sorties/segments';

-- Attendu : 50 000 lignes, 50 000 user_id distincts, somme = 1 000 000
SELECT COUNT(*)                AS nb_users,
       COUNT(DISTINCT user_id) AS nb_users_distincts,
       SUM(event_count)        AS somme_evenements
FROM segments_sortie;

-- Répartition et respect des bornes de chaque segment
SELECT segment, COUNT(*) AS nb_users,
       MIN(total_watch_sec) AS min_sec, MAX(total_watch_sec) AS max_sec
FROM segments_sortie
GROUP BY segment
ORDER BY nb_users DESC;

SELECT * FROM segments_sortie LIMIT 5;

-- A4.4 Export (terminal JupyterLab, hors Beeline) :
--   rm -f /home/jovyan/work/output/segments_utilisateurs.csv
--   hdfs dfs -getmerge /inf6303/devoir1/sorties/segments \
--     /home/jovyan/work/output/segments_utilisateurs.csv
--   wc -l /home/jovyan/work/output/segments_utilisateurs.csv   # attendu : 50000

