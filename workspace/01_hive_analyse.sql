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
-- En-tête réel : content_id,title,genre,release_year,duration_sec
-- Le fichier fourni est en fins de ligne CRLF : le '\r' reste collé à
-- duration_sec, mais Hive le convertit bien en INT (0 NULL, contrôlé en A3).
DROP TABLE IF EXISTS catalogue_contenus;

CREATE EXTERNAL TABLE catalogue_contenus (
  content_id   STRING,   -- clé de jointure avec evenements_massifs.content_id
  title        STRING,
  genre        STRING,
  release_year INT,
  duration_sec INT       -- durée du contenu en secondes
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/inf6303/devoir1/catalogue'
TBLPROPERTIES ('skip.header.line.count'='1');

-- TODO A3 : contrôles de qualité et analyses demandées dans l'énoncé.

-- ---------------------------------------------------------------------
-- A1 - Volumes (attendus : 1 000 000 événements, 2 000 contenus)
-- ---------------------------------------------------------------------

-- Volume des événements (attendu : 1 000 000)
SELECT COUNT(*) AS nb_evenements FROM evenements_massifs;

-- Volume du catalogue (attendu : 2 000) et unicité de la clé : un
-- content_id en double multiplierait les lignes de la jointure A2.
SELECT COUNT(*)                   AS nb_contenus,
       COUNT(DISTINCT content_id) AS nb_content_id_distincts
FROM catalogue_contenus;

-- ---------------------------------------------------------------------
-- A1 - Contrôles de qualité
-- ---------------------------------------------------------------------

-- Identifiants et durées invalides
SELECT
  SUM(CASE WHEN user_id IS NULL OR TRIM(user_id) = '' THEN 1 ELSE 0 END) AS user_id_invalide,
  SUM(CASE WHEN watch_seconds IS NULL                  THEN 1 ELSE 0 END) AS duree_nulle_ou_non_numerique,
  SUM(CASE WHEN watch_seconds < 0                      THEN 1 ELSE 0 END) AS duree_negative
FROM evenements_massifs;

-- Format des identifiants et doublons d'event_id, en un seul passage :
-- * format attendu (vu sur l'en-tête et les données) : e + 9 chiffres,
--   u + 6 chiffres, c + 5 chiffres ; une valeur vide, tronquée ou décalée
--   d'une colonne échoue au RLIKE ;
-- * event_id est la clé de l'événement : nb_evenements - distincts = nombre
--   de lignes en double (un fichier déposé deux fois, par exemple) ;
-- * durée nulle (0 s) : valide pour un événement sans lecture (pause,
--   recherche...), suspecte pour une lecture.
SELECT
  SUM(CASE WHEN event_id   IS NULL OR event_id   NOT RLIKE '^e[0-9]{9}$' THEN 1 ELSE 0 END) AS event_id_invalide,
  COUNT(*) - COUNT(DISTINCT event_id)                                                     AS event_id_en_double,
  SUM(CASE WHEN user_id    IS NULL OR user_id    NOT RLIKE '^u[0-9]{6}$' THEN 1 ELSE 0 END) AS user_id_format_invalide,
  SUM(CASE WHEN content_id IS NULL OR content_id NOT RLIKE '^c[0-9]{5}$' THEN 1 ELSE 0 END) AS content_id_invalide,
  SUM(CASE WHEN watch_seconds = 0                                     THEN 1 ELSE 0 END) AS duree_zero,
  SUM(CASE WHEN watch_seconds = 0 AND event_type = 'lecture'          THEN 1 ELSE 0 END) AS lecture_duree_zero,
  MIN(watch_seconds)                                                                      AS duree_min,
  MAX(watch_seconds)                                                                      AS duree_max
FROM evenements_massifs;

-- Durées par type d'événement : seules les lectures portent de longues durées.
SELECT event_type, COUNT(*) AS nb, MIN(watch_seconds) AS min_sec,
       MAX(watch_seconds) AS max_sec, ROUND(AVG(watch_seconds), 1) AS moy_sec
FROM evenements_massifs
GROUP BY event_type
ORDER BY nb DESC;

-- Intégrité référentielle événements -> catalogue (LEFT JOIN) :
-- * content_id absent du catalogue : l'événement serait perdu par la
--   jointure interne du top 10 ;
-- * durée regardée > durée du contenu : signalée, non exclue (watch_seconds
--   cumule le temps de la session, revisionnages compris, et la sortie A3
--   doit couvrir tous les événements).
SELECT
  SUM(CASE WHEN c.content_id IS NULL THEN 1 ELSE 0 END)              AS content_id_absent_catalogue,
  SUM(CASE WHEN e.watch_seconds > c.duration_sec THEN 1 ELSE 0 END)  AS duree_superieure_au_contenu,
  COUNT(DISTINCT e.content_id)                                       AS contenus_utilises
FROM evenements_massifs e
LEFT JOIN catalogue_contenus c ON e.content_id = c.content_id;

-- Qualité du catalogue : durées et années manquantes ou impossibles.
SELECT
  SUM(CASE WHEN content_id IS NULL OR content_id NOT RLIKE '^c[0-9]{5}$' THEN 1 ELSE 0 END) AS content_id_invalide,
  SUM(CASE WHEN duration_sec IS NULL OR duration_sec <= 0               THEN 1 ELSE 0 END) AS duree_contenu_invalide,
  SUM(CASE WHEN release_year IS NULL                                     THEN 1 ELSE 0 END) AS annee_manquante,
  MIN(duration_sec) AS duree_min, MAX(duration_sec) AS duree_max,
  MIN(release_year) AS annee_min, MAX(release_year) AS annee_max
FROM catalogue_contenus;

-- ---------------------------------------------------------------------
-- A2 - Analyse distribuée
-- Les valeurs attendues viennent de data/MANIFESTE_DONNEES.json ; chaque
-- requête compare le résultat obtenu à ces valeurs (colonne controle).
-- ---------------------------------------------------------------------

-- Répartition croisée pays x type : un seul passage sur le million de
-- lignes, puis les deux répartitions simples sont dérivées des 40 lignes.
DROP TABLE IF EXISTS repartition_pays_type;

CREATE TEMPORARY TABLE repartition_pays_type AS
SELECT country, event_type, COUNT(*) AS nb_evenements
FROM evenements_massifs
GROUP BY country, event_type;

SELECT country, event_type, nb_evenements
FROM repartition_pays_type
ORDER BY country, nb_evenements DESC;

-- Répartition par pays, comparée au manifeste (events_by_country)
SELECT o.country, o.nb_evenements, a.attendu,
       ROUND(100 * o.nb_evenements / 1000000, 2) AS pct,
       CASE WHEN o.nb_evenements = a.attendu THEN 'OK' ELSE 'ECART' END AS controle
FROM (SELECT country, SUM(nb_evenements) AS nb_evenements
      FROM repartition_pays_type GROUP BY country) o
FULL OUTER JOIN (
  SELECT stack(8,
    'Canada', 311247, 'Senegal', 132617, 'Cote_d_Ivoire', 119916,
    'France', 118790, 'Maroc', 98758, 'Cameroun', 89530,
    'Belgique', 69962, 'Tunisie', 59180) AS (country, attendu)
) a ON o.country = a.country
ORDER BY o.nb_evenements DESC;

-- Répartition par type, comparée au manifeste (events_by_type)
SELECT o.event_type, o.nb_evenements, a.attendu,
       ROUND(100 * o.nb_evenements / 1000000, 2) AS pct,
       CASE WHEN o.nb_evenements = a.attendu THEN 'OK' ELSE 'ECART' END AS controle
FROM (SELECT event_type, SUM(nb_evenements) AS nb_evenements
      FROM repartition_pays_type GROUP BY event_type) o
FULL OUTER JOIN (
  SELECT stack(5,
    'lecture', 549714, 'pause', 139884, 'recherche', 119987,
    'abandon', 110611, 'ajout_favori', 79804) AS (event_type, attendu)
) a ON o.event_type = a.event_type
ORDER BY o.nb_evenements DESC;

-- Top 10 des contenus par durée totale, après jointure avec le catalogue.
-- Requête analysée par EXPLAIN plus bas.
DROP TABLE IF EXISTS top10_contenus;

CREATE TEMPORARY TABLE top10_contenus AS
SELECT e.content_id, c.title, c.genre, c.duration_sec,
       SUM(e.watch_seconds) AS duree_totale_sec,
       COUNT(*)             AS nb_evenements
FROM evenements_massifs e
JOIN catalogue_contenus c ON e.content_id = c.content_id
GROUP BY e.content_id, c.title, c.genre, c.duration_sec
ORDER BY duree_totale_sec DESC
LIMIT 10;

-- Top 10 comparé au manifeste (top_content_by_watch) : même rang, même
-- content_id, même durée totale.
SELECT t.rang, t.content_id, t.title, t.genre, t.duree_totale_sec,
       t.nb_evenements, a.content_id AS content_id_attendu, a.attendu,
       CASE WHEN t.content_id = a.content_id AND t.duree_totale_sec = a.attendu
            THEN 'OK' ELSE 'ECART' END AS controle
FROM (SELECT ROW_NUMBER() OVER (ORDER BY duree_totale_sec DESC) AS rang,
             content_id, title, genre, duree_totale_sec, nb_evenements
      FROM top10_contenus) t
FULL OUTER JOIN (
  SELECT stack(10,
    1, 'c00545', 1226712,  2, 'c01844', 1221503,  3, 'c00686', 1216833,
    4, 'c00277', 1210971,  5, 'c00553', 1204604,  6, 'c01825', 1201793,
    7, 'c00882', 1200490,  8, 'c00912', 1199344,  9, 'c01263', 1198489,
    10, 'c01769', 1198083) AS (rang, content_id, attendu)
) a ON t.rang = a.rang
ORDER BY t.rang;

-- Durée moyenne par appareil et qualité. Toutes les lignes sont gardées
-- (moyenne globale) ; la moyenne des seules lectures est ajoutée, car les
-- autres types (pause, recherche...) ne durent que 0 à 180 s.
SELECT device, quality,
       COUNT(*)                                                         AS nb_evenements,
       ROUND(AVG(watch_seconds), 1)                                     AS duree_moy_sec,
       ROUND(AVG(CASE WHEN event_type = 'lecture' THEN watch_seconds END), 1) AS duree_moy_lecture_sec
FROM evenements_massifs
GROUP BY device, quality
ORDER BY device, duree_moy_sec DESC;

-- Plan d'exécution de la requête du top 10 (jointure + agrégation + tri).
EXPLAIN
SELECT e.content_id, c.title, c.genre, c.duration_sec,
       SUM(e.watch_seconds) AS duree_totale_sec,
       COUNT(*)             AS nb_evenements
FROM evenements_massifs e
JOIN catalogue_contenus c ON e.content_id = c.content_id
GROUP BY e.content_id, c.title, c.genre, c.duration_sec
ORDER BY duree_totale_sec DESC
LIMIT 10;

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

