#!/usr/bin/env bash
set -euo pipefail
hdfs dfs -mkdir -p /inf6303/devoir1/evenements /inf6303/devoir1/catalogue
hdfs dfs -rm -f /inf6303/devoir1/evenements/* /inf6303/devoir1/catalogue/* || true
for f in /home/jovyan/data/evenements/*.csv.gz; do hdfs dfs -put "$f" /inf6303/devoir1/evenements/; done
hdfs dfs -put /home/jovyan/data/catalogue_contenus.csv /inf6303/devoir1/catalogue/
hdfs dfs -ls -h -R /inf6303/devoir1
