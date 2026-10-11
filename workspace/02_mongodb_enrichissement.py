"""DEVOIR 1 - Partie B. Compléter les TODO sans générer de nouvelles données."""
import csv, gzip, json
from pymongo import MongoClient, UpdateOne

client = MongoClient("mongodb://inf6303:inf6303@mongo:27017/?authSource=admin")
db = client["inf6303_devoir1"]
profiles = db["profils"]

# TODO B1 : vider la collection et importer profils_utilisateurs.jsonl.gz.
# TODO B2 : créer les index demandés.
# TODO B3 : lire output/segments_utilisateurs.csv et enrichir les profils
#           par user_id avec nb_events, total_watch_sec et segment.
# TODO B4 : produire output/audience_cible.jsonl pour les segments intensif et actif.
# TODO B5 : afficher les contrôles demandés dans l'énoncé.
