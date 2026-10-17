"""
INF6303 - Devoir 1
Partie B - MongoDB

B1 : Importation documentaire des profils utilisateurs.
"""

import gzip
import json

from pymongo import MongoClient

# Connexion au serveur MongoDB
client = MongoClient(
    "mongodb://inf6303:inf6303@mongo:27017/?authSource=admin"
)

# Selection de la base et de la collection
db = client["inf6303_devoir1"]
profiles = db["profils"]

# Chemin du fichier source
SOURCE_FILE = "/home/jovyan/data/profils_utilisateurs.jsonl.gz"

print("=== PARTIE B1 : IMPORT DOCUMENTAIRE ===")

# Reinitialisation de la collection
profiles.drop()

# Lecture et importation des profils
batch = []
batch_size = 1000
total_imported = 0

with gzip.open(SOURCE_FILE, "rt", encoding="utf-8") as f:

    for line in f:

        if not line.strip():
            continue

        document = json.loads(line)
        batch.append(document)

        if len(batch) >= batch_size:

            result = profiles.insert_many(batch)

            total_imported += len(result.inserted_ids)

            batch = []

    # Importation du dernier lot
    if batch:

        result = profiles.insert_many(batch)

        total_imported += len(result.inserted_ids)

# Verification du nombre de documents
total_profiles = profiles.count_documents({})

print(f"Profils importes : {total_imported}")
print(f"Documents MongoDB : {total_profiles}")

# Verification de la structure documentaire
sample = profiles.find_one(
    {},
    {
        "_id": 0,
        "user_id": 1,
        "preferences": 1,
        "devices": 1,
        "consent_analytics": 1
    }
)

print("\nExemple de profil importe :")
print(json.dumps(sample, indent=2, ensure_ascii=False))

# Controle du volume attendu
assert total_profiles == 50000, (
    f"Erreur : 50000 profils attendus, {total_profiles} trouves"
)

print("\nB1 - Importation terminee avec succes.")

# =====================================================
# B1 - CREATION DES INDEX MONGODB
# =====================================================

print("\n=== B1 : CREATION DES INDEX ===")

# Index unique sur user_id
index_user = profiles.create_index(
    "user_id",
    unique=True
)

print(f"Index unique cree : {index_user}")

# Index compose pour la selection des audiences
index_audience = profiles.create_index([
    ("consent_analytics", 1),
    ("analytics.segment", 1)
])

print(f"Index compose cree : {index_audience}")

# Affichage des index existants
print("\nListe des index MongoDB :")

for index in profiles.list_indexes():
    print({
        "name": index["name"],
        "key": dict(index["key"]),
        "unique": index.get("unique", False)
    })

print("\nB1 - Indexation terminee avec succes.")

# =====================================================
# B2 - ENRICHISSEMENT DES PROFILS AVEC HIVE
# =====================================================

import csv
from pymongo import UpdateOne

print("\n=== PARTIE B2 : ENRICHISSEMENT HIVE -> MONGODB ===")

CSV_FILE = "/home/jovyan/work/output/segments_utilisateurs.csv"

operations = []
batch_size = 1000

total_segments = 0
total_updated = 0
total_unmatched = 0

with open(CSV_FILE, "r", encoding="utf-8") as f:

    reader = csv.reader(f)

    for row in reader:

        if not row:
            continue

        if len(row) != 5:
            raise ValueError(
                f"Ligne CSV invalide : {row}"
            )

        user_id, country, nb_events, total_watch_sec, segment = row

        analytics = {
            "nb_events": int(nb_events),
            "total_watch_sec": int(total_watch_sec),
            "segment": segment
        }

        operations.append(
            UpdateOne(
                {"user_id": user_id},
                {"$set": {"analytics": analytics}},
                upsert=False
            )
        )

        total_segments += 1

        if len(operations) >= batch_size:

            result = profiles.bulk_write(operations)

            total_updated += result.matched_count

            operations = []

    # Traiter le dernier lot
    if operations:

        result = profiles.bulk_write(operations)

        total_updated += result.matched_count

# =====================================================
# CONTROLES DE COHERENCE
# =====================================================

total_profiles = profiles.count_documents({})

total_enriched = profiles.count_documents({
    "analytics": {"$exists": True}
})

total_unmatched = total_segments - total_updated

print(f"Profils MongoDB : {total_profiles}")
print(f"Segments Hive : {total_segments}")
print(f"Correspondances trouvees : {total_updated}")
print(f"Profils enrichis : {total_enriched}")
print(f"Segments sans correspondance : {total_unmatched}")

# Exemple de profil enrichi
sample = profiles.find_one(
    {"user_id": "u000001"},
    {"_id": 0}
)

print("\nExemple de profil enrichi :")
print(json.dumps(sample, indent=2, ensure_ascii=False))

# Validation
assert total_segments == 50000, (
    f"Nombre de segments inattendu : {total_segments}"
)

assert total_unmatched == 0, (
    f"{total_unmatched} segments sans correspondance"
)

assert total_enriched == total_profiles, (
    "Tous les profils ne sont pas enrichis"
)

print("\nB2 - Enrichissement termine avec succes.")

# =====================================================
# B3 - EXPORTATION DE L'AUDIENCE CIBLE VERS CASSANDRA
# =====================================================

import os

print("\n=== PARTIE B3 : EXPORTATION AUDIENCE CIBLE ===")

OUTPUT_FILE = "/home/jovyan/work/output/audience_cible.jsonl"

# Critères de sélection imposés par le professeur
filtre_audience = {
    "consent_analytics": True,
    "analytics.segment": {
        "$in": ["intensif", "actif"]
    }
}

# Projection des champs exigés
projection = {
    "_id": 0,
    "user_id": 1,
    "country": 1,
    "plan": 1,
    "analytics": 1
}

# Créer le répertoire de sortie si nécessaire
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# Compteur de documents exportés
total_exported = 0

# Exportation au format JSONL
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:

    cursor = profiles.find(
        filtre_audience,
        projection
    ).sort("user_id", 1)

    for document in cursor:

        f.write(
            json.dumps(
                document,
                ensure_ascii=False
            ) + "\n"
        )

        total_exported += 1

# =====================================================
# CONTROLES DE COHERENCE
# =====================================================

total_eligible = profiles.count_documents(filtre_audience)

print(f"Profils MongoDB : {total_profiles}")
print(f"Profils admissibles : {total_eligible}")
print(f"Profils exportes : {total_exported}")

# Validation du nombre exporté
assert total_exported == total_eligible, (
    "Erreur : le nombre de profils exportes "
    "ne correspond pas au nombre de profils admissibles"
)

# Vérification du fichier généré
assert os.path.exists(OUTPUT_FILE), (
    "Erreur : fichier audience_cible.jsonl introuvable"
)

# Calcul de la réduction
reduction = (
    (total_profiles - total_exported)
    / total_profiles
) * 100

print(f"Reduction du volume : {reduction:.2f}%")

print(f"Fichier genere : {OUTPUT_FILE}")

print("\nB3 - Exportation terminee avec succes.")

client.close()
