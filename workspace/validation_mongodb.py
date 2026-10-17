"""
INF6303 - Devoir 1
Validation du livrable MongoDB
"""

import json
from pathlib import Path

FICHIER = Path("/home/jovyan/work/output/audience_cible.jsonl")

total = 0
erreurs = []
identifiants = set()

segments = {
    "intensif": 0,
    "actif": 0
}

champs_obligatoires = {
    "user_id",
    "country",
    "plan",
    "analytics"
}

print("=== VALIDATION DU LIVRABLE MONGODB ===")

with FICHIER.open("r", encoding="utf-8") as f:

    for numero_ligne, ligne in enumerate(f, start=1):

        total += 1

        try:
            document = json.loads(ligne)

            if not isinstance(document, dict):
                raise ValueError("Document JSON non valide")

            # Verification des champs obligatoires
            if set(document.keys()) != champs_obligatoires:
                raise ValueError("Champs manquants ou supplementaires")

            # Verification de l'identifiant
            user_id = document["user_id"]

            if not isinstance(user_id, str) or not user_id:
                raise ValueError("Identifiant invalide")

            if user_id in identifiants:
                raise ValueError("Identifiant duplique")

            identifiants.add(user_id)

            # Verification des attributs
            if not isinstance(document["country"], str):
                raise ValueError("Pays invalide")

            if not isinstance(document["plan"], str):
                raise ValueError("Plan invalide")

            # Verification du sous-document analytics
            analytics = document["analytics"]

            if not isinstance(analytics, dict):
                raise ValueError("Analytics invalide")

            if not isinstance(analytics.get("nb_events"), int):
                raise ValueError("Nombre d'evenements invalide")

            if not isinstance(analytics.get("total_watch_sec"), int):
                raise ValueError("Duree invalide")

            segment = analytics.get("segment")

            if segment not in segments:
                raise ValueError("Segment non autorise")

            segments[segment] += 1

        except (ValueError, KeyError, TypeError) as e:

            erreurs.append(
                f"Ligne {numero_ligne} : {e}"
            )

print(f"Documents analyses : {total}")
print(f"Identifiants uniques : {len(identifiants)}")
print(f"Segments intensifs : {segments['intensif']}")
print(f"Segments actifs : {segments['actif']}")
print(f"Erreurs detectees : {len(erreurs)}")

if erreurs:
    print("\nPremieres erreurs :")

    for erreur in erreurs[:10]:
        print(erreur)

assert total == 34855, "Nombre de documents incorrect"
assert len(erreurs) == 0, "Erreurs de validation detectees"
assert len(identifiants) == total, "Identifiants non uniques"
assert sum(segments.values()) == total, "Repartition incoherente"

print("\nVALIDATION MONGODB REUSSIE")