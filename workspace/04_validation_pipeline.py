"""DEVOIR 1 - Partie D1. Traçabilité Hive -> MongoDB -> Cassandra.

Usage : python 04_validation_pipeline.py [user_id ...]
Sans argument, suit les trois user_id retenus pour le rapport.
"""
import csv, json, sys, time
from collections import Counter
from pathlib import Path
from pymongo import MongoClient
from cassandra.cluster import Cluster, NoHostAvailable

out = Path("/home/jovyan/work/output")
CSV_FILE = out / "segments_utilisateurs.csv"
AUDIENCE_FILE = out / "audience_cible.jsonl"
KEYSPACE = "inf6303_devoir1"
TABLE = "audience_par_pays_segment"
SEGMENTS = ["intensif", "actif", "occasionnel"]

# Trois cas réels : intensif consentant, actif consentant, et un profil
# occasionnel non consentant qui ne doit pas arriver dans Cassandra.
USER_IDS = sys.argv[1:] or ["u000001", "u000003", "u000002"]

ecarts = []  # anomalies détectées ; le script échoue à la fin s'il y en a


def controle(libelle, obtenu, attendu):
    """Affiche un contrôle et mémorise l'écart éventuel."""
    ok = obtenu == attendu
    print(f"  {'OK    ' if ok else 'ECART '} {libelle:<52} {obtenu:>7}  (attendu {attendu})")
    if not ok:
        ecarts.append(libelle)


# =====================================================================
# 0 - FICHIERS DE PASSAGE
# =====================================================================
print("=== FICHIERS DE PASSAGE ===")
required = [CSV_FILE, AUDIENCE_FILE]
for p in required:
    print(p.name, "OK" if p.exists() and p.stat().st_size else "MANQUANT", p.stat().st_size if p.exists() else 0)


# =====================================================================
# D1.1 - COMPTAGES À CHAQUE ÉTAPE
# =====================================================================
print("\n=== D1.1 : COMPTAGES PAR ETAPE ===")

# Étape 1 : sortie Hive (sans en-tête) user_id,country,event_count,total_watch_sec,segment
hive = {}
with open(CSV_FILE, encoding="utf-8") as f:
    for user_id, country, nb_events, total, segment in csv.reader(f):
        hive[user_id] = {"country": country, "nb_events": int(nb_events),
                         "total_watch_sec": int(total), "segment": segment}

# Étape 2 : MongoDB, collection enrichie par 02_mongodb_enrichissement.py
client = MongoClient("mongodb://inf6303:inf6303@mongo:27017/?authSource=admin")
profils = client["inf6303_devoir1"]["profils"]
filtre_audience = {"consent_analytics": True,
                   "analytics.segment": {"$in": ["intensif", "actif"]}}
nb_profils = profils.count_documents({})
nb_enrichis = profils.count_documents({"analytics": {"$exists": True}})
nb_admissibles = profils.count_documents(filtre_audience)
mongo = {d["user_id"]: d for d in profils.find(
    {}, {"_id": 0, "user_id": 1, "country": 1, "plan": 1,
         "consent_analytics": 1, "analytics": 1})}

# Étape 3 : fichier exporté par MongoDB pour Cassandra
audience = {}
with open(AUDIENCE_FILE, encoding="utf-8") as f:
    for ligne in f:
        if ligne.strip():
            doc = json.loads(ligne)
            audience[doc["user_id"]] = doc

# Étape 4 : Cassandra. On lit partition par partition, par la clé, comme le
# service : tous les pays connus x les 3 segments (24 partitions). Les
# partitions occasionnel doivent être vides puisque MongoDB ne les exporte pas.
cluster = Cluster(["cassandra"], port=9042)
session = None
for tentative in range(1, 19):
    try:
        session = cluster.connect(KEYSPACE)
        break
    except NoHostAvailable:
        print(f"Cassandra pas encore prete (tentative {tentative}/18), attente 10 s...")
        time.sleep(10)
if session is None:
    raise SystemExit("Cassandra injoignable apres 3 minutes")
session.default_timeout = 60

lecture = session.prepare(
    f"SELECT country, segment, score, user_id, nb_events, plan FROM {TABLE} "
    "WHERE country = ? AND segment = ?")
cassandra = {}
lignes_par_partition = Counter()
for pays in sorted({h["country"] for h in hive.values()}):
    for segment in SEGMENTS:
        for r in session.execute(lecture, (pays, segment)):
            cassandra[r.user_id] = r
            lignes_par_partition[(pays, segment)] += 1
nb_cassandra = sum(lignes_par_partition.values())

print(f"{'Etape':<44} {'Lignes':>8}")
print(f"{'1. Hive : segments_utilisateurs.csv':<44} {len(hive):>8}")
print(f"{'2. MongoDB : profils':<44} {nb_profils:>8}")
print(f"{'   MongoDB : profils enrichis (analytics)':<44} {nb_enrichis:>8}")
print(f"{'   MongoDB : consentants intensif/actif':<44} {nb_admissibles:>8}")
print(f"{'3. Fichier : audience_cible.jsonl':<44} {len(audience):>8}")
print(f"{'4. Cassandra : ' + TABLE:<44} {nb_cassandra:>8}")
print(f"   Partitions non vides : {len(lignes_par_partition)}")

print("\nReduction par filtre MongoDB :")
repartition = Counter((h["segment"], mongo[u]["consent_analytics"]) for u, h in hive.items())
for segment in SEGMENTS:
    print(f"  {segment:<12} consentants {repartition[(segment, True)]:>6}"
          f"   non consentants {repartition[(segment, False)]:>6}")
exclus_segment = repartition[("occasionnel", True)] + repartition[("occasionnel", False)]
exclus_consent = repartition[("intensif", False)] + repartition[("actif", False)]
print(f"  Exclus car occasionnel : {exclus_segment}")
print(f"  Exclus car intensif/actif non consentant : {exclus_consent}")

print("\nControles de coherence :")
controle("Hive : user_id distincts", len(hive), 50000)
controle("Hive : somme des nb_events", sum(h["nb_events"] for h in hive.values()), 1000000)
controle("MongoDB : profils = lignes Hive", nb_profils, len(hive))
controle("MongoDB : profils enrichis = profils", nb_enrichis, nb_profils)
controle("JSONL : lignes = admissibles MongoDB", len(audience), nb_admissibles)
controle("JSONL + exclus = profils", len(audience) + exclus_segment + exclus_consent, nb_profils)
controle("Cassandra : lignes = lignes JSONL", nb_cassandra, len(audience))
controle("Cassandra : lignes occasionnel", sum(n for (_, s), n in lignes_par_partition.items()
                                               if s == "occasionnel"), 0)

# Comparaison valeur par valeur sur l'ensemble des utilisateurs, pas seulement
# sur les trois suivis : chaque étape doit recopier l'étape précédente.
diff_hive_mongo = sum(
    1 for u, h in hive.items()
    if u not in mongo or mongo[u]["country"] != h["country"]
    or mongo[u].get("analytics") != {k: h[k] for k in ("nb_events", "total_watch_sec", "segment")})
diff_mongo_jsonl = sum(
    1 for u, d in audience.items()
    if u not in mongo or {k: mongo[u][k] for k in ("country", "plan", "analytics")}
    != {k: d[k] for k in ("country", "plan", "analytics")})
diff_jsonl_cassandra = sum(
    1 for u, d in audience.items()
    if u not in cassandra
    or (cassandra[u].country, cassandra[u].segment, cassandra[u].score,
        cassandra[u].nb_events, cassandra[u].plan)
    != (d["country"], d["analytics"]["segment"], d["analytics"]["total_watch_sec"],
        d["analytics"]["nb_events"], d["plan"]))
controle("Hive -> MongoDB : profils differents", diff_hive_mongo, 0)
controle("MongoDB -> JSONL : documents differents", diff_mongo_jsonl, 0)
controle("JSONL -> Cassandra : lignes differentes", diff_jsonl_cassandra, 0)
controle("Cassandra : user_id absents du JSONL", len(set(cassandra) - set(audience)), 0)


# =====================================================================
# D1.2 - TRAÇABILITÉ DE TROIS user_id
# =====================================================================
print(f"\n=== D1.2 : TRACABILITE DE {len(USER_IDS)} user_id ===")
colonnes = ["etape", "pays", "nb_events", "duree/score", "segment", "plan", "consent"]
largeurs = [22, 14, 9, 11, 11, 9, 7]


def ligne_tableau(valeurs):
    return " | ".join(f"{str(v):<{l}}" for v, l in zip(valeurs, largeurs))


for user_id in USER_IDS:
    h, m = hive.get(user_id), mongo.get(user_id)
    d, c = audience.get(user_id), cassandra.get(user_id)
    print(f"\n--- {user_id} ---")
    print(ligne_tableau(colonnes))
    print("-+-".join("-" * l for l in largeurs))
    lignes = {
        "Hive (CSV)": None if h is None else
            (h["country"], h["nb_events"], h["total_watch_sec"], h["segment"], "-", "-"),
        "MongoDB (profils)": None if m is None else
            (m["country"], m["analytics"]["nb_events"], m["analytics"]["total_watch_sec"],
             m["analytics"]["segment"], m["plan"], m["consent_analytics"]),
        "audience_cible.jsonl": None if d is None else
            (d["country"], d["analytics"]["nb_events"], d["analytics"]["total_watch_sec"],
             d["analytics"]["segment"], d["plan"], "-"),
        "Cassandra": None if c is None else
            (c.country, c.nb_events, c.score, c.segment, c.plan, "-"),
    }
    for etape, valeurs in lignes.items():
        print(ligne_tableau((etape,) + (valeurs or ("ABSENT",) + ("",) * 5)))

    # Explication : on compare chaque étape présente à Hive (source des
    # mesures) et à MongoDB (source du plan), puis on justifie les absences.
    if h is None or m is None:
        print("=> ECART : user_id absent de Hive ou de MongoDB")
        ecarts.append(f"{user_id} absent")
        continue
    reference = (h["country"], h["nb_events"], h["total_watch_sec"], h["segment"], m["plan"])
    for etape in ("MongoDB (profils)", "audience_cible.jsonl", "Cassandra"):
        if lignes[etape] is not None and lignes[etape][:5] != reference:
            print(f"=> ECART : valeurs differentes dans {etape}")
            ecarts.append(f"{user_id} {etape}")
    admissible = m["consent_analytics"] and h["segment"] in ("intensif", "actif")
    if admissible and d is not None and c is not None:
        print("=> Present aux 4 etapes, valeurs identiques. Le score Cassandra est la "
              "duree totale Hive (total_watch_sec) ; le plan vient du profil MongoDB.")
    elif not admissible and d is None and c is None:
        raisons = []
        if h["segment"] == "occasionnel":
            raisons.append("segment occasionnel")
        if not m["consent_analytics"]:
            raisons.append("consent_analytics = false")
        print(f"=> Absent du JSONL et de Cassandra, comme attendu : {' et '.join(raisons)}. "
              "Le filtre B3 l'exclut ; il reste enrichi dans MongoDB.")
    else:
        print("=> ECART : presence incoherente avec le filtre B3")
        ecarts.append(f"{user_id} filtre B3")

cluster.shutdown()
client.close()

print()
if ecarts:
    raise SystemExit(f"VALIDATION ECHOUEE : {len(ecarts)} ecart(s) : {', '.join(ecarts)}")
print("VALIDATION DU PIPELINE REUSSIE : Hive -> MongoDB -> Cassandra coherent")
