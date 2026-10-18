"""DEVOIR 1 - Partie C. Publier l'audience produite par MongoDB."""
import json, time
from collections import Counter
from cassandra.cluster import Cluster
from cassandra.cluster import NoHostAvailable
from cassandra import InvalidRequest
from cassandra.concurrent import execute_concurrent_with_args

# Fichier de passage produit par la partie B (02_mongodb_enrichissement.py, B3)
AUDIENCE_FILE = "/home/jovyan/work/output/audience_cible.jsonl"
KEYSPACE = "inf6303_devoir1"


def afficher(titre, lignes, colonnes):
    """Affiche le résultat d'une requête CQL sous forme de tableau simple."""
    print(f"\n--- {titre} ---")
    print(" | ".join(f"{c:>15}" for c in colonnes))
    for ligne in lignes:
        print(" | ".join(f"{str(getattr(ligne, c)):>15}" for c in colonnes))


# =====================================================================
# C1 - CONNEXION, KEYSPACE ET MODÈLE ORIENTÉ REQUÊTE
# =====================================================================
print("=== PARTIE C1 : MODELE ORIENTE REQUETE ===")

# Cassandra peut demander jusqu'à deux minutes avant d'accepter les
# connexions : on réessaie toutes les 10 s pendant 3 minutes au maximum.
cluster = Cluster(["cassandra"], port=9042)
session = None
for tentative in range(1, 19):
    try:
        session = cluster.connect()
        break
    except NoHostAvailable:
        print(f"Cassandra pas encore prete (tentative {tentative}/18), attente 10 s...")
        time.sleep(10)
if session is None:
    raise SystemExit("Cassandra injoignable apres 3 minutes")
session.default_timeout = 60
print("Connexion Cassandra etablie")

# Un seul nœud dans l'environnement Docker : SimpleStrategy, facteur 1.
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
""")
session.set_keyspace(KEYSPACE)

# Table conçue pour UNE requête : top utilisateurs d'un pays et d'un
# segment, triés par score décroissant.
#  * Clé de partition composée (country, segment) : toutes les lignes d'un
#    couple pays/segment sont stockées ensemble => une requête = une partition.
#  * Clustering (score DESC, user_id ASC) : les lignes sont déjà triées sur
#    disque dans l'ordre demandé, aucun tri à la lecture (LIMIT 10 suffit).
#  * user_id dans le clustering : plusieurs utilisateurs ont le même score
#    (total_watch_sec). Sans user_id, la clé (country, segment, score) ne
#    serait pas unique et Cassandra écraserait les ex aequo (upsert).
#  * Colonnes dénormalisées (nb_events, plan) : la réponse est complète sans
#    jointure, Cassandra n'en fait pas.
# On recrée la table pour que le script soit rejouable (même nombre de lignes).
session.execute("DROP TABLE IF EXISTS audience_par_pays_segment")
session.execute("""
    CREATE TABLE audience_par_pays_segment (
        country   text,
        segment   text,
        score     int,      -- = analytics.total_watch_sec (secondes, Hive)
        user_id   text,     -- clé d'interconnexion Hive -> MongoDB -> Cassandra
        nb_events int,      -- = analytics.nb_events
        plan      text,
        PRIMARY KEY ((country, segment), score, user_id)
    ) WITH CLUSTERING ORDER BY (score DESC, user_id ASC)
""")
print("Table audience_par_pays_segment creee :")
print("  PRIMARY KEY ((country, segment), score, user_id)")
print("  CLUSTERING ORDER BY (score DESC, user_id ASC)")


# =====================================================================
# C2 - CHARGEMENT DE audience_cible.jsonl PAR REQUÊTE PRÉPARÉE
# =====================================================================
print("\n=== PARTIE C2 : CHARGEMENT ===")

# Lecture du fichier exporté par MongoDB : une ligne JSON = un profil.
# Le score n'est pas inventé : c'est la durée totale calculée par Hive.
lignes_a_charger = []
with open(AUDIENCE_FILE, "r", encoding="utf-8") as f:
    for ligne in f:
        if not ligne.strip():
            continue
        doc = json.loads(ligne)
        a = doc["analytics"]
        lignes_a_charger.append((
            doc["country"], a["segment"], int(a["total_watch_sec"]),
            doc["user_id"], int(a["nb_events"]), doc["plan"],
        ))
nb_exportes = len(lignes_a_charger)
print(f"Documents lus dans audience_cible.jsonl : {nb_exportes}")

# Requête préparée : le CQL est analysé une seule fois par le serveur,
# puis seules les valeurs sont envoyées pour chaque ligne (plus rapide,
# et pas de concaténation de chaînes donc pas d'injection CQL).
insert = session.prepare("""
    INSERT INTO audience_par_pays_segment
        (country, segment, score, user_id, nb_events, plan)
    VALUES (?, ?, ?, ?, ?, ?)
""")
debut = time.time()
resultats = execute_concurrent_with_args(
    session, insert, lignes_a_charger, concurrency=64, raise_on_first_error=True
)
nb_echecs = sum(1 for succes, _ in resultats if not succes)
print(f"Insertions envoyees : {nb_exportes}, echecs : {nb_echecs} "
      f"({time.time() - debut:.1f} s)")

# Comptage des lignes chargées partition par partition : on interroge
# chaque couple (country, segment) par sa clé, comme le fera le service,
# au lieu d'un COUNT(*) global qui balaierait tout le cluster.
attendu_par_partition = Counter((r[0], r[1]) for r in lignes_a_charger)
compte = session.prepare(
    "SELECT COUNT(*) AS n FROM audience_par_pays_segment "
    "WHERE country = ? AND segment = ?"
)
print("\nLignes par partition (country, segment) :")
nb_charges = 0
for (pays, seg) in sorted(attendu_par_partition):
    n = session.execute(compte, (pays, seg)).one().n
    nb_charges += n
    print(f"  {pays:<14} {seg:<9} {n:>6}  (attendu {attendu_par_partition[(pays, seg)]})")
print(f"Nombre de partitions : {len(attendu_par_partition)}")

print(f"\nDocuments exportes par MongoDB : {nb_exportes}")
print(f"Lignes chargees dans Cassandra : {nb_charges}")
assert nb_charges == nb_exportes, (
    f"Ecart : {nb_exportes} documents exportes, {nb_charges} lignes chargees"
)
print("C2 - Chargement verifie : exportes = charges")


# =====================================================================
# C3 - REQUÊTES DE SERVICE
# =====================================================================
print("\n=== PARTIE C3 : REQUETES DE SERVICE ===")
colonnes = ["user_id", "score", "nb_events", "plan"]

top = session.prepare("""
    SELECT user_id, score, nb_events, plan
    FROM audience_par_pays_segment
    WHERE country = ? AND segment = ?
    LIMIT 10
""")

# 1) Requête cible : une seule partition lue, déjà triée par score décroissant.
afficher("Top 10 Canada / intensif",
         session.execute(top, ("Canada", "intensif")), colonnes)

# 2) Autre couple pertinent : France / actif (premier marché européen,
#    segment le plus nombreux, donc audience à relancer vers l'intensif).
afficher("Top 10 France / actif",
         session.execute(top, ("France", "actif")), colonnes)

# 3) Recherche globale par plan : plan n'est ni dans la clé de partition
#    ni dans le clustering. Cassandra ne sait pas quelles partitions lire
#    et refuse la requête plutôt que de balayer toute la table.
print("\n--- Recherche globale par plan sur audience_par_pays_segment ---")
requete_plan = ("SELECT user_id, country, segment, score "
                "FROM audience_par_pays_segment WHERE plan = 'premium' LIMIT 10")
print(f"CQL : {requete_plan}")
try:
    session.execute(requete_plan)
    print("Requete acceptee (inattendu)")
except InvalidRequest as e:
    print(f"Refusee par Cassandra : {e}")

# Avec ALLOW FILTERING la requête passe, mais elle lit les 16 partitions
# et jette les lignes non premium : coût proportionnel à toute la table.
filtre = session.execute(
    "SELECT COUNT(*) AS n FROM audience_par_pays_segment "
    "WHERE plan = 'premium' ALLOW FILTERING"
).one().n
print(f"Avec ALLOW FILTERING : {filtre} lignes premium trouvees apres "
      f"lecture des {nb_charges} lignes de la table (balayage complet)")

# 4) Table supplémentaire pour le besoin « audience par plan ».
#    Même données, autre clé : en Cassandra on duplique pour chaque requête.
#    Partition = plan, clustering identique pour garder le tri par score.
session.execute("DROP TABLE IF EXISTS audience_par_plan")
session.execute("""
    CREATE TABLE audience_par_plan (
        plan      text,
        score     int,
        user_id   text,
        country   text,
        segment   text,
        nb_events int,
        PRIMARY KEY ((plan), score, user_id)
    ) WITH CLUSTERING ORDER BY (score DESC, user_id ASC)
""")
print("\nTable audience_par_plan creee :")
print("  PRIMARY KEY ((plan), score, user_id)")
print("  CLUSTERING ORDER BY (score DESC, user_id ASC)")

insert_plan = session.prepare("""
    INSERT INTO audience_par_plan
        (plan, score, user_id, country, segment, nb_events)
    VALUES (?, ?, ?, ?, ?, ?)
""")
execute_concurrent_with_args(
    session, insert_plan,
    [(r[5], r[2], r[3], r[0], r[1], r[4]) for r in lignes_a_charger],
    concurrency=64, raise_on_first_error=True,
)

# Contrôle : chaque plan contient autant de lignes que dans le fichier.
attendu_par_plan = Counter(r[5] for r in lignes_a_charger)
compte_plan = session.prepare(
    "SELECT COUNT(*) AS n FROM audience_par_plan WHERE plan = ?"
)
total_plan = 0
print("Lignes par plan :")
for plan in sorted(attendu_par_plan):
    n = session.execute(compte_plan, (plan,)).one().n
    total_plan += n
    print(f"  {plan:<9} {n:>6}  (attendu {attendu_par_plan[plan]})")
assert total_plan == nb_exportes, "audience_par_plan incomplete"
assert attendu_par_plan["premium"] == filtre, "comptage premium incoherent"

top_plan = session.prepare("""
    SELECT user_id, score, country, segment
    FROM audience_par_plan WHERE plan = ? LIMIT 10
""")
afficher("Top 10 premium (audience_par_plan, une seule partition lue)",
         session.execute(top_plan, ("premium",)),
         ["user_id", "score", "country", "segment"])

print("\nC3 - Requetes de service terminees.")
cluster.shutdown()
