from pathlib import Path
import csv, json
out=Path("/home/jovyan/work/output")
required=[out/"segments_utilisateurs.csv",out/"audience_cible.jsonl"]
for p in required:
    print(p.name, "OK" if p.exists() and p.stat().st_size else "MANQUANT", p.stat().st_size if p.exists() else 0)
