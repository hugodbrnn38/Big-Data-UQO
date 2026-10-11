# INF6303 - Devoir 1

Pipeline interconnecté Hive -> MongoDB -> Cassandra  évalué sur 15 %.

## Données fournies

- 1 000 000 événements répartis en 8 fichiers gzip;
- 50 000 profils JSON;
- 2 000 contenus.

Ces données sont différentes de celles des mini-labs. Aucun script de génération n'est fourni.

## Démarrage

```powershell
docker compose --profile hadoop up -d --build
```

Ouvrir http://localhost:8888/lab?token=inf6303 puis suivre `Devoir_1_Enonce.pdf`.
Pour MongoDB et Cassandra :

```powershell
docker compose --profile nosql up -d --build
```

Ressources recommandées : 10 Go de RAM pour exécuter tous les services simultanément; sinon  arrêter le profil Hadoop avant de lancer le profil NoSQL.
