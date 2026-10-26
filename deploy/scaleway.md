# Déployer sur Scaleway

La stack complète (Redpanda, Flink, Postgres, Grafana, producteurs) tient sur une seule instance avec 8 Go de RAM.
Flink réserve environ 2,5 Go (JobManager 1 Go + TaskManager 1,5 Go), Redpanda 768 Mo.

## 1. Créer l'instance

Dans la console Scaleway, **Instances → Create instance** :

| Réglage | Valeur |
| --- | --- |
| Zone | `PAR-1` (ou `PAR-2`) |
| Type | une instance avec **8 Go de RAM** (gamme Development ou Production-Optimized ; 4 Go peut passer avec le swap mais c'est juste) |
| Image | **Ubuntu 24.04 LTS** |
| Volume | 20 Go suffisent |
| IP | une IPv4 publique (flexible IP) |
| Clé SSH | ajouter ta clé publique (`~/.ssh/id_ed25519.pub`) |

Dans le **security group** de l'instance, autoriser en entrée TCP 22 (SSH) et TCP 80 (dashboard).

## 2. Installer et lancer

```bash
ssh root@<IP>
curl -fsSL https://raw.githubusercontent.com/eliottguls/rennes-velo-streaming/main/deploy/setup-server.sh | bash
```

Le script installe Docker, active le pare-feu, crée 2 Go de swap, clone le repo, génère des mots de passe dans `.env`
et lance `docker compose up -d --build`. Le dashboard est ensuite sur `http://<IP>` (lecture anonyme activée).

## 3. Vérifier

```bash
cd /opt/rennes-velo-streaming
docker compose ps
docker compose logs -f bus-producer      # affiche la cadence réelle du flux GTFS-RT ("nouveau flux, Xs après le précédent")
docker compose logs -f velo-producer
```

L'interface Flink et Redpanda Console ne sont exposées que sur `127.0.0.1`. Pour y accéder depuis ton poste :

```bash
ssh -L 8081:localhost:8081 -L 8080:localhost:8080 root@<IP>
# puis http://localhost:8081 (Flink) ; Redpanda Console : docker compose --profile debug up -d console
```

## 4. Mettre à jour

```bash
cd /opt/rennes-velo-streaming && git pull && docker compose up -d --build
```

Les jobs Flink déjà en cours ne sont pas resoumis. Pour déployer une nouvelle version d'un fichier SQL :
annuler le job dans l'UI Flink (ou `docker compose exec jobmanager flink cancel <job-id>`), puis
`docker compose up -d flink-submit`. Les offsets Kafka étant commités par le job, il reprend où il s'était arrêté.

## Option : nom de domaine et HTTPS

Pointer un enregistrement DNS `A` vers l'IP, puis mettre Caddy devant Grafana (port 80/443) :
`GRAFANA_PORT=3000` dans `.env` et un `Caddyfile` contenant `velo.mondomaine.fr { reverse_proxy localhost:3000 }`.
