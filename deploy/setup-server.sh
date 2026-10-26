#!/usr/bin/env bash
# Prépare une instance Ubuntu 24.04 fraîche (Scaleway ou autre) et lance la stack.
# Usage, en root sur le serveur :
#   curl -fsSL https://raw.githubusercontent.com/eliottguls/rennes-velo-streaming/main/deploy/setup-server.sh | bash
set -euo pipefail

REPO="${REPO:-https://github.com/eliottguls/rennes-velo-streaming.git}"
DIR=/opt/rennes-velo-streaming

# Docker + plugin compose
if ! command -v docker >/dev/null; then
  curl -fsSL https://get.docker.com | sh
fi

# Pare-feu : SSH et Grafana (servi en HTTP sur le port 80) uniquement
apt-get install -y ufw git
ufw allow OpenSSH
ufw allow 80/tcp
ufw --force enable

# Un peu de swap pour absorber les pics mémoire de Flink sur les petites instances
if ! swapon --show | grep -q /swapfile; then
  fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

if [ ! -d "$DIR" ]; then
  git clone "$REPO" "$DIR"
fi
cd "$DIR"
git pull --ff-only

if [ ! -f .env ]; then
  cp .env.example .env
  sed -i "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=$(openssl rand -hex 16)/" .env
  sed -i "s/^GRAFANA_ADMIN_PASSWORD=.*/GRAFANA_ADMIN_PASSWORD=$(openssl rand -hex 12)/" .env
  sed -i "s/^GRAFANA_PORT=.*/GRAFANA_PORT=80/" .env
  IP=$(curl -fsS https://api.ipify.org || hostname -I | awk '{print $1}')
  sed -i "s|^GRAFANA_ROOT_URL=.*|GRAFANA_ROOT_URL=http://$IP|" .env
fi

docker compose up -d --build
echo
echo "Stack lancée. Dashboard : $(grep GRAFANA_ROOT_URL .env | cut -d= -f2)"
echo "Mot de passe admin Grafana : $(grep GRAFANA_ADMIN_PASSWORD .env | cut -d= -f2)"
