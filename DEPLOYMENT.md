# Deployment

Docker Compose provisions the verified catalog and model, then runs the API on port `8082`.
Named volumes preserve the model, catalog, cold-start cache, event logs, live profiles, and worker state.

## Start

The VM needs Docker Engine with the Compose plugin. Connect over SSH, then run:

```bash
ssh <user>@<VM-address>
git clone https://github.com/Revenge-of-the-Fit/RecommendationModel.git
cd RecommendationModel
```

Set up the course tunnel using the [course script](https://raw.githubusercontent.com/mlip-cmu-online/lab-kafka/main/connect-kafka):

```bash
curl -fsS https://raw.githubusercontent.com/mlip-cmu-online/lab-kafka/main/connect-kafka -o connect-kafka
bash ./connect-kafka kafkatunnel@128.2.24.239 9092
```

Use the tunnel and Kafka passwords from Canvas when prompted. The script starts a
background SSH tunnel on port `9092` and writes a private `~/.config/mlip-kafka.conf`.
The tunnel must be running on the VM that runs Docker.

Create a private `.env` in the checkout with the team OpenAI key and the **absolute**
configuration directory generated above:

```dotenv
OPENAI_API_KEY=<team key>
KAFKA_CONFIG_DIRECTORY=/home/<user>/.config
```

Then start:

```bash
chmod 600 .env
APP_REVISION="$(git rev-parse HEAD)" docker compose up -d --build
```

The Docker Kafka service can read the private configuration before dropping privileges.
Keep `.env` and `mlip-kafka.conf` outside Git. Port `8082` must be reachable by the course server.

The first start downloads the catalog and trains the model. Later starts reuse the model volume.
To replace it, prefix the Compose command with `RETRAIN_MODEL=1`.

## Our existing VM

The shared deployment is already configured at `/home/production`. It is an exported
checkout, so it has no `.git` directory; its private `.env` supplies `APP_REVISION`.
From a teammate account, start the existing deployment with:

```bash
ssh <andrew-id>@CMU-ONLINE-MLIP-VM02.ANDREW.CMU.EDU
cd /home/production
sudo docker compose --project-name recommendationmodel up -d --no-build --no-recreate
```

This reuses the existing images and state. To build a source update already copied into
this directory, use `up -d --build` and set `APP_REVISION` to that source's commit.
The existing tunnel and private configuration are shared team setup; a new login alone
does not replace them.

## Verify

```bash
docker compose ps
curl -fsS http://localhost:8082/health/ready
curl -fsS http://localhost:8082/recommend/1
docker compose exec api cat /app/models/artifact-manifest.json
docker compose exec api printenv APP_REVISION
```

The manifest revision describes model provisioning. `APP_REVISION` describes the API
deployment; the saved verification also checks the API source and model fingerprints.

Also test `http://<VM-address>:8082/recommend/1` from outside the VM. Allow inbound TCP
port `8082` in the VM firewall if required.

Example for our VM:
```bash
curl.exe -i http://CMU-ONLINE-MLIP-VM02.ANDREW.CMU.EDU:8082/health/ready
curl.exe -i http://CMU-ONLINE-MLIP-VM02.ANDREW.CMU.EDU:8082/recommend/1
```

To confirm profile state survives container replacement:

```bash
docker compose exec api sh -c 'echo ok > /app/state/profiles/persistence-check'
docker compose up -d --force-recreate api
docker compose exec api cat /app/state/profiles/persistence-check
```

On our configured VM, use `sudo docker compose --project-name recommendationmodel`
and add `--no-build --no-deps` to `up` when replacing only the existing API image.
Replacement currently interrupts serving while resources load (about 48 seconds in
the October 11 check). Real request/profile/cache checks are saved in [verification](verification/README.md).

## Update or stop

```bash
git pull --ff-only
APP_REVISION="$(git rev-parse HEAD)" docker compose up -d --build
docker compose down
```

`docker compose down` keeps the named volumes. Do not add `--volumes` unless the stored state
should be deleted.
