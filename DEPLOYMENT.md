# Deployment

Docker Compose provisions the verified catalog and model, then runs the API on port `8082`.
Named volumes preserve the model, catalog, cold-start cache, event logs, live profiles, and worker state.

## Start

The VM needs Docker Engine with the Compose plugin. Connect over SSH, then run:

```bash
ssh <user>@<VM-address>
git clone https://github.com/Revenge-of-the-Fit/RecommendationModel.git
cd RecommendationModel
APP_REVISION="$(git rev-parse HEAD)" docker compose up -d --build
```

The first start downloads the catalog and trains the model. Later starts reuse the model volume.
To replace it, prefix the Compose command with `RETRAIN_MODEL=1`.

## Verify

```bash
docker compose ps
curl -fsS http://localhost:8082/health/ready
curl -fsS http://localhost:8082/recommend/1
docker compose exec api cat /app/models/artifact-manifest.json
```

Also test `http://<VM-address>:8082/recommend/1` from outside the VM. Allow inbound TCP
port `8082` in the VM firewall if required.

To confirm profile state survives container replacement:

```bash
docker compose exec api sh -c 'echo ok > /app/state/profiles/persistence-check'
docker compose up -d --force-recreate api
docker compose exec api cat /app/state/profiles/persistence-check
```

## Update or stop

```bash
git pull --ff-only
APP_REVISION="$(git rev-parse HEAD)" docker compose up -d --build
docker compose down
```

`docker compose down` keeps the named volumes. Do not add `--volumes` unless the stored state
should be deleted.
