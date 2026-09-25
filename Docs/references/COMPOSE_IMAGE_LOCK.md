# Compose Image Lock

Every image used by `docker-compose.yml`, `docker-compose.dev.yml`, and
`docker-compose.prod.yml` is pinned to a digest. A tag is a mutable pointer:
`latest` can pull a different build on a machine that happens to be restarting,
which makes an incident unreproducible.

## Pinned images

| Image | Digest |
| --- | --- |
| `ghcr.io/browserless/chromium:latest` | `sha256:ae07025606e03c9b263620aaf4d09c52728f71a104ac7d471a68c23dec72c05f` |
| `prom/prometheus:latest` | `sha256:efd719c99d83b060d9daefdcf00360461adf279f45ef5391f8d111892118753e` |
| `grafana/grafana:latest` | `sha256:ac461fb352abc50da10a51c7d02462e9c05488f11f53f14b3ad79a8145f638a0` |
| `prom/alertmanager:v0.33.1` | `sha256:9e082985f56f4c8c9f724e18f2288c6708f472e56a5286b8863d080434ea065d` |
| `postgres:16` | `sha256:1a6ab3f5345eb6dbe04a1349529caabdb0ab09293a09590fad07b2246bfa4b54` |
| `pgvector/pgvector:pg16` | `sha256:ccc6e83d6e35e931dc7c5def2022729d5a6c370318d099181995567ff1fb4d6b` |

The first three are still referenced by a mutable `latest` tag on purpose: the
digest is what Compose resolves, and the tag is kept only so a human reading
`docker compose config` can tell what the digest corresponds to. Compose accepts
`image: name:tag@sha256:...`, and the digest always wins.

## Refreshing a pin

```bash
docker buildx imagetools inspect <image>:<tag> | awk '/^Digest:/{print $2; exit}'
```

Then update the compose file, this table, and run:

```bash
python3 -m src.cli.kbo --help            # unaffected, but cheap
python3 -m pytest tests/test_github_workflows.py tests/docker -q
docker compose -f docker-compose.dev.yml config >/dev/null
docker compose -f docker-compose.prod.yml config >/dev/null
```

`tests/test_docker_compose_contract.py` fails if any compose file uses a bare
`:latest` with no digest, if the prod file publishes an observability port, or
if the prod Grafana service leaves anonymous access enabled.

## Why digests and not version tags

A version tag such as `grafana/grafana:11.3.0` is better than `latest` but still
a tag: a publisher can re-tag or delete it. Since these services hold scheduler
state volumes, an unpinned upgrade is a silent data-shape change. The cost of a
digest is a manual refresh; `Docs/runbooks/OPERATIONAL_RUNBOOK.md` covers it.

The project's own images (`scheduler`, `api-server`, `text-relay`) are built
locally from the `Dockerfile` and are tagged by `docker_build.yml`; they are not
pinned here because their content is the commit being built.
