"""Docker Compose contract: pinned images and a hardened production profile.

The dev and prod stacks share a history, and the failure mode that matters is a
dev-only convenience (anonymous Grafana, a published Prometheus port, a floating
`latest`) quietly becoming the production default. These tests make that
impossible to merge by accident.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DEV_COMPOSE = ROOT / "docker-compose.dev.yml"
PROD_COMPOSE = ROOT / "docker-compose.prod.yml"
IMAGE_LOCK_DOC = ROOT / "Docs" / "references" / "COMPOSE_IMAGE_LOCK.md"

#: Services whose ports must not be reachable from outside in production.
INTERNAL_ONLY_SERVICES = ("prometheus", "alertmanager", "grafana", "browserless")

#: Services that only exist for local development.
DEV_ONLY_SERVICES = ("postgres", "pgvector")

DIGEST_PATTERN = re.compile(r"@sha256:[0-9a-f]{64}$")


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def dev() -> dict:
    return _load(DEV_COMPOSE)


@pytest.fixture(scope="module")
def prod() -> dict:
    return _load(PROD_COMPOSE)


def _services(doc: dict) -> dict:
    return doc.get("services", {})


def _env_items(service: dict) -> list[str]:
    """Return a service's environment as `KEY=VALUE` strings.

    Compose accepts both a mapping and a list here, and this repository uses
    both shapes across services, so a contract test cannot assume either.
    """
    env = service.get("environment", {})
    if isinstance(env, dict):
        return [f"{key}={value}" for key, value in env.items()]
    return [str(item) for item in env]


class TestImagesArePinned:
    @pytest.mark.parametrize("compose", [DEV_COMPOSE, PROD_COMPOSE], ids=lambda p: p.name)
    def test_every_image_carries_a_digest(self, compose: Path) -> None:
        unpinned = []
        for name, service in _services(_load(compose)).items():
            image = service.get("image")
            if image and not DIGEST_PATTERN.search(image):
                unpinned.append(f"{name}: {image}")
        assert not unpinned, f"images without a digest: {unpinned}"

    def test_project_images_are_built_not_pulled(self, prod: dict) -> None:
        # These are built from the Dockerfile; a digest would defeat the build.
        for name in ("scheduler", "api-server"):
            assert "build" in _services(prod)[name]
            assert "image" not in _services(prod)[name]

    def test_image_lock_doc_records_every_pinned_digest(self) -> None:
        lock = IMAGE_LOCK_DOC.read_text(encoding="utf-8")
        for compose in (DEV_COMPOSE, PROD_COMPOSE):
            for service in _services(_load(compose)).values():
                image = service.get("image")
                if not image:
                    continue
                digest = DIGEST_PATTERN.search(image).group(0).lstrip("@")  # type: ignore[union-attr]
                assert digest in lock, f"{image} is not recorded in {IMAGE_LOCK_DOC.name}"


class TestProductionHardening:
    @pytest.mark.parametrize("service", INTERNAL_ONLY_SERVICES)
    def test_observability_ports_are_not_published(self, prod: dict, service: str) -> None:
        assert "ports" not in _services(prod)[service], f"{service} publishes a port in production"

    @pytest.mark.parametrize("service", DEV_ONLY_SERVICES)
    def test_dev_only_services_are_absent_from_production(self, prod: dict, service: str) -> None:
        assert service not in _services(prod), f"{service} is a local-only service"

    def test_grafana_anonymous_access_is_disabled(self, prod: dict) -> None:
        assert "GF_AUTH_ANONYMOUS_ENABLED=false" in _env_items(_services(prod)["grafana"])

    def test_grafana_admin_password_has_no_default(self, prod: dict) -> None:
        env = _env_items(_services(prod)["grafana"])
        passwords = [item for item in env if item.startswith("GF_SECURITY_ADMIN_PASSWORD=")]
        assert passwords, "Grafana admin password is not configured"
        # `:?` makes Compose fail rather than fall back to a known password.
        assert ":?" in passwords[0], passwords[0]

    def test_database_url_is_required(self, prod: dict) -> None:
        for name in ("scheduler", "api-server"):
            env = _env_items(_services(prod)[name])
            assert any(item.startswith("DATABASE_URL=") and ":?" in item for item in env), name

    def test_wallet_path_is_required(self, prod: dict) -> None:
        for name in ("scheduler", "api-server"):
            volumes = _services(prod)[name]["volumes"]
            assert any(isinstance(v, str) and "ORACLE_WALLET_HOST_PATH:?" in v for v in volumes), name

    def test_api_is_the_only_published_service(self, prod: dict) -> None:
        published = [name for name, svc in _services(prod).items() if "ports" in svc]
        assert published == ["api-server"], published

    def test_api_is_bound_to_loopback(self, prod: dict) -> None:
        for port in _services(prod)["api-server"]["ports"]:
            assert port.startswith("127.0.0.1:"), port


class TestDevStackKeepsItsConveniences:
    def test_dev_still_publishes_grafana(self, dev: dict) -> None:
        assert "ports" in _services(dev)["grafana"]

    def test_dev_allows_anonymous_read_only_access(self, dev: dict) -> None:
        env = _env_items(_services(dev)["grafana"])
        assert any(item.startswith("GF_AUTH_ANONYMOUS_ENABLED=") for item in env)

    def test_dev_keeps_local_databases(self, dev: dict) -> None:
        services = _services(dev)
        for name in DEV_ONLY_SERVICES:
            assert name in services, name
            assert "profiles" in services[name], f"{name} must stay opt-in via a profile"


@pytest.mark.parametrize("compose_path", [DEV_COMPOSE, PROD_COMPOSE], ids=["dev", "prod"])
def test_playwright_browsers_come_from_the_image_not_a_volume(compose_path: Path) -> None:
    """No service may mount over ``/ms-playwright``.

    ``python -m playwright install chromium`` runs at image build, so the browser
    revision always matches the installed ``playwright`` package. A named volume
    on that path shadows the image: Docker fills only an *empty* volume from the
    image, so after a ``playwright`` upgrade an existing volume keeps the old
    revision and every launch fails with "Executable doesn't exist".
    """
    doc = _load(compose_path)

    mounts = [
        (name, volume)
        for name, service in _services(doc).items()
        for volume in service.get("volumes", [])
        if "/ms-playwright" in (volume if isinstance(volume, str) else str(volume.get("target", "")))
    ]
    assert mounts == []
    assert "playwright-browsers" not in (doc.get("volumes") or {})


def test_dev_stack_does_not_require_an_oracle_wallet(dev: dict) -> None:
    """The dev stack runs on PostgreSQL; a missing wallet path must not block ``up``.

    ``${ORACLE_WALLET_HOST_PATH:?...}`` made ``docker compose up`` fail at interpolation for
    every service that mounted it, so recreating a container needed a placeholder directory.
    Production keeps the requirement (see ``test_wallet_path_is_required``).
    """
    for name, service in _services(dev).items():
        for volume in service.get("volumes", []):
            assert "ORACLE_WALLET_HOST_PATH" not in str(volume), name
        assert "TNS_ADMIN" not in " ".join(_env_items(service)), name


LOCAL_COMPOSE = ROOT / "docker-compose.local.yml"


def _published_ports(doc: dict) -> list[tuple[str, str]]:
    """Return ``(service, port_spec)`` for every published port in a compose document."""
    return [(name, str(port)) for name, service in _services(doc).items() for port in service.get("ports", [])]


@pytest.mark.parametrize("compose_path", [DEV_COMPOSE, LOCAL_COMPOSE], ids=["dev", "local"])
def test_published_ports_are_never_bound_to_every_interface(compose_path: Path) -> None:
    """A bare ``HOST:CONTAINER`` mapping listens on 0.0.0.0, including a public interface.

    Docker Desktop on Windows also opens the host firewall for ``com.docker.backend.exe`` on the Public
    profile, so a bare mapping on a host with a public address exposes Prometheus, Alertmanager, anonymous
    Grafana, browserless and the databases to the internet. Every published port must name its bind address,
    and the default must be loopback.
    """
    ports = _published_ports(_load(compose_path))
    assert ports, "expected at least one published port"

    for service, spec in ports:
        assert spec.startswith("${KBO_BIND_ADDR:-127.0.0.1}:"), f"{service}: {spec} is not bound to KBO_BIND_ADDR"
        assert not spec.startswith("0.0.0.0"), f"{service}: {spec} binds every interface"


@pytest.mark.parametrize("compose_path", [DEV_COMPOSE, PROD_COMPOSE], ids=["dev", "prod"])
def test_scheduler_keeps_at_least_a_week_of_logs_room(compose_path: Path) -> None:
    """The nightly sync reports failures only through the scheduler's stdout.

    With ``10m x 3`` the json-file driver rotated them away within two days, so the
    cause of a partial failure was gone by the time anyone looked. Retention must be
    at least 200MB so a failure survives several nights.
    """
    options = _services(_load(compose_path))["scheduler"]["logging"]["options"]
    size_mb = int(options["max-size"].rstrip("m"))
    assert size_mb * int(options["max-file"]) >= 200
