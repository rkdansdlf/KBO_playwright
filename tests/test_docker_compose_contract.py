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
