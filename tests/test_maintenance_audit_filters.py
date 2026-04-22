import scripts.maintenance.audit_2026_season as audit_2026
import scripts.maintenance.check_player_integrity as player_integrity


class _FakeQuery:
    def __init__(self):
        self.filters = []

    def filter(self, *conditions):
        self.filters.extend(conditions)
        return self

    def all(self):
        return []


class _FakeAuditSession:
    def __init__(self):
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def query(self, *_models):
        query = _FakeQuery()
        self.queries.append(query)
        return query


class _FakeResult:
    def fetchall(self):
        return []


class _FakeIntegritySession:
    def __init__(self):
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement):
        self.statements.append(str(statement))
        return _FakeResult()


def _compile_condition(condition) -> str:
    return str(condition.compile(compile_kwargs={"literal_binds": True}))


def test_2026_audit_excludes_cancelled_and_postponed_games(monkeypatch):
    session = _FakeAuditSession()
    monkeypatch.setattr(audit_2026, "SessionLocal", lambda: session)

    audit_2026.audit_2026_season()

    filters = [_compile_condition(condition) for condition in session.queries[0].filters]
    assert any("game.game_date LIKE '2026%'" in condition for condition in filters)
    assert any(
        "game.game_status NOT IN ('CANCELLED', 'POSTPONED')" in condition
        for condition in filters
    )


def test_player_integrity_reconciles_regular_season_only(monkeypatch):
    session = _FakeIntegritySession()
    monkeypatch.setattr(player_integrity, "SessionLocal", lambda: session)

    player_integrity.run_audit()

    reconciliation_query = next(
        statement
        for statement in session.statements
        if "WITH game_sums AS" in statement
    )
    assert "s.season = 2024" in reconciliation_query
    assert "s.league = 'REGULAR'" in reconciliation_query
