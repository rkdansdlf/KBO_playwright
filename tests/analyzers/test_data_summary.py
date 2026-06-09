from unittest.mock import MagicMock, patch

from src.analyzers.data_summary import (
    _fmt,
    _int,
    analyze_events,
    analyze_food,
    analyze_parking,
    analyze_roster,
    analyze_seats,
    analyze_tickets,
    generate_report,
)


class TestHelpers:
    def test_fmt_none_returns_dash(self):
        assert _fmt(None) == "-"

    def test_fmt_value_returns_str(self):
        assert _fmt(42) == "42"
        assert _fmt("hello") == "hello"

    def test_int_none_returns_zero(self):
        assert _int(None) == 0

    def test_int_value_returns_int(self):
        assert _int("5") == 5
        assert _int(3) == 3


class TestAnalyzeEvents:
    @patch("src.analyzers.data_summary.SessionLocal")
    def test_analyze_events_returns_summary_and_rows(self, mock_session_local):
        mock_session = MagicMock()
        mock_session_local.return_value.__enter__.return_value = mock_session

        mock_session.execute.return_value.scalar.return_value = 10
        mock_session.execute.return_value.fetchall.return_value = [
            MagicMock(team_id="LG", event_type="CANCEL", cnt=3, last_event="2025-01-01"),
        ]

        result = analyze_events()

        assert result[0]["section"] == "Events"
        assert result[0]["total"] == 10
        assert result[1]["team"] == "LG"
        assert result[1]["type"] == "CANCEL"

    @patch("src.analyzers.data_summary.SessionLocal")
    def test_analyze_events_empty(self, mock_session_local):
        mock_session = MagicMock()
        mock_session_local.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.scalar.return_value = 0
        mock_session.execute.return_value.fetchall.return_value = []

        result = analyze_events()
        assert result[0]["total"] == 0
        assert len(result) == 1


class TestAnalyzeRoster:
    @patch("src.analyzers.data_summary.SessionLocal")
    def test_analyze_roster_structure(self, mock_session_local):
        mock_session = MagicMock()
        mock_session_local.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.scalar.return_value = 20
        mock_session.execute.return_value.fetchall.return_value = []

        result = analyze_roster()
        assert result[0]["section"] == "Roster"
        assert result[0]["total"] == 20


class TestAnalyzeTickets:
    @patch("src.analyzers.data_summary.SessionLocal")
    def test_analyze_tickets_structure(self, mock_session_local):
        mock_session = MagicMock()
        mock_session_local.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.scalar.return_value = 50
        mock_session.execute.return_value.fetchall.return_value = []

        result = analyze_tickets()
        assert result[0]["section"] == "Ticket"
        assert result[0]["total"] == 50


class TestAnalyzeSeats:
    @patch("src.analyzers.data_summary.SessionLocal")
    def test_analyze_seats_structure(self, mock_session_local):
        mock_session = MagicMock()
        mock_session_local.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.scalar.return_value = 30
        mock_session.execute.return_value.fetchall.return_value = []

        result = analyze_seats()
        assert result[0]["section"] == "Seats"
        assert result[0]["total"] == 30


class TestAnalyzeParking:
    @patch("src.analyzers.data_summary.SessionLocal")
    def test_analyze_parking_structure(self, mock_session_local):
        mock_session = MagicMock()
        mock_session_local.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.scalar.return_value = 15
        mock_session.execute.return_value.fetchall.return_value = []

        result = analyze_parking()
        assert result[0]["section"] == "Parking"
        assert result[0]["lots"] == 15


class TestAnalyzeFood:
    @patch("src.analyzers.data_summary.SessionLocal")
    def test_analyze_food_structure(self, mock_session_local):
        mock_session = MagicMock()
        mock_session_local.return_value.__enter__.return_value = mock_session
        calls = iter([10, 25])
        mock_session.execute.return_value.scalar.side_effect = lambda: next(calls) if hasattr(calls, "__next__") else next(calls)
        mock_session.execute.return_value.fetchall.return_value = []

        mock_session.execute.side_effect = None
        mock_session.execute.side_effect = [
            MagicMock(scalar=MagicMock(return_value=10)),
            MagicMock(scalar=MagicMock(return_value=25)),
            MagicMock(fetchall=MagicMock(return_value=[])),
        ]

        result = analyze_food()
        assert result[0]["section"] == "Food"
        assert result[0]["vendors"] == 10
        assert result[0]["menu_items"] == 25


class TestGenerateReport:
    @patch("src.analyzers.data_summary.analyze_events")
    @patch("src.analyzers.data_summary.analyze_roster")
    @patch("src.analyzers.data_summary.analyze_tickets")
    @patch("src.analyzers.data_summary.analyze_seats")
    @patch("src.analyzers.data_summary.analyze_parking")
    @patch("src.analyzers.data_summary.analyze_food")
    def test_generate_report_contains_sections(self, mock_food, mock_parking, mock_seats, mock_tickets, mock_roster, mock_events):
        mock_events.return_value = [{"section": "Events", "total": 5}]
        mock_roster.return_value = [{"section": "Roster", "total": 3}]
        mock_tickets.return_value = [{"section": "Ticket", "total": 2}]
        mock_seats.return_value = [{"section": "Seats", "total": 4}]
        mock_parking.return_value = [{"section": "Parking", "lots": 1}]
        mock_food.return_value = [{"section": "Food", "vendors": 2}]

        report = generate_report()
        assert "# KBO Pipeline Data Summary" in report
        assert "## Events" in report
        assert "## Roster Transactions" in report
        assert "## Ticket Prices" in report
        assert "## Seat Sections" in report
        assert "## Parking" in report
        assert "## Food & Beverage" in report
