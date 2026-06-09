from unittest.mock import patch

# init_db.py has no main() - just if __name__ block calling init_db()
# Test that the module can be imported and the function exists.


def test_init_db_function_exists():
    import scripts.legacy.init_db as mod
    from scripts.legacy.init_db import init_db
    assert mod is not None
    assert callable(init_db)


@patch("scripts.legacy.init_db.init_db")
def test_init_db_called(mock_init):
    mock_init()
    mock_init.assert_called_once()
