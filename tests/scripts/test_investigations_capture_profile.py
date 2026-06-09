
# This script runs an async Playwright browser; test that the module loads.
def test_module_imports():
    import scripts.investigations.capture_profile as mod
    assert mod is not None
    assert callable(mod.main)
