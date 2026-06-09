from unittest.mock import MagicMock, patch


class TestBenchmarkOCISync:
    def test_main_quick(self):
        with patch("scripts.benchmark_oci_sync.bench_table_sweep") as mock_table, \
             patch("scripts.benchmark_oci_sync.bench_dirty_detection") as mock_dirty, \
             patch("scripts.benchmark_oci_sync.bench_connection_overhead") as mock_conn, \
             patch("sys.argv", ["script", "--quick"]):
            from scripts.benchmark_oci_sync import main
            mock_suite = MagicMock()
            mock_table.return_value = mock_suite
            mock_dirty.return_value = mock_suite
            mock_conn.return_value = mock_suite
            main()

    def test_table_sweep_only(self):
        with patch("scripts.benchmark_oci_sync.bench_table_sweep") as mock_table, \
             patch("sys.argv", ["script", "--table-sweep"]):
            from scripts.benchmark_oci_sync import main
            mock_suite = MagicMock()
            mock_table.return_value = mock_suite
            main()

    def test_dirty_detection(self):
        with patch("scripts.benchmark_oci_sync.bench_dirty_detection") as mock_dirty, \
             patch("sys.argv", ["script", "--dirty-sweep"]):
            from scripts.benchmark_oci_sync import main
            mock_suite = MagicMock()
            mock_dirty.return_value = mock_suite
            main()
