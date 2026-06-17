import logging
import os
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def main():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    years = list(range(2024, 2009, -1))
    logger.info(f"Starting sequential game detail sync for years: {years}")

    for year in years:
        logger.info(f"Syncing Year {year}...")
        log_file = Path(project_root, "data", f"sync_oci_{year}_v4.log")
        cmd = [sys.executable, "-m", "src.cli.sync_oci", "--game-details", "--year", str(year)]
        env = os.environ.copy()
        env["PYTHONPATH"] = f"{project_root}{os.pathsep}{env.get('PYTHONPATH', '')}"

        with log_file.open("w") as f:
            try:
                subprocess.run(cmd, env=env, check=True, stdout=f, stderr=subprocess.STDOUT, cwd=project_root)
                logger.info(f"Year {year} sync finished.")
            except subprocess.CalledProcessError:
                logger.info(f"Year {year} sync failed. Check {log_file}")

    logger.info("All requested years processed.")


if __name__ == "__main__":
    main()
