import os
import subprocess
import sys

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)

years = list(range(2024, 2009, -1))
print(f"Starting sequential game detail sync for years: {years}")

for year in years:
    print(f"\nSyncing Year {year}...")
    log_file = os.path.join(project_root, "data", f"sync_oci_{year}_v4.log")
    cmd = [sys.executable, "-m", "src.cli.sync_oci", "--game-details", "--year", str(year)]
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{project_root}{os.pathsep}{env.get('PYTHONPATH', '')}"

    with open(log_file, "w") as f:
        try:
            subprocess.run(cmd, env=env, check=True, stdout=f, stderr=subprocess.STDOUT, cwd=project_root)
            print(f"Year {year} sync finished.")
        except subprocess.CalledProcessError:
            print(f"Year {year} sync failed. Check {log_file}")

print("All requested years processed.")
