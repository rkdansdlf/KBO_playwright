import os
import subprocess

years = list(range(2024, 2009, -1))
print(f"Starting sequential game detail sync for years: {years}")

for year in years:
    print(f"\n🚀 Syncing Year {year}...")
    log_file = f"data/sync_oci_{year}_v4.log"
    cmd = ["venv/bin/python3", "src/cli/sync_oci.py", "--game-details", "--year", str(year)]
    env = os.environ.copy()
    env["PYTHONPATH"] = env.get("PYTHONPATH", "") + ":."

    with open(log_file, "w") as f:
        try:
            subprocess.run(cmd, env=env, check=True, stdout=f, stderr=subprocess.STDOUT)
            print(f"✅ Year {year} sync finished.")
        except subprocess.CalledProcessError:
            print(f"❌ Year {year} sync failed. Check {log_file}")

print("\n🎉 All requested years processed.")
