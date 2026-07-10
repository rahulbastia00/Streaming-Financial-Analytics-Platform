import subprocess
import time
import sys

# Define the 6 commands we want to execute simultaneously
commands = [
    [sys.executable, "-m", "snowflake_consumer"],
    [sys.executable, "-m", "scripts.producer_finnhub"],
    [sys.executable, "-m", "scripts.producer_alltick"],
    [sys.executable, "-m", "scripts.producer_marketaux"],
    [sys.executable, "-m", "scripts.producer_newsapi"],
    [sys.executable, "-m", "scripts.producer_alphavantage"],
]

processes = []

print("[*] Launching the complete Financial Data Stack...")

try:
    # Loop through and boot each script up in the background
    for cmd in commands:
        # subrocess.Popen runs the file without locking your terminal screen
        p = subprocess.Popen(cmd)
        processes.append(p)
        time.sleep(1)  # Small 1-second pause to let logs print cleanly

    print("\n[+] Success! All 6 channels are streaming live in the background.")
    print("[!] PRESS CTRL+C IN THIS WINDOW TO STOP THE ENTIRE PIPELINE AT ONCE.\n")

    # Keep this master manager script alive while background tasks are running
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print(
        "\n[!] Emergency Stop Triggered! Shutting down all background pipelines safely..."
    )
    # DataOps Cleanup: Cleanly terminate all child operations
    for p in processes:
        p.terminate()
    print("[+] System offline. Clean exit achieved.")
