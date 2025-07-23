import gzip
import json
from pathlib import Path
from datetime import datetime

# Get the directory where the script is located
script_dir = Path(__file__).resolve().parent

# Find all .json.gz files in that directory
gz_files = script_dir.glob("*.json.gz")

# Track all receive_time values
timestamps = []

# Process each file
for gz_file in gz_files:
    # print(f"Processing file: {gz_file.name}")
    with gzip.open(gz_file, 'rt', encoding='utf-8') as f:
        for line_number, line in enumerate(f, 1):
            try:
                data = json.loads(line)
                ip = data.get("sender_ip_str")
                time_str = data.get("receive_time")
                if ip and time_str:
                    # print(f"{time_str} {ip}")
                    print(f"{ip}")
                    try:
                        time_obj = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                        timestamps.append(time_obj)
                    except ValueError:
                        pass  # Skip invalid timestamp formats
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON on line {line_number} in {gz_file.name}: {e}")

# After processing all lines, print the time span
if timestamps:
    start = min(timestamps)
    end = max(timestamps)
    print(f"\nIP requests from {start.isoformat()} to {end.isoformat()}")
else:
    print("\nNo valid receive_time values found to calculate time span.")

