import subprocess
import argparse
import numpy as np
from datetime import datetime, timedelta
import math

def get_iso_timestamp():
    now = datetime.now()

    # Ceil the seconds
    fractional_sec = now.microsecond / 1_000_000
    ceil_seconds = now.second + math.ceil(fractional_sec)

    if ceil_seconds >= 60:
        now += timedelta(minutes=1)
        ceil_seconds = 0

    now = now.replace(second=ceil_seconds, microsecond=0)

    return now.isoformat()  # Example: '2025-04-12T15:30:02'

# setup
num_threads = 8

# for partitioning:
# avg_partition_size is calculated as n / (num_threads * num_partitions_per_thread)
num_partitions_per_thread = 4

# for dense partitioning:
avg_partition_size_dense_partitioning = 3500

def run_cmd(type, l, a, cmd, log_file, results_file):
    for run_id in range(1, 4): # Repeat each run 3 times
        run_desc = f"{type}: [l={l:.2f}, a={a:.2f}, run={run_id}]"
        print(f"Running: {run_desc}")
        print(f"Command: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        log = result.stdout.decode()
        results = result.stderr.decode()
        log_file.write(f"{log}")
        results_file.write(f"{results}")

def run_build(n, base_filename=None):
    lambda_values = np.arange(2.0, 5.1, 0.5)
    alpha_values = np.arange(0.94, 1.001, 0.02)
    start_timestamp = get_iso_timestamp().replace(":", "-")

    if base_filename == None:
        base_filename = start_timestamp

    log_filename = "results." + base_filename + ".log"
    results_filename = "results." + base_filename + ".json"

    log_file = open(log_filename, 'w')
    results_file = open(results_filename, 'w')
    print(f"\n==== Run started at {start_timestamp} with n={n} ====\n")

    for l in lambda_values:
        for a in alpha_values:

            cmd = [
                "./build",
                "-n", str(n),
                "-l", f"{l:.2f}",
                "-a", f"{a:.2f}",
                "-e", "all",
                "-r", "xor",
                "-b", "skew",
                "-s", "0",
                "-q", str(n),
                "-t", str(num_threads),
                "--minimal",
                "--check",
                "--verbose"
            ]

            run_cmd("SINGLE", l, a, cmd, log_file, results_file)

            avg_partition_size = n / (num_threads * num_partitions_per_thread)
            run_cmd("PARTITIONED", l, a, cmd + ["-p", str(avg_partition_size)], log_file, results_file)

            avg_partition_size = avg_partition_size_dense_partitioning
            run_cmd("DENSE-PARTITIONED", l, a, cmd + ["-p", str(avg_partition_size), "--dense"], log_file, results_file)

    log_file.close()
    results_file.close()
    stop_timestamp = get_iso_timestamp()
    print(f"\n==== Run completed at {stop_timestamp} ====\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run build command with parameter sweep.")
    parser.add_argument("-n", type=int, required=True, help="Number of keys for benchmark")
    parser.add_argument("-o", "--output", type=str, required=False, help="Base output filename (no extension). If omitted, a timestamp is used.")

    args = parser.parse_args()
    run_build(args.n, args.output)
