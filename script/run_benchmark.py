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

def run_cmd(type, cmd, log_file, results_file):
    num_runs = 2
    for run_id in range(1, num_runs+1):
        run_desc = f"{type}: [run={run_id}]"
        print(f"Running: {run_desc}")
        print(f"Command: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        log = result.stdout.decode()
        results = result.stderr.decode()
        log_file.write(f"{log}")
        results_file.write(f"{results}")

def run_build(n, base_filename=None):
    min_lambda = 2.0
    max_lambda = 10.0
    min_alpha = 0.94
    max_alpha = 0.97
    lambda_values = np.arange(min_lambda, max_lambda + 0.1, 1.0)
    alpha_values = np.arange(min_alpha, max_alpha + 0.01, 0.03)
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
            for b in ["skew", "opt"]:

                cmd = [
                    "./build",
                    "-n", str(n),
                    "-l", str(l),
                    "-a", str(a),
                    "-e", "all",
                    "-b", b,
                    "-s", "0",
                    "-q", str(n),
                    "-t", str(num_threads),
                    "--minimal",
                    "--check",
                    "--verbose",
                    "--cache-input"
                ]

                run_cmd("SINGLE", cmd, log_file, results_file)

                avg_partition_size = n / (num_threads * num_partitions_per_thread)
                run_cmd("PARTITIONED", cmd + ["-p", str(avg_partition_size)], log_file, results_file)

                if a == max_alpha:
                    run_cmd("DENSE-PARTITIONED", cmd + ["--dense"], log_file, results_file)

    log_file.close()
    results_file.close()
    stop_timestamp = get_iso_timestamp()
    print(f"\n==== Run completed at {stop_timestamp} ====\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run build command with parameter sweep.")
    parser.add_argument("-n", type=int, required=True, help="Number of keys for benchmark. It must be > 4096.")
    parser.add_argument("-o", "--output", type=str, required=False, help="Base output filename (no extension). If omitted, a timestamp is used.")

    args = parser.parse_args()
    run_build(args.n, args.output)
