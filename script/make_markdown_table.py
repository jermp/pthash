import sys
import json, math
from collections import defaultdict

def process_json_file(input_filename):
    # Read the JSON lines
    with open(input_filename, 'r') as file:
        lines = [json.loads(line.strip()) for line in file]

    averaged_rows = []
    temp_rows = []

    # Iterate through rows and calculate averages every three rows
    for idx, row in enumerate(lines):
        temp_rows.append(row)
        if (idx + 1) % 3 == 0:
            avg_row = calculate_average_row(temp_rows)
            averaged_rows.append(avg_row)
            temp_rows = []

    # Generate the markdown table
    return generate_markdown_table(averaged_rows)

def calculate_average_row(rows):
    # Base row parameters (assuming all three rows have identical non-average fields)
    base_row = rows[0]

    averaged_row = {
        "encoder_type": base_row["encoder_type"],
        "alpha": base_row["alpha"],
        "lambda": base_row["lambda"],
        "bits_per_key": base_row["bits_per_key"],
    }

    # Calculate averages for the specified fields
    fields_to_average = [
        "partitioning_microseconds",
        "mapping_ordering_microseconds",
        "searching_microseconds",
        "encoding_microseconds",
        "total_microseconds",
        "nanosec_per_key"
    ]

    for field in fields_to_average:
        averaged_row[field] = sum(float(row[field]) for row in rows) / len(rows)

    return averaged_row

def generate_markdown_table(averaged_rows):
    header = (
        "| Encoder | $\\alpha$ | $\\lambda$ | Mapping (sec) | Mapping (%) | Searching (sec) | Searching (%) "
        "| Encoding (sec) | Encoding (%) | Total (sec) | Space (bits/key) | Lookup (ns/key) |\n"
        "|:---------:|:-------:|:--------:|:-------------:|:-------------:|:--------------:|:----------------:"
        "|:--------------:|:--------------:|:-----------:|:----------:|:--------------:|\n"
    )

    rows = []
    for row in averaged_rows:
        total_sec = row["total_microseconds"] / 1e6
        mapping_sec = row["mapping_ordering_microseconds"] / 1e6
        searching_sec = row["searching_microseconds"] / 1e6
        encoding_sec = row["encoding_microseconds"] / 1e6

        mapping_pct = (mapping_sec / total_sec) * 100
        searching_pct = (searching_sec / total_sec) * 100
        encoding_pct = (encoding_sec / total_sec) * 100

        markdown_row = (
            f"| {row['encoder_type']} | {float(row['alpha']):.2f} | {float(row['lambda']):.3f} | {mapping_sec:.2f} | {mapping_pct:.2f} "
            f"| {searching_sec:.2f} | {searching_pct:.2f} "
            f"| {encoding_sec:.2f} | {encoding_pct:.2f} "
            f"| {total_sec:.2f} | {float(row['bits_per_key']):.2f} | {row['nanosec_per_key']:.0f} |"
        )
        rows.append(markdown_row)

    return header + "\n".join(rows)


input_filename = sys.argv[1] # should have a .json extension
output_filename = sys.argv[2] # should have a .md extension

# Generate the markdown table
markdown_table = process_json_file(input_filename)

# Save or print the table
with open(output_filename, 'w') as output_file:
    output_file.write(markdown_table)

# print(markdown_table)
