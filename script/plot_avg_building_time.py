import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import FuncFormatter
import matplotlib.gridspec as gridspec
import argparse
import sys

def format_ticks(x, pos):
    return f'{x:.2f}'

def main(json_file, pdf_filename, bucketer, alpha=None):
    # Load the data from the provided JSON file
    with open(json_file, 'r') as f:
        records = [json.loads(line) for line in f]  # Read each line as a separate JSON object

    # Transform the data into a DataFrame for easier manipulation
    df = pd.DataFrame(records)

    # Convert relevant columns to numeric types
    df['total_microseconds'] = pd.to_numeric(df['total_microseconds'])
    df['lambda'] = pd.to_numeric(df['lambda'])
    df['alpha'] = pd.to_numeric(df['alpha'])

    min_y = min(df['total_microseconds']) / 1_000_000
    max_y = max(df['total_microseconds']) / 1_000_000

    # Define configurations for filtering
    configurations = [

        ((df['avg_partition_size'] == "0") & (df['num_partitions'] == "0") & (df['dense_partitioning'] == "false") & (df['bucketer_type'] == bucketer)
                , "SINGLE"),

        ((df['avg_partition_size'] != "0") & (df['num_partitions'] != "0") & (df['dense_partitioning'] == "false") & (df['bucketer_type'] == bucketer)
                , "PARTITIONED"),

        ((df['avg_partition_size'] != "0") & (df['num_partitions'] != "0") & (df['dense_partitioning'] == "true") & (df['bucketer_type'] == bucketer)
                , "DENSE-PARTITIONED")

    ]

    # Create list to hold grouped data
    grouped_data = []

    for condition, title in configurations:
        # Filter the DataFrame based on the current configuration
        filtered_df = df[condition]

        # Group by the specified fields and calculate the mean 'total_microseconds'
        grouped_avg = filtered_df.groupby([
            'n', 'lambda', 'alpha', 'minimal',
            'bucketer_type', 'avg_partition_size',
            'num_partitions', 'dense_partitioning',
            'seed', 'num_threads', 'external_memory'
        ])['total_microseconds'].mean().reset_index()

        # Convert total_microseconds to seconds
        grouped_avg['total_seconds'] = grouped_avg['total_microseconds'] / 1_000_000  # Convert microseconds to seconds

        # Store the grouped data for later use to calculate y limits
        grouped_data.append((grouped_avg, title))

    alpha_handles = []
    alpha_color_map = {}

    # Create a new PDF file to save plots
    with PdfPages(pdf_filename) as pdf:
        # Create a single row of subplots

        fig = plt.figure(figsize=(20, 8))
        gs = gridspec.GridSpec(1, 4, width_ratios=[3, 3, 3, 1])
        axs = [fig.add_subplot(gs[i]) for i in range(3)]

        i = 0
        for (grouped_avg, _) in grouped_data:
            for alpha_value in sorted(grouped_avg['alpha'].unique(), reverse=True):
                if alpha_value not in alpha_color_map.keys():
                    alpha_color_map[alpha_value] = i
                    i += 1

        colors = plt.get_cmap('tab20', len(alpha_color_map))  # Use 'tab20'

        for ax, (grouped_avg, title) in zip(axs, grouped_data):
            # Scatter plot for each unique alpha value
            for alpha_value in sorted(grouped_avg['alpha'].unique(), reverse=True):

                if alpha == None or alpha_value == alpha: # filter on specific alpha

                    color_alpha = colors(alpha_color_map[alpha_value])
                    subset = grouped_avg[grouped_avg['alpha'] == alpha_value]

                    label = rf'$\alpha$ = {float(alpha_value):.2f}'
                    if not any(label in l.get_label() for l in alpha_handles):
                        alpha_handles.append(ax.plot([], [], label=label, color=color_alpha)[0])

                    ax.plot(subset['lambda'], subset['total_seconds'],
                            marker='o', # marker_symbols[i],
                            markersize=6, color=color_alpha)

            # Set plot labels and title with LaTeX formatting
            ax.set_xlabel(r'$\lambda$', fontsize=14)
            ax.set_ylabel(r'Building time (seconds)', fontsize=14)
            ax.set_title(title, fontsize=16)  # Title for the current configuration
            ax.set_ylim(min_y, max_y)  # Set Y-axis limits

            # Set the Y-axis ticks to 10 evenly spaced ticks, including min and max
            ticks = np.linspace(min_y, max_y, 20)  # Create 10 evenly spaced ticks
            ax.set_yticks(ticks)  # Set updated ticks
            # Apply custom formatter to the Y-axis
            ax.yaxis.set_major_formatter(FuncFormatter(format_ticks))  # Format ticks to 2 decimal places
            ax.grid()

        # Create a new axis for the legend at the bottom of the main figure
        legend_ax = fig.add_subplot(gs[-1, :])  # Use GridSpec to create the legend axis
        legend_ax.axis('off')

        alpha_labels = [h.get_label() for h in alpha_handles]
        plt.legend(alpha_handles, alpha_labels, loc='upper right', fontsize=14)

        # Adjust the layout of the main figure
        plt.tight_layout()

        # Save the current figure to the PDF
        pdf.savefig(fig)  # Save the figure
        plt.close(fig)  # Close the figure

    print(f'Saved plots to {pdf_filename}')

if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="Process input JSON and output PDF files.")

    # Define the expected arguments
    parser.add_argument('-i', '--input_json_filename', required=True, type=str, help='Path to the input JSON file.')
    parser.add_argument('-o', '--output_pdf_filename', required=True, type=str, help='Path for the output PDF file.')
    parser.add_argument('-b', '--bucketer', required=True, type=str, help='Bucketer type: values are "skew" or "opt".')
    parser.add_argument('-a', '--alpha', required=False, type=float, help='Value of alpha (a float).')

    # Parse the arguments
    args = parser.parse_args()

    # Call the main function with parsed arguments
    main(args.input_json_filename, args.output_pdf_filename, args.bucketer, args.alpha)
