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

def main(json_file, pdf_filename, alpha, bucketer):
    # Load the data from the provided JSON file
    with open(json_file, 'r') as f:
        records = [json.loads(line) for line in f]

    # Transform the data into a DataFrame for easier manipulation
    df = pd.DataFrame(records)

    # Convert relevant columns to numeric types
    df['lambda'] = pd.to_numeric(df['lambda'])
    df['bits_per_key'] = pd.to_numeric(df['bits_per_key'])

    df['alpha'] = df.apply(
        lambda row: pd.to_numeric(row['alpha']) if row['dense_partitioning'] == "false" else alpha,
        axis=1
    )

    min_y = min(df['bits_per_key'])
    max_y = max(df['bits_per_key'])
    if max_y > 5.0:
        max_y = 5.0 # saturate to 5 bits/key (that's enough!)

    # Define configurations for filtering
    configurations = [

        # ((df['avg_partition_size'] == "0") & (df['num_partitions'] == "0") & (df['dense_partitioning'] == "false") & (df['bucketer_type'] == bucketer)
        #         , "SINGLE"),

        ((df['avg_partition_size'] != "0") & (df['num_partitions'] != "0") & (df['dense_partitioning'] == "false")  & (df['bucketer_type'] == bucketer)
                , "PARTITIONED"),

        ((df['avg_partition_size'] != "0") & (df['num_partitions'] != "0") & (df['dense_partitioning'] == "true")  & (df['bucketer_type'] == bucketer)
                , "DENSE-PARTITIONED")
    ]

    # Collect all Y values to determine the limits later
    all_y_values = []

    # Create list to hold grouped data
    grouped_data = []

    for condition, title in configurations:
        # Filter the DataFrame based on the current configuration
        filtered_df = df[condition]

        # Group by the specified fields
        grouped_avg = filtered_df.groupby([
            'n', 'lambda', 'alpha', 'minimal',
            'bucketer_type', 'avg_partition_size',
            'num_partitions', 'dense_partitioning',
            'seed', 'num_threads',
            'external_memory', 'encoder_type'
        ])['bits_per_key'].mean().reset_index()

        # Store the grouped data for later use to calculate y limits
        grouped_data.append((grouped_avg, title))

    # Create a new PDF file to save plots
    with PdfPages(pdf_filename) as pdf:
        fig = plt.figure(figsize=(12, 6))
        gs = gridspec.GridSpec(1, 3, width_ratios=[3, 3, 0.7])
        axs = [fig.add_subplot(gs[i]) for i in range(2)]

        encoder_handles = []
        encoder_color_map = {}
        i = 0
        for (grouped_avg, _) in grouped_data:
            for e in sorted(grouped_avg['encoder_type'].unique()):
                if e not in encoder_color_map.keys():
                    encoder_color_map[e] = i
                    i += 1

        colors = plt.get_cmap('tab20', len(encoder_color_map))  # Use 'tab20'

        for ax, (grouped_avg, title) in zip(axs, grouped_data):

            encoder_types = sorted(grouped_avg['encoder_type'].unique())

            for encoder_type in encoder_types:

                encoder_color = colors(encoder_color_map[encoder_type])
                for alpha_value in sorted(grouped_avg['alpha'].unique(), reverse=True):

                    if alpha_value == alpha: # filter on specific alpha

                        subset = grouped_avg[(grouped_avg['alpha'] == alpha_value) & (grouped_avg['encoder_type'] == encoder_type)]

                        ax.plot(subset['lambda'], subset['bits_per_key'],
                                marker='o', # marker_symbols[i],
                                markersize=6, color=encoder_color)

                handle = ax.plot([], [], marker='o', # marker_symbols[i],
                                 label=encoder_type,
                                 color=encoder_color,
                                 linestyle='none')[0]

                if not encoder_type in [l.get_label() for l in encoder_handles]:
                    encoder_handles.append(handle)

            # Set plot labels and title with LaTeX formatting
            ax.set_xlabel(r'$\lambda$', fontsize=14)
            ax.set_ylabel('Bits/key', fontsize=14)
            ax.set_title(title, fontsize=16)
            ax.set_ylim(min_y, max_y)

            ticks = np.linspace(min_y, max_y, 20)
            ax.set_yticks(ticks)
            ax.yaxis.set_major_formatter(FuncFormatter(format_ticks))
            ax.grid(True, linestyle='--', alpha=0.7)
            ax.tick_params(axis='both', which='major', labelsize=12)

        # Create three columns in the legend
        encoder_labels = [h.get_label() for h in encoder_handles]

        # Create a new axis for the legend at the bottom of the main figure
        legend_ax = fig.add_subplot(gs[-1, :])  # Use GridSpec to create the legend axis
        legend_ax.axis('off')

        # Adjusting the `bbox_to_anchor` to move legends further to the right
        plt.legend(encoder_handles, encoder_labels, loc='upper right', fontsize=14)

        # Adjust the layout of the main figure
        plt.tight_layout()

        # Save the current figure to the PDF
        pdf.savefig(fig)
        plt.close(fig)

    print(f'Saved plots to {pdf_filename}')

if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="Process input JSON and output PDF files.")

    # Define the expected arguments
    parser.add_argument('-i', '--input_json_filename', required=True, type=str, help='Path to the input JSON file.')
    parser.add_argument('-o', '--output_pdf_filename', required=True, type=str, help='Path for the output PDF file.')
    parser.add_argument('-a', '--alpha', required=True, type=float, help='Value of alpha (a float).')
    parser.add_argument('-b', '--bucketer', required=True, type=str, help='Bucketer type: values are "skew" or "opt".')

    # Parse the arguments
    args = parser.parse_args()

    # Call the main function with parsed arguments
    main(args.input_json_filename, args.output_pdf_filename, args.alpha, args.bucketer)
