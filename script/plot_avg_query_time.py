import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import FuncFormatter
import matplotlib.gridspec as gridspec
import sys

def format_ticks(x, pos):
    return f'{x:.2f}'

def main(json_file, pdf_filename):
    # Load the data from the provided JSON file
    with open(json_file, 'r') as f:
        records = [json.loads(line) for line in f]

    # Transform the data into a DataFrame for easier manipulation
    df = pd.DataFrame(records)

    # Convert relevant columns to numeric types
    df['lambda'] = pd.to_numeric(df['lambda'])
    df['nanosec_per_key'] = pd.to_numeric(df['nanosec_per_key'])

    # Define configurations for filtering
    configurations = [
        ((df['avg_partition_size'] == "0") & (df['num_partitions'] == "0") & (df['dense_partitioning'] == "false"),
         "SINGLE"),
        ((df['avg_partition_size'] != "0") & (df['num_partitions'] != "0") & (df['dense_partitioning'] == "false"),
         "PARTITIONED"),
        ((df['avg_partition_size'] != "0") & (df['num_partitions'] != "0") & (df['dense_partitioning'] == "true"),
         "DENSE-PARTITIONED"),
    ]

    # Collect all Y values to determine the limits later
    all_y_values = []

    # Create list to hold grouped data
    grouped_data = []

    for condition, title in configurations:
        # Filter the DataFrame based on the current configuration
        filtered_df = df[condition]

        # Group by the specified fields and calculate the mean 'nanosec_per_key'
        grouped_avg = filtered_df.groupby([
            'n', 'lambda', 'alpha', 'minimal',
            'search_type', 'bucketer_type', 'avg_partition_size',
            'num_partitions', 'dense_partitioning', 'seed', 'num_threads',
            'external_memory', 'encoder_type'
        ])['nanosec_per_key'].mean().reset_index()

        # Store the grouped data for later use to calculate y limits
        grouped_data.append((grouped_avg, title))
        all_y_values.extend(grouped_avg['nanosec_per_key'].tolist())  # Gather all y values

    # Determine global min and max for Y-axis
    min_y = min(all_y_values)
    max_y = max(all_y_values)

    # Define different marker symbols for each encoder_type
    marker_symbols = ['o', 'v', '^', '<', '>', '8', 's', 'p', '*', 'h', 'H', 'D', 'd', 'P', 'X']

    prop_cycle = plt.rcParams['axes.prop_cycle']
    colors = prop_cycle.by_key()['color']

    # Create a new PDF file to save plots
    with PdfPages(pdf_filename) as pdf:
        # Create three rows of subplots, one plot per row
        # fig, axs = plt.subplots(3, 1, figsize=(12, 30))  # Increased figure height for legend

        fig = plt.figure(figsize=(12, 30))
        gs = gridspec.GridSpec(4, 1, height_ratios=[3, 3, 3, 1])  # 3 rows for plots, 1 row for legend
        axs = [fig.add_subplot(gs[i]) for i in range(3)]

        alpha_handles = []
        inter_mono_handles = []
        other_encoder_handles = []

        for ax, (grouped_avg, title) in zip(axs, grouped_data):
            encoder_types = sorted(grouped_avg['encoder_type'].unique())  # Get unique encoder_types

            for alpha_index, alpha_value in enumerate(sorted(grouped_avg['alpha'].unique(), reverse=True)):
                label = rf'$\alpha$ = {float(alpha_value):.2f}'
                if not any(label in l.get_label() for l in alpha_handles):
                    alpha_handles.append(ax.plot([], [], label=label, color=colors[alpha_index % len(colors)])[0])

            for i, encoder_type in enumerate(encoder_types):
                for alpha_index, alpha_value in enumerate(sorted(grouped_avg['alpha'].unique(), reverse=True)):
                    subset = grouped_avg[(grouped_avg['alpha'] == alpha_value) & (grouped_avg['encoder_type'] == encoder_type)]

                    ax.plot(subset['lambda'], subset['nanosec_per_key'],
                            marker=marker_symbols[i % len(marker_symbols)],
                            color=colors[alpha_index % len(colors)])


                handle = ax.plot([], [], marker=marker_symbols[i % len(marker_symbols)],
                                 label=encoder_type, color='black', linestyle='none')[0]

                if 'inter' in encoder_type or 'mono' in encoder_type:
                    inter_mono_handles.append(handle)
                else:
                    if not any(encoder_type in l.get_label() for l in other_encoder_handles):
                        other_encoder_handles.append(handle)

            # Set plot labels and title with LaTeX formatting
            ax.set_xlabel(r'$\lambda$', fontsize=14)
            ax.set_ylabel(f'Query time (avg. ns per query)', fontsize=14)
            ax.set_title(title, fontsize=16)
            ax.set_ylim(min_y, max_y)

            ticks = np.linspace(min_y, max_y, 10)
            ax.set_yticks(ticks)
            ax.yaxis.set_major_formatter(FuncFormatter(format_ticks))
            ax.grid(True, linestyle='--', alpha=0.7)
            ax.tick_params(axis='both', which='major', labelsize=12)

        # Create three columns in the legend
        alpha_labels = [h.get_label() for h in alpha_handles]
        other_encoder_labels = [h.get_label() for h in other_encoder_handles]
        inter_mono_labels = [h.get_label() for h in inter_mono_handles]

        # Create a new axis for the legend at the bottom of the main figure
        legend_ax = fig.add_subplot(gs[-1, :])  # Use GridSpec to create the legend axis
        legend_ax.axis('off')

        # Create three separate legends
        alpha_legend = legend_ax.legend(alpha_handles, alpha_labels, loc='center left', bbox_to_anchor=(0, 0.5))
        other_legend = legend_ax.legend(other_encoder_handles, other_encoder_labels, loc='center', bbox_to_anchor=(0.25, 0.5), title='SINGLE and PARTITIONED encoders')
        inter_mono_legend = legend_ax.legend(inter_mono_handles, inter_mono_labels, loc='center right', bbox_to_anchor=(0.605, 0.5), title='DENSE-PARTITIONED encoders')

        # Add the legends to the axis
        legend_ax.add_artist(alpha_legend)
        legend_ax.add_artist(other_legend)
        legend_ax.add_artist(inter_mono_legend)

        # Adjust the layout of the main figure
        plt.tight_layout()

        # Save the current figure to the PDF
        pdf.savefig(fig)
        plt.close(fig)

    print(f'Saved plots to {pdf_filename}')

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <json_file> <output_pdf_file>")
    else:
        json_file_path = sys.argv[1]
        output_pdf_filename = sys.argv[2]
        main(json_file_path, output_pdf_filename)
