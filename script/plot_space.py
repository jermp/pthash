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

def main(json_file, pdf_filename, alpha):
    # Load the data from the provided JSON file
    with open(json_file, 'r') as f:
        records = [json.loads(line) for line in f]

    # Transform the data into a DataFrame for easier manipulation
    df = pd.DataFrame(records)

    # Convert relevant columns to numeric types
    df['lambda'] = pd.to_numeric(df['lambda'])
    df['bits_per_key'] = pd.to_numeric(df['bits_per_key'])
    df['alpha'] = pd.to_numeric(df['alpha'])

    # Define configurations for filtering
    configurations = [
        ((df['avg_partition_size'] == "0") & (df['num_partitions'] == "0") & (df['dense_partitioning'] == "false")
                & (df['alpha'] == alpha),
         "SINGLE"),
        ((df['avg_partition_size'] != "0") & (df['num_partitions'] != "0") & (df['dense_partitioning'] == "false")
                & (df['alpha'] == alpha),
         "PARTITIONED"),
        ((df['avg_partition_size'] != "0") & (df['num_partitions'] != "0") & (df['dense_partitioning'] == "true")
                & (df['alpha'] == alpha),
         "DENSE-PARTITIONED"),
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
            'search_type', 'bucketer_type', 'avg_partition_size',
            'num_partitions', 'dense_partitioning', 'seed', 'num_threads',
            'external_memory', 'encoder_type'
        ])['bits_per_key'].mean().reset_index()

        # Store the grouped data for later use to calculate y limits
        grouped_data.append((grouped_avg, title))
        all_y_values.extend(grouped_avg['bits_per_key'].tolist())  # Gather all y values

    # Determine global min and max for Y-axis
    min_y = min(all_y_values)
    max_y = max(all_y_values)
    if max_y > 5.0:
        max_y = 5.0 # saturate to 5 bits/key (that's enough!)

    # Define different marker symbols for each encoder_type
    # marker_symbols = ['o', 'v', '^', '<', '>', '8', 's', 'p', '*', 'h', 'H', 'D', 'd', 'P', 'X']
    colors = plt.get_cmap('tab20', 12)  # Use 'tab20'

    # Create a new PDF file to save plots
    with PdfPages(pdf_filename) as pdf:
        fig = plt.figure(figsize=(20, 8))
        gs = gridspec.GridSpec(1, 4, width_ratios=[3, 3, 3, 1])
        axs = [fig.add_subplot(gs[i]) for i in range(3)]

        inter_mono_handles = []
        other_encoder_handles = []

        for ax, (grouped_avg, title) in zip(axs, grouped_data):

            encoder_types = sorted(grouped_avg['encoder_type'].unique())

            for i, encoder_type in enumerate(encoder_types):

                encoder_color = colors(i)
                for alpha_value in sorted(grouped_avg['alpha'].unique(), reverse=True):
                    subset = grouped_avg[(grouped_avg['alpha'] == alpha_value) & (grouped_avg['encoder_type'] == encoder_type)]

                    ax.plot(subset['lambda'], subset['bits_per_key'],
                            marker='o', # marker_symbols[i],
                            markersize=6, color=encoder_color)

                handle = ax.plot([], [], marker='o', # marker_symbols[i],
                                 label=encoder_type,
                                 color=encoder_color,
                                 linestyle='none')[0]

                if 'inter' in encoder_type or 'mono' in encoder_type:
                    inter_mono_handles.append(handle)
                else:
                    if not any(encoder_type in l.get_label() for l in other_encoder_handles):
                        other_encoder_handles.append(handle)

            # Set plot labels and title with LaTeX formatting
            ax.set_xlabel(r'$\lambda$', fontsize=14)
            ax.set_ylabel('Bits/key', fontsize=14)
            ax.set_title(title, fontsize=16)
            ax.set_ylim(min_y, max_y)

            ticks = np.linspace(min_y, max_y, 10)
            ax.set_yticks(ticks)
            ax.yaxis.set_major_formatter(FuncFormatter(format_ticks))
            ax.grid(True, linestyle='--', alpha=0.7)
            ax.tick_params(axis='both', which='major', labelsize=12)

        # Create three columns in the legend
        other_encoder_labels = [h.get_label() for h in other_encoder_handles]
        inter_mono_labels = [h.get_label() for h in inter_mono_handles]

        # Create a new axis for the legend at the bottom of the main figure
        legend_ax = fig.add_subplot(gs[-1, :])  # Use GridSpec to create the legend axis
        legend_ax.axis('off')

        # Adjusting the `bbox_to_anchor` to move legends further to the right
        other_legend = legend_ax.legend(other_encoder_handles, other_encoder_labels, loc='upper right',
            title='SINGLE and PARTITIONED')  # Move further right
        inter_mono_legend = legend_ax.legend(inter_mono_handles, inter_mono_labels, loc='center right',
            bbox_to_anchor=(1, 0.45),
            title='DENSE-PARTITIONED')  # Adjusted position

        # Add the legends to the axis
        legend_ax.add_artist(other_legend)
        legend_ax.add_artist(inter_mono_legend)

        # Adjust the layout of the main figure
        plt.tight_layout()

        # Save the current figure to the PDF
        pdf.savefig(fig)
        plt.close(fig)

    print(f'Saved plots to {pdf_filename}')

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script.py <json_file> <output_pdf_file> <alpha>")
    else:
        json_file_path = sys.argv[1]
        output_pdf_filename = sys.argv[2]
        alpha = float(sys.argv[3])
        main(json_file_path, output_pdf_filename, alpha)
