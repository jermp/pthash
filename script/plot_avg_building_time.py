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
        records = [json.loads(line) for line in f]  # Read each line as a separate JSON object

    # Transform the data into a DataFrame for easier manipulation
    df = pd.DataFrame(records)

    # Convert relevant columns to numeric types
    df['total_microseconds'] = pd.to_numeric(df['total_microseconds'])
    df['lambda'] = pd.to_numeric(df['lambda'])

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

        # Group by the specified fields and calculate the mean 'total_microseconds'
        grouped_avg = filtered_df.groupby([
            'n', 'lambda', 'alpha', 'minimal',
            'search_type',
            'bucketer_type', 'avg_partition_size',
            'num_partitions', 'dense_partitioning',
            'seed', 'num_threads', 'external_memory'
        ])['total_microseconds'].mean().reset_index()

        # Convert total_microseconds to seconds
        grouped_avg['total_seconds'] = grouped_avg['total_microseconds'] / 1_000_000  # Convert microseconds to seconds

        # Store the grouped data for later use to calculate y limits
        grouped_data.append((grouped_avg, title))
        all_y_values.extend(grouped_avg['total_seconds'].tolist())  # Gather all y values

    # Determine global min and max for Y-axis
    min_y = min(all_y_values)
    max_y = max(all_y_values)
    colors = plt.get_cmap('tab20', 12)  # Use 'tab20'
    alpha_handles = []

    # Create a new PDF file to save plots
    with PdfPages(pdf_filename) as pdf:
        # Create a single row of subplots
        # fig, axs = plt.subplots(1, 3, figsize=(18, 6))

        fig = plt.figure(figsize=(20, 8))
        gs = gridspec.GridSpec(1, 4, width_ratios=[3, 3, 3, 1])
        axs = [fig.add_subplot(gs[i]) for i in range(3)]

        for ax, (grouped_avg, title) in zip(axs, grouped_data):
            # Scatter plot for each unique alpha value
            for i, alpha_value in enumerate(sorted(grouped_avg['alpha'].unique(), reverse=True)):
                subset = grouped_avg[grouped_avg['alpha'] == alpha_value]
                # ax.plot(subset['lambda'], subset['total_seconds'], label=rf'$\alpha$ = {float(alpha_value):.2f}', marker='o')

                label = rf'$\alpha$ = {float(alpha_value):.2f}'
                if not any(label in l.get_label() for l in alpha_handles):
                    alpha_handles.append(ax.plot([], [], label=label, color=colors(i))[0])

                ax.plot(subset['lambda'], subset['total_seconds'],
                        marker='o', # marker_symbols[i],
                        markersize=6, color=colors(i))

            # Set plot labels and title with LaTeX formatting
            ax.set_xlabel(r'$\lambda$', fontsize=14)
            ax.set_ylabel(r'Building time (seconds)', fontsize=14)
            ax.set_title(title, fontsize=16)  # Title for the current configuration
            ax.set_ylim(min_y, max_y)  # Set Y-axis limits

            # Set the Y-axis ticks to 10 evenly spaced ticks, including min and max
            ticks = np.linspace(min_y, max_y, 10)  # Create 10 evenly spaced ticks
            ax.set_yticks(ticks)  # Set updated ticks
            # Apply custom formatter to the Y-axis
            ax.yaxis.set_major_formatter(FuncFormatter(format_ticks))  # Format ticks to 2 decimal places
            ax.grid()

        # Create a new axis for the legend at the bottom of the main figure
        legend_ax = fig.add_subplot(gs[-1, :])  # Use GridSpec to create the legend axis
        legend_ax.axis('off')

        alpha_labels = [h.get_label() for h in alpha_handles]

        # Adjusting the `bbox_to_anchor` to move legends further to the right
        alpha_legend = legend_ax.legend(alpha_handles, alpha_labels, loc='upper right'
            # title='SINGLE and PARTITIONED'
            )  # Move further right
        legend_ax.add_artist(alpha_legend)

        # Adjust the layout of the main figure
        plt.tight_layout()

        # Save the current figure to the PDF
        pdf.savefig(fig)  # Save the figure
        plt.close(fig)  # Close the figure

    print(f'Saved plots to {pdf_filename}')

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <json_file> <output_pdf_file>")
    else:
        json_file_path = sys.argv[1]
        output_pdf_filename = sys.argv[2]
        main(json_file_path, output_pdf_filename)
