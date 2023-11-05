import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from scipy.stats import gaussian_kde
import gc
from reproduction import *
# ray.init(num_cpus=4)


def main():
    # Load logistics data
    file_paths = [f'./data/cleaned/data_{i}/cleaned_logistics_detail_{i}.feather' for i in range(1, 8)]
    full_logistics_data = []
    for file_path in file_paths:
        full_logistics_data.append(pd.read_feather(file_path))
    full_logistics_data_df = pd.concat(full_logistics_data)

    # Load order data
    file_paths = [f'./data/cleaned/data_{i}/cleaned_order_data_{i}.feather' for i in range(1, 8)]
    full_order_data = []
    for file_path in file_paths:
        full_order_data.append(pd.read_feather(file_path))
    full_order_data_df = pd.concat(full_order_data)

    # Calculate distribution difference
    distribution_difference = compute_action_time_distribution_difference(full_logistics_data_df, full_order_data_df)

    # Plot
    plt.figure(figsize=(12, 8))
    actions = ["CONSIGN", "GOT", "DEPARTURE", "ARRIVAL", "SENT_SCAN"]

    for action in actions:
        subset = distribution_difference[distribution_difference['action'] == action]
        
        # Vary line thickness based on score (for simplicity, we'll just vary linewidth directly)
        for score in subset['logistics_review_score'].unique():
            score_subset = subset[subset['logistics_review_score'] == score]
            # linewidth = 3 if score == 5 else 1  # adjust as necessary
            sns.lineplot(x='action_time_interval', y='difference', data=score_subset, label=f'Score {score}')
        
        plt.title(f"Action: {action}")
        plt.legend()
        plt.show()

if __name__ == '__main__':
    main()
