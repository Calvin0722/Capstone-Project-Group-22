import matplotlib.pyplot as plt
import seaborn as sns
from reproduction import *
from data_processing import *


def main():
    full_logistics_data_df = load_full_logistics_data()
    full_order_data_df = load_full_order_data()

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
            sns.lineplot(x='action_time_interval', y='difference', data=score_subset, label=f'Score {score}')

        plt.title(f"Action: {action}")
        plt.legend()
        plt.show()


if __name__ == '__main__':
    main()
