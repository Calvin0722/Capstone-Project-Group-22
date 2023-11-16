import matplotlib.pyplot as plt
import seaborn as sns
from reproduction import *
from data_processing import *
from constants import *


def main():
    full_logistics_data_df = load_full_logistics_data()
    full_order_data_df = load_full_order_data()

    # Calculate distribution difference
    distribution_difference = compute_action_time_distribution_difference(full_logistics_data_df, full_order_data_df)

    # Plot
    plt.figure(figsize=(12, 8))
    actions = [CONSIGN, GOT, DEPARTURE, ARRIVAL, SENT_SCAN]

    # Define the number of rows/cols for the subplot grid
    cols = 1
    rows = len(actions)

    # Create subplots
    fig, axs = plt.subplots(rows, cols, figsize=(15, 3 * rows)) 
    axs = axs.flatten()  # Flatten the axis array for easy iteration if it's 2D

    for idx, action in enumerate(actions):
        subset = distribution_difference[distribution_difference[ACTION] == action]
        
        for score in subset[LOGISTICS_REVIEW_SCORE].unique():
            score_subset = subset[subset[LOGISTICS_REVIEW_SCORE] == score]
            # Plot on the subplot axes
            sns.lineplot(x=ACTION_TIME_INTERVAL, y=DISTRIBUTION_DIFFERENCE, data=score_subset, label=f'Score {score}', ax=axs[idx])
        
        axs[idx].set_title(f"Action: {action}")
        axs[idx].legend()

    # Adjust layout
    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    main()
