import pandas as pd
import numpy as np


def compute_action_time(logistics_data: pd.DataFrame, order_data: pd.DataFrame) -> pd.DataFrame:
    order_time = order_data[["order_id", "pay_timestamp_datetime", "logistics_review_score"]]
    sign_time = logistics_data[logistics_data["action"] == "SIGNED"][["order_id", "action", "timestamp_datetime"]]
    shipment_time = sign_time.merge(order_time, on='order_id', how='left')
    shipment_time["shipment_time"] = shipment_time["timestamp_datetime"] - shipment_time["pay_timestamp_datetime"]
    shipment_time = shipment_time.rename(
        columns={"pay_timestamp_datetime": "order_time", "timestamp_datetime": "sign_time"})
    shipment_time = shipment_time[["order_id", "sign_time", "order_time", "logistics_review_score", "shipment_time"]]

    action_time = logistics_data[["order_id", "action", "timestamp_datetime"]]
    action_time = action_time.merge(shipment_time, on='order_id', how='left')
    action_time["action_time"] = \
        (action_time["timestamp_datetime"] - action_time["order_time"]) / action_time["shipment_time"]
    action_time = action_time[action_time["action_time"] >= 0]
    action_time = action_time[action_time["action_time"] <= 1]
    return action_time


def compute_action_time_distribution_difference(logistics_data: pd.DataFrame, order_data: pd.DataFrame, bin_size: float = 0.05) -> pd.DataFrame:
    action_time_df = compute_action_time(logistics_data, order_data)

    # Create intervals for action_time
    bins = np.arange(0, action_time_df['action_time'].max() + bin_size, bin_size)
    action_time_df['binned_action_time'] = pd.cut(action_time_df['action_time'], bins=bins)
    action_time_df['action_time_interval'] = action_time_df['binned_action_time'].apply(lambda x: x.left)
    action_time_df = action_time_df[~action_time_df['action_time_interval'].isna()]
    action_time_df.loc[action_time_df['logistics_review_score'] <= 1, 'logistics_review_score'] = 2

    # Calculate conditional PDFs
    conditional_pdfs = action_time_df.groupby(['logistics_review_score', 'action', 'action_time_interval', 'order_id']).size()
    conditional_pdfs = conditional_pdfs.groupby(
        ['logistics_review_score', 'action_time_interval', 'action']).mean().rename('conditional_density')
    conditional_pdfs = conditional_pdfs / conditional_pdfs.groupby(['action', 'logistics_review_score']).transform(
        'sum')
    conditional_pdfs = conditional_pdfs.reset_index()

    # Calculate unconditional PDFs
    unconditional_pdfs = action_time_df.groupby(['action_time_interval', 'action', 'order_id']).size()
    unconditional_pdfs = unconditional_pdfs.groupby(['action_time_interval', 'action']).mean().rename(
        'unconditional_density')
    unconditional_pdfs = unconditional_pdfs / unconditional_pdfs.groupby('action').transform('sum')
    unconditional_pdfs = unconditional_pdfs.reset_index()

    # Merge to have both conditional and unconditional densities in the same dataframe
    merged = conditional_pdfs.merge(unconditional_pdfs, on=['action', 'action_time_interval'], how='left')

    # Calculate the difference
    merged['difference'] = merged['conditional_density'] - merged['unconditional_density']
    return merged
