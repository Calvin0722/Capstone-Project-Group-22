import pandas as pd
import numpy as np


def compute_action_time(logistics_data: pd.DataFrame, order_data: pd.DataFrame) -> pd.DataFrame:
    """
    Merge logistics and order data and compute the action time of each action
    :param logistics_data: dataframe containing logistics data
    :param order_data: dataframe containing order data
    :return: merged dataframe with action time column added
    """
    # Merge the two dataframes and trim columns
    sign_time = logistics_data[logistics_data["action"] == "SIGNED"][
        ["order_id", "action", "timestamp_datetime", "logistic_company_id", "facility_id"]]
    shipment_time = sign_time.merge(order_data, on='order_id', how='left')
    shipment_time["shipment_time"] = shipment_time["timestamp_datetime"] - shipment_time["pay_timestamp_datetime"]
    shipment_time = shipment_time.rename(
        columns={"pay_timestamp_datetime": "order_time", "timestamp_datetime": "sign_time"})
    shipment_time = shipment_time[
        ["order_id", "sign_time", "order_time", "logistics_review_score", "shipment_time", "item_det_info",
         "logistic_company_id", "facility_id", "merchant_id"]]

    # Compute action time
    action_time = logistics_data[["order_id", "action", "timestamp_datetime"]]
    action_time = action_time.merge(shipment_time, on='order_id', how='left')
    action_time["action_time"] = \
        (action_time["timestamp_datetime"] - action_time["order_time"]) / action_time["shipment_time"]

    # Filter out invalid action times
    action_time = action_time[action_time["action_time"] >= 0]
    action_time = action_time[action_time["action_time"] <= 1]

    return action_time


def bin_action_time(action_time_df: pd.DataFrame, bin_size: float = 0.1):
    # Create intervals for action_time
    bins = np.arange(0, action_time_df['action_time'].max() + bin_size, bin_size)
    action_time_df['action_time_interval'] = pd.cut(action_time_df['action_time'], bins=bins)
    action_time_df['action_time_interval'] = action_time_df['action_time_interval'].apply(lambda x: x.left)
    action_time_df = action_time_df[~action_time_df['action_time_interval'].isna()]

    return action_time_df


def compute_action_time_distribution_difference(
        logistics_data: pd.DataFrame, order_data: pd.DataFrame, bin_size: float = 0.1) -> pd.DataFrame:
    action_time_df = compute_action_time(logistics_data, order_data)
    bin_action_time(action_time_df, bin_size)

    action_time_df.loc[action_time_df['logistics_review_score'] <= 1, 'logistics_review_score'] = 2

    # Calculate conditional PDFs
    conditional_pdfs = (action_time_df.groupby(['logistics_review_score', 'action', 'action_time_interval'])
                        .size().rename('conditional_density'))
    conditional_pdfs = conditional_pdfs / conditional_pdfs.groupby(['action', 'logistics_review_score']).transform(
        'sum')
    conditional_pdfs = conditional_pdfs.reset_index()

    # Calculate unconditional PDFs
    unconditional_pdfs = action_time_df.groupby(['action_time_interval', 'action']).size().rename(
        'unconditional_density')
    unconditional_pdfs = unconditional_pdfs / unconditional_pdfs.groupby(['action']).transform('sum')
    unconditional_pdfs = unconditional_pdfs.reset_index()

    # Merge to have both conditional and unconditional densities in the same dataframe
    merged = conditional_pdfs.merge(unconditional_pdfs, on=['action', 'action_time_interval'], how='left')

    # Calculate the difference
    merged['difference'] = merged['conditional_density'] - merged['unconditional_density']
    return merged


def add_dummy_variables(action_time_df: pd.DataFrame, item_df: pd.DataFrame):
    # Compute dummy variable values
    facility_counts = action_time_df.groupby('order_id')['facility_id'].nunique().rename('facility_count').reset_index()
    arrive_counts = action_time_df[action_time_df["action"] == "ARRIVAL"].groupby('order_id').size().rename('arrive_count').reset_index()
    depart_counts = action_time_df[action_time_df["action"] == "DEPARTURE"].groupby('order_id').size().rename('depart_count').reset_index()
    receive_counts = action_time_df[action_time_df["action"] == "GOT"].groupby('order_id').size().rename('receive_count').reset_index()
    scan_counts = action_time_df[action_time_df["action"] == "SENT_SCAN"].groupby('order_id').size().rename('scan_count').reset_index()
    action_counts = action_time_df.groupby(['order_id', 'action_time_interval'], observed=True).size().rename('action_count').reset_index()
    action_time_df["days"] = (action_time_df["order_time"] - pd.Timestamp("2017-01-01")).dt.days
    action_time_df["week"] = action_time_df["days"] // 7
    action_time_df["day"] = action_time_df["day_count"] % 7

    action_count_df = action_time_df.groupby(['order_id'], observed=True).size().rename('total_action_count').reset_index()
    action_count_df = action_time_df.merge(action_count_df, on=["order_id"])

    # Merge dummy variables into the main dataframe
    action_count_df = action_count_df\
        .merge(facility_counts, on="order_id", how="left")\
        .merge(arrive_counts, on="order_id", how="left")\
        .merge(depart_counts, on="order_id", how="left")\
        .merge(receive_counts, on="order_id", how="left")\
        .merge(scan_counts, on="order_id", how="left")\
        .merge(action_counts, on=["order_id", "action_time_interval"], how="left")
    
    action_count_df = action_count_df.merge(item_df, on=["item_id", "merchant_id"], how="left")
    action_count_df.fillna(0, inplace=True)

    return action_count_df
