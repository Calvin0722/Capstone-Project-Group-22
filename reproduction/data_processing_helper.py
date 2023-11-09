import logging

import numpy as np

from constants import *

CONDITIONAL_DENSITY = 'conditional_density'
UNCONDITIONAL_DENSITY = 'unconditional_density'
DAYS = 'days'


def compute_action_time(logistics_data: pd.DataFrame, order_data: pd.DataFrame) -> pd.DataFrame:
    """
    Merge logistics and order data and compute the action time of each action
    :param logistics_data: dataframe containing logistics data
    :param order_data: dataframe containing order data
    :return: merged dataframe with action time column added
    """
    logging.info("Started computing action time")
    # Merge the two dataframes and trim columns
    sign_time = logistics_data[logistics_data[ACTION] == SIGNED][
        [ORDER_ID, ACTION, TIMESTAMP_DATE_TIME, LOGISTIC_COMPANY_ID, FACILITY_ID]]
    shipment_time = sign_time.merge(order_data, on=ORDER_ID, how=LEFT)
    shipment_time[SHIPMENT_TIME] = shipment_time[TIMESTAMP_DATE_TIME] - shipment_time[PAY_TIMESTAMP_DATETIME]
    shipment_time = shipment_time.rename(
        columns={PAY_TIMESTAMP_DATETIME: ORDER_TIME, TIMESTAMP_DATE_TIME: SIGN_TIME})
    shipment_time = shipment_time[
        [ORDER_ID, SIGN_TIME, ORDER_TIME, LOGISTICS_REVIEW_SCORE, SHIPMENT_TIME, ITEM_DETAIL_INFO, LOGISTIC_COMPANY_ID,
         FACILITY_ID, MERCHANT_ID]]

    # Compute action time
    action_time = logistics_data[[ORDER_ID, ACTION, TIMESTAMP_DATE_TIME]]
    action_time = action_time.merge(shipment_time, on=ORDER_ID, how=LEFT)
    action_time[ACTION_TIME] = (action_time[TIMESTAMP_DATE_TIME] - action_time[ORDER_TIME]) / action_time[SHIPMENT_TIME]

    # Filter out invalid action times
    action_time = action_time[action_time[ACTION_TIME] >= 0]
    action_time = action_time[action_time[ACTION_TIME] <= 1]

    logging.info("Finished computing action time")
    return action_time


def bin_action_time(action_time_df: pd.DataFrame, bin_size: float = 0.1):
    """
    Divide rows into bins based on action time
    :param action_time_df: data frame with action time
    :param bin_size: size of each bin
    :return: dataframe with action time bin added
    """
    logging.info("Started binning action time")
    # Create intervals for action_time
    bins = np.arange(0, action_time_df[ACTION_TIME].max() + bin_size, bin_size)
    action_time_df[ACTION_TIME_INTERVAL] = pd.cut(action_time_df[ACTION_TIME], bins=bins)
    action_time_df[ACTION_TIME_INTERVAL] = action_time_df[ACTION_TIME_INTERVAL].apply(lambda x: x.left)
    action_time_df = action_time_df[~action_time_df[ACTION_TIME_INTERVAL].isna()]
    logging.info("Finished binning action time")

    return action_time_df


def compute_action_time_distribution_difference(
        logistics_data: pd.DataFrame, order_data: pd.DataFrame, bin_size: float = 0.1) -> pd.DataFrame:
    """
    Compute the difference in the expected number of actions in each action time interval when conditioned and
    unconditioned on logistics review score
    :param logistics_data: dataframe containing logistics data
    :param order_data: dataframe containing order data
    :param bin_size: size of each action time bin
    :return: dataframe with the difference in distribution added
    """
    logging.info("Started compute action time distribution difference")
    action_time_df = compute_action_time(logistics_data, order_data)
    bin_action_time(action_time_df, bin_size)

    action_time_df.loc[action_time_df[LOGISTICS_REVIEW_SCORE] <= 1, LOGISTICS_REVIEW_SCORE] = 2

    # Calculate conditional PDFs
    conditional_pdfs = (action_time_df.groupby([LOGISTICS_REVIEW_SCORE, ACTION, ACTION_TIME_INTERVAL], observed=True)
                        .size().rename(CONDITIONAL_DENSITY))
    conditional_pdfs = conditional_pdfs / conditional_pdfs.groupby([ACTION, LOGISTICS_REVIEW_SCORE],
                                                                   observed=True).transform(SUM)
    conditional_pdfs = conditional_pdfs.reset_index()

    # Calculate unconditional PDFs
    unconditional_pdfs = action_time_df.groupby([ACTION_TIME_INTERVAL, ACTION], observed=True).size().rename(
        UNCONDITIONAL_DENSITY)
    unconditional_pdfs = unconditional_pdfs / unconditional_pdfs.groupby([ACTION]).transform(SUM)
    unconditional_pdfs = unconditional_pdfs.reset_index()

    # Merge to have both conditional and unconditional densities in the same dataframe
    merged = conditional_pdfs.merge(unconditional_pdfs, on=[ACTION, ACTION_TIME_INTERVAL], how=LEFT)

    # Calculate the difference
    merged[DISTRIBUTION_DIFFERENCE] = merged[CONDITIONAL_DENSITY] - merged[UNCONDITIONAL_DENSITY]
    merged.drop([CONDITIONAL_DENSITY, UNCONDITIONAL_DENSITY], axis=1)
    logging.info("Finished compute action time distribution difference")
    return merged


def add_dummy_variables(df: pd.DataFrame, item_df: pd.DataFrame):
    """
    Add dummy variables to the dataframe, dummy variables include facility counts, arrive counts, depart counts,
    receive counts, scan counts, action counts, week_counts, and day of the week
    :param df: dataframe with action time intervals
    :param item_df: dataframe containing item info
    :return: dataframe with dummy variables added
    """
    logging.info("Started adding dummy variables")
    # Compute dummy variable values
    facility_counts = df.groupby(ORDER_ID)[FACILITY_ID].nunique().rename(FACILITY_COUNT).reset_index()
    arrive_counts = df[df[ACTION] == ARRIVAL].groupby(ORDER_ID).size().rename(ARRIVE_COUNT).reset_index()
    depart_counts = df[df[ACTION] == DEPARTURE].groupby(ORDER_ID).size().rename(DEPART_COUNT).reset_index()
    receive_counts = df[df[ACTION] == GOT].groupby(ORDER_ID).size().rename(RECEIVE_COUNT).reset_index()
    scan_counts = df[df[ACTION] == SENT_SCAN].groupby(ORDER_ID).size().rename(SCAN_COUNT).reset_index()
    action_counts = df.groupby([ORDER_ID, ACTION_TIME_INTERVAL], observed=True).size().rename(ACTION_COUNT)\
        .reset_index()
    df[DAYS] = (df[ORDER_TIME] - FIRST_DAY).dt.days
    df[WEEK_COUNT] = df[DAYS] // 7
    df[DAY_OF_WEEK] = df[DAYS] % 7

    action_count_df = df.groupby([ORDER_ID], observed=True).size().rename(
        SHIPMENT_ACTION_COUNT).reset_index()
    action_count_df = df.merge(action_count_df, on=[ORDER_ID])

    logging.info("Started merging dummy variables.")
    # Merge dummy variables into the main dataframe
    action_count_df = action_count_df \
        .merge(facility_counts, on=ORDER_ID, how=LEFT) \
        .merge(arrive_counts, on=ORDER_ID, how=LEFT) \
        .merge(depart_counts, on=ORDER_ID, how=LEFT) \
        .merge(receive_counts, on=ORDER_ID, how=LEFT) \
        .merge(scan_counts, on=ORDER_ID, how=LEFT) \
        .merge(action_counts, on=[ORDER_ID, ACTION_TIME_INTERVAL], how=LEFT)

    action_count_df = action_count_df.merge(item_df, on=[ITEM_ID, MERCHANT_ID], how=LEFT)
    action_count_df.fillna(0, inplace=True)
    logging.info("Finished adding dummy variables")

    return action_count_df
