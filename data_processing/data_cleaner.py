import inspect
import logging
import os.path

import pandas as pd

from constants import *


class DataCleaner:
    """
    Class to clean data and remove erroneous or unwanted entries.
    Data cleaning is performed according to "Operational Transparency: Showing When Work Gets Done"
    """

    def __init__(self, order_data_df: pd.DataFrame, logistics_detail_data_df: pd.DataFrame):
        """
        Initialize the cleaner
        :param order_data_df: Dataframe representing order data
        :param logistics_detail_data_df: Data frame representing logistics detail data
        """
        self.order_data_df = order_data_df
        self.logistics_data_df = logistics_detail_data_df

    def convert_timestamp_to_datetime(self):
        """
        Convert the timestamps in dataframes into datetime format
        """
        logging.info("started converting timestamp to datetime")
        if TIMESTAMP_DATE_TIME not in self.logistics_data_df.columns:
            logging.info("started converting logistics data timestamp to datetime")
            self.logistics_data_df[TIMESTAMP_DATE_TIME] = pd.to_datetime(
                self.logistics_data_df[TIMESTAMP], errors='coerce')
            null_timestamps = self.logistics_data_df[self.logistics_data_df[TIMESTAMP_DATE_TIME].isnull()]
            self.remove_order_ids(null_timestamps[ORDER_ID])
        if PAY_TIMESTAMP_DATETIME not in self.order_data_df.columns:
            logging.info("started converting order data payment timestamp to datetime")
            self.order_data_df[PAY_TIMESTAMP_DATETIME] = pd.to_datetime(
                self.order_data_df[PAY_TIMESTAMP], errors='coerce')
            null_pay_timestamps = self.order_data_df[self.order_data_df[PAY_TIMESTAMP_DATETIME].isnull()]
            self.remove_order_ids(null_pay_timestamps[ORDER_ID])
        logging.info("finished converting timestamp to datetime")

    def remove_order_ids(self, order_ids: pd.Series):
        """
        Convert the timestamps in dataframes into datetime format
        """
        logging.info("started removing order ids")
        original_order_count = self.order_data_df.shape[0]
        self.order_data_df = self.order_data_df[~self.order_data_df[ORDER_ID].isin(order_ids)]
        self.logistics_data_df = self.logistics_data_df[~self.logistics_data_df[ORDER_ID].isin(order_ids)]
        cleaned_order_count = self.order_data_df.shape[0]
        logging.info(f'finished removing order ids, removed {(1 - cleaned_order_count/original_order_count)*100}% of '
                     f'order for {inspect.stack()[1].function}')

    def drop_duplicates(self):
        """
        Drop duplicate rows from logistics data and order data
        """
        logging.info("started removing order ids")
        original_order_count = self.order_data_df.shape[0]
        original_logistics_count = self.logistics_data_df.shape[0]
        self.order_data_df = self.order_data_df.drop_duplicates()
        self.logistics_data_df = self.logistics_data_df.drop_duplicates()
        cleaned_order_count = self.order_data_df.shape[0]
        cleaned_logistics_count = self.logistics_data_df.shape[0]
        logging.info(
            f'finished dropping duplicates, removed {(1 - cleaned_order_count/original_order_count)*100}% of orders')
        logging.info(
            f'finished dropping duplicates, removed {(1 - cleaned_logistics_count/original_logistics_count)*100}% of '
            f'logistics data')

    def remove_trade_success_actions(self):
        """
        Removes all trade success actions from logistics detail.
        """
        logging.info("started removing trade success actions")
        original_logistics_count = self.logistics_data_df.shape[0]
        self.logistics_data_df = self.logistics_data_df[self.logistics_data_df[ACTION] != TRADE_SUCCESS]
        cleaned_logistics_count = self.logistics_data_df.shape[0]
        logging.info(
            f'finished dropping trade success actions, removed '
            f'{(1 - cleaned_logistics_count/original_logistics_count)*100}% of logistics data')

    def remove_failed_delivery(self):
        """
        Removes all shipments with a failure action.
        """
        logging.info("started removing failed delivery")
        failure_actions = self.logistics_data_df[self.logistics_data_df[ACTION] == FAILURE]
        self.remove_order_ids(failure_actions[ORDER_ID])
        logging.info("finished removing failed delivery")

    def remove_not_cainiao(self):
        """
        Remove all shipments with an origin warehouse not managed by Cainiao
        """
        logging.info("started removing shipments with an original warehouse not managed by Cainiao")
        not_cainiao_orders = self.order_data_df[self.order_data_df[IF_CAINIAO] != 1]
        self.remove_order_ids(not_cainiao_orders[ORDER_ID])
        logging.info("finished removing shipments with an original warehouse not managed by Cainiao")

    def remove_without_shipment_score(self):
        """
        Remove all shipments without a shipment score
        """
        logging.info("started removing shipments without a shipment score")
        without_shipment_score_orders = self.order_data_df[self.order_data_df[LOGISTICS_REVIEW_SCORE].isna()]
        self.remove_order_ids(without_shipment_score_orders[ORDER_ID])
        logging.info("finished removing shipments without a shipment score")

    def remove_without_shipment_times(self):
        """
        Remove all shipments without shipment times
        """
        logging.info("started removing shipments without shipment times")
        without_shipment_times_orders = self.logistics_data_df[
            self.logistics_data_df[TIMESTAMP].isnull() | self.logistics_data_df[ORDER_DATE].isnull()]
        self.remove_order_ids(without_shipment_times_orders[ORDER_ID])
        logging.info("finished removing shipments without shipment times")

    def remove_with_action_before_order(self):
        """
        Remove all shipments with actions reported before the order action
        """
        logging.info("started removing shipments with actions reported before the order action")
        self.convert_timestamp_to_datetime()
        joined_df = self.logistics_data_df.join(self.order_data_df, on=ORDER_ID, lsuffix='logistics_', rsuffix='order_')
        with_action_before_order = joined_df[joined_df[TIMESTAMP_DATE_TIME] < joined_df[PAY_TIMESTAMP_DATETIME]]
        self.remove_order_ids(with_action_before_order[f'logistics_{ORDER_ID}'])
        logging.info("finished removing shipments with actions reported before the order action")

    def remove_with_action_after_sign(self):
        """
        Remove all shipments with actions reported after the sign action
        """
        logging.info("started removing shipments with actions reported after the sign action")
        self.convert_timestamp_to_datetime()
        signed_actions = self.logistics_data_df[self.logistics_data_df[ACTION] == SIGNED]
        joined_df = self.logistics_data_df.join(signed_actions, on=ORDER_ID, lsuffix='_actions', rsuffix='_signed',
                                                how='left')
        with_action_after_sign_order = joined_df[
            joined_df[f'{TIMESTAMP_DATE_TIME}_actions'] > joined_df[f'{TIMESTAMP_DATE_TIME}_signed']]
        self.remove_order_ids(with_action_after_sign_order[f'{ORDER_ID}_actions'])
        logging.info("finished removing shipments with actions reported after the sign action")

    def remove_without_exactly_one_sign_action(self):
        """
        Remove all shipments without exactly one sign action
        """
        logging.info("started removing shipments without exactly one sign action")
        self.convert_timestamp_to_datetime()
        action_counts = self.logistics_data_df.groupby([ORDER_ID, ACTION]).size().unstack(fill_value=0)
        without_exactly_one_sign_action_order = action_counts[action_counts[SIGNED] != 1].index
        self.remove_order_ids(without_exactly_one_sign_action_order)
        logging.info("finished removing shipments without exactly one sign action")

    def remove_without_exactly_one_consign_action(self):
        """
        Remove all shipments without exactly one consign action
        """
        logging.info("started removing shipments without exactly one consign action")
        self.convert_timestamp_to_datetime()
        action_counts = self.logistics_data_df.groupby([ORDER_ID, ACTION]).size().unstack(fill_value=0)
        without_exactly_one_consign_action_order = action_counts[action_counts[CONSIGN] != 1].index
        self.remove_order_ids(without_exactly_one_consign_action_order)
        logging.info("finished removing shipments without exactly one consign action")

    def remove_without_slowest_shipping_speed(self):
        """
        Remove all shipments without the slowest shipping speed
        """
        logging.info("started removing shipments without slowest shipping speed")
        without_slowest_shipping_speed = self.order_data_df[
            self.order_data_df[PROMISE_SPEED].isnull() | self.order_data_df[PROMISE_SPEED] == 0]
        self.remove_order_ids(without_slowest_shipping_speed[ORDER_ID])
        logging.info("finished removing shipments without slowest shipping speed")

    def remove_with_multiple_shippers(self):
        """
        Remove all shipments with multiple shippers
        """
        logging.info("started removing shipments with multiple shippers")
        self.convert_timestamp_to_datetime()

        joined_df = self.logistics_data_df.join(self.order_data_df, on=ORDER_ID, lsuffix='_l', rsuffix='_r')
        with_multiple_shippers = joined_df[
            joined_df[LOGISTIC_COMPANY_ID] != joined_df[LOGISTIC_COMPANY_ID]]
        self.remove_order_ids(with_multiple_shippers[f'{ORDER_ID}_l'])
        logging.info("finished removing shipments with multiple shippers")

    def remove_with_multiple_product_types(self):
        """
        Remove all shipments with multiple product types
        """
        logging.info("started removing shipments with multiple product types")
        with_multiple_product_types = self.order_data_df[
            self.order_data_df[ITEM_DETAIL_INFO].str.split(",").apply(lambda x: len(x) > 1)]
        self.remove_order_ids(with_multiple_product_types[ORDER_ID])
        logging.info("finished removing shipments with multiple product types")

    def remove_shipment_time_more_than_eight_days(self):
        """
        Remove all shipments with shipment times in excess of eight days
        """
        logging.info("started removing shipments with shipment times in excess of eight days")
        self.convert_timestamp_to_datetime()
        signed_actions = self.logistics_data_df[self.logistics_data_df[ACTION] == SIGNED]
        consign_actions = self.logistics_data_df[self.logistics_data_df[ACTION] == CONSIGN]
        joined_df = signed_actions.join(consign_actions, on=ORDER_ID, lsuffix='_sign', rsuffix='_consign')
        joined_df["shipment_time"] = \
            joined_df[f'{TIMESTAMP_DATE_TIME}_sign'] - joined_df[f'{TIMESTAMP_DATE_TIME}_consign']

        shipment_more_than_eight_days = joined_df[joined_df["shipment_time"].dt.days > 8]
        self.remove_order_ids(shipment_more_than_eight_days[f'{ORDER_ID}_consign'])
        logging.info("finished removing shipments with shipment times in excess of eight days")

    def remove_more_than_ten_actions(self):
        """
        Remove all shipments with more than ten posted actions
        """
        logging.info("started removing shipments with more than ten posted actions")
        order_counts = self.logistics_data_df.groupby(ORDER_ID).size().reset_index(name='count')
        with_more_than_ten_actions = order_counts[order_counts['count'] > 10]
        self.remove_order_ids(with_more_than_ten_actions[ORDER_ID])
        logging.info("finished removing shipments with more than ten posted actions")

    def remove_less_than_four_actions(self):
        """
        Remove all shipments with fewer than four posted actions
        """
        logging.info("started removing shipments with less than four posted actions")
        order_counts = self.logistics_data_df.groupby(ORDER_ID).size().reset_index(name='count')
        with_less_than_four_actions = order_counts[order_counts['count'] < 4]
        self.remove_order_ids(with_less_than_four_actions[ORDER_ID])
        logging.info("finished removing shipments with less than four posted actions")

    def clean_up(self):
        """
        Run data cleaning according to "Operational Transparency: Showing When Work Gets Done"
        """
        logging.info('started data cleanup')
        logging.info(f'original logistics detail shape: {self.logistics_data_df.shape}')
        logging.info(f'original order data shape: {self.order_data_df.shape}')
        self.remove_not_cainiao()
        self.remove_without_shipment_score()
        self.remove_trade_success_actions()
        self.drop_duplicates()
        self.remove_failed_delivery()
        self.remove_without_shipment_times()
        self.remove_with_action_before_order()
        self.remove_without_exactly_one_sign_action()
        self.remove_without_exactly_one_consign_action()
        self.remove_with_action_after_sign()
        self.remove_without_slowest_shipping_speed()
        self.remove_with_multiple_shippers()
        self.remove_with_multiple_product_types()
        self.remove_shipment_time_more_than_eight_days()
        self.remove_more_than_ten_actions()
        self.remove_less_than_four_actions()
        logging.info("finished data cleanup")
        logging.info(self.logistics_data_df.head())
        logging.info(self.order_data_df.head())

    def export_data(self, root_dir: str, index: int):
        """
        Export dataframe as feather files
        Data will be exported as feather per the analysis performed at:
            https://towardsdatascience.com/the-best-format-to-save-pandas-data-414dca023e0d
        :param root_dir: The root directory to export the files in
        :param index: The index of the dataset, index should be from 1 and 7
        """
        assert 0 < index < 8
        assert os.path.isdir(root_dir)

        os.makedirs(os.path.join(root_dir, "cleaned", f'data_{index}'), exist_ok=True)
        logistic_data_file_dir = os.path.join(
            root_dir, "cleaned", f'data_{index}', f'cleaned_logistics_detail_{index}.feather')
        order_data_file_dir = os.path.join(
            root_dir, "cleaned", f'data_{index}', f'cleaned_order_data_{index}.feather')

        logging.info(f'cleaned logistics detail shape: {self.logistics_data_df.shape}')
        logging.info(f'cleaned order data shape: {self.order_data_df.shape}')

        logging.info(f'started exporting logistics data to {logistic_data_file_dir}')
        self.logistics_data_df.to_feather(logistic_data_file_dir)
        logging.info('finished exporting logistics data')

        logging.info(f'started exporting order data to {order_data_file_dir}')
        self.order_data_df.to_feather(order_data_file_dir)
        logging.info('finished exporting order data')
