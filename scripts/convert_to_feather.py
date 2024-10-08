# Script to clean up data
import argparse
import gc
import logging
import os.path

import pandas as pd

from data_processing import DataCleaner
from constants import *

LOGISTICS_COLUMN_NAMES = [ORDER_ID, ORDER_DATE, LOGISTICS_ORDER_ID, ACTION, FACILITY_ID, FACILITY_TYPE, CITY_ID,
                          LOGISTIC_COMPANY_ID, TIMESTAMP]
LOGISTICS_COLS = [0, 1, 2, 3, 4, 5, 6, 7, 8]
ORDER_COLUMN_NAMES = [DAY, ORDER_ID, ITEM_DETAIL_INFO, PAY_TIMESTAMP, BUYER_ID, PROMISE_SPEED, IF_CAINIAO, MERCHANT_ID,
                      LOGISTICS_REVIEW_SCORE]
ORDER_COLS = [0, 1, 2, 3, 4, 5, 6, 7, 8]

# LOGISTICS_COLUMN_NAMES = [ORDER_ID, ORDER_DATE, ACTION, FACILITY_TYPE, CITY_ID, LOGISTIC_COMPANY_ID, TIMESTAMP]
# LOGISTICS_COLS = [0, 1, 3, 5, 6, 7, 8]
# ORDER_COLUMN_NAMES = [ORDER_ID, ITEM_DETAIL_INFO, PAY_TIMESTAMP, PROMISE_SPEED, IF_CAINIAO, LOGISTICS_REVIEW_SCORE]
# ORDER_COLS = [1, 2, 3, 5, 6, 8]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", "-r", type=str, required=True)
    parser.add_argument("--logistics_detail", "-l", type=str, default="msom_logistic_detail")
    parser.add_argument("--order_data", "-o", type=str, default="msom_order_data")
    parser.add_argument("--indices", "-i", nargs="+", default=[i for i in range(1, 8)])

    args = parser.parse_args()
    # logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    for index in args.indices:
        logging.info(f'started reading index {index}')
        logistics_data_df = pd.read_csv(
            os.path.join(args.root, f'data_{index}', f'{args.logistics_detail}_{index}.csv'),
            names=LOGISTICS_COLUMN_NAMES)
        order_data_df = pd.read_csv(
            os.path.join(args.root, f'data_{index}', f'{args.order_data}_{index}.csv'),
            names=ORDER_COLUMN_NAMES)

        logistic_data_file_dir = os.path.join(args.root, f'data_{index}', f'{args.logistics_detail}_{index}.feather')
        order_data_file_dir = os.path.join(args.root, f'data_{index}', f'{args.order_data}_{index}.feather')

        logging.info(f'started exporting logistics data to {logistic_data_file_dir}')
        logistics_data_df.to_feather(logistic_data_file_dir)
        logging.info('finished exporting logistics data')

        logging.info(f'started exporting order data to {order_data_file_dir}')
        order_data_df.to_feather(order_data_file_dir)
        logging.info('finished exporting order data')


if __name__ == "__main__":
    main()
