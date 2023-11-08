import pandas as pd

from config import *


def load_full_logistics_data():
    """
    Loads and merges logistics data from drive
    :return: dataframe containing logistics data
    """
    file_paths = [f'{CLEANED_DATA_DIR_ROOT}/data_{i}/cleaned_logistics_detail_{i}.feather' for i in range(1, 8)]

    full_logistics_data = []
    for file_path in file_paths:
        full_logistics_data.append(pd.read_feather(file_path))
    full_logistics_data_df = pd.concat(full_logistics_data)
    return full_logistics_data_df


def load_full_order_data():
    """
    Loads and merges order data from drive
    :return: dataframe containing order data
    """
    file_paths = [f'{CLEANED_DATA_DIR_ROOT}/data_{i}/cleaned_order_data_{i}.feather' for i in range(1, 8)]

    full_order_data = []
    for file_path in file_paths:
        full_order_data.append(pd.read_feather(file_path))
    full_order_data_df = pd.concat(full_order_data)
    return full_order_data_df


def load_item_data():
    """
    Loads item data from drive
    :return: dataframe containing item data
    """
    item_df = pd.read_feather(f'{CLEANED_DATA_DIR_ROOT}/data_8/msom_item_data.feather')
    return item_df
