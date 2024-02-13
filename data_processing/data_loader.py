import pandas as pd

from config import *
from constants import *

MAPPING = {
    CONSIGN: 0,
    GOT: 1,
    DEPARTURE: 2,
    ARRIVAL: 2,
    SENT_SCAN: 4,
    SIGNED: 5,
    TRADE_SUCCESS: 6,
    FAILURE: 7
}

def load_full_logistics_data(i: int = 1, j: int = 8):
    """
    Loads and merges logistics data from drive
    :return: dataframe containing logistics data
    """
    file_paths = [f'{CLEANED_DATA_DIR_ROOT}/data_{i}/cleaned_logistics_detail_{i}.feather' for i in range(i, j)]

    full_logistics_data = []
    for file_path in file_paths:
        full_logistics_data.append(pd.read_feather(file_path))
    full_logistics_data_df = pd.concat(full_logistics_data)
    full_logistics_data_df.action = full_logistics_data_df.action.map(MAPPING)
    return full_logistics_data_df

def load_train_logistics_data():
    """
    Loads and merges test logistics data from drive
    :return: dataframe containing logistics data
    """
    return load_full_logistics_data(1, 5)


def load_validation_logistics_data():
    """
    Loads and merges validation logistics data from drive
    :return: dataframe containing logistics data
    """
    return load_full_logistics_data(5, 7)


def load_test_logistics_data():
    """
    Loads and merges test logistics data from drive
    :return: dataframe containing logistics data
    """
    return load_full_logistics_data(7, 8)


def load_full_order_data(i: int = 1, j: int = 8):
    """
    Loads and merges order data from drive
    :return: dataframe containing order data
    """
    file_paths = [f'{CLEANED_DATA_DIR_ROOT}/data_{i}/cleaned_order_data_{i}.feather' for i in range(i, j)]

    full_order_data = []
    for file_path in file_paths:
        full_order_data.append(pd.read_feather(file_path))
    full_order_data_df = pd.concat(full_order_data)
    return full_order_data_df


def load_train_order_data():
    """
    Loads and merges train order data from drive
    :return: dataframe containing order data
    """
    return load_full_order_data(1, 5)


def load_validation_order_data():
    """
    Loads and merges validation order data from drive
    :return: dataframe containing order data
    """
    return load_full_order_data(5, 7)


def load_test_order_data():
    """
    Loads and merges order data from drive
    :return: dataframe containing order data
    """
    return load_full_order_data(7, 8)


def load_item_data():
    """
    Loads item data from drive
    :return: dataframe containing item data
    """
    item_df = pd.read_feather(f'{CLEANED_DATA_DIR_ROOT}/data_8/msom_item_data.feather')
    return item_df
