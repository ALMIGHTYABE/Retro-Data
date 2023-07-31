import pandas as pd
import numpy as np
import yaml
import json
import os
from application_logging.logger import logger
import gspread
from gspread_dataframe import set_with_dataframe


# Params
params_path = "params.yaml"


def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


config = read_params(params_path)

try:
    logger.info("Revenue Data Started")

    # Params Data
    bribe_data = config["files"]["bribe_data"]
    fee_data = config["files"]["fee_data"]
    emissions_data = config["files"]["emissions_data"]

    # Read Data
    bribe_df = pd.read_csv(bribe_data)
    fee_df = pd.read_csv(fee_data)
    emissions_df = pd.read_csv(emissions_data)

    # Data Wrangling
    epoch_wise_fees = fee_df.groupby(["epoch", "name_pool"], as_index=False)["fee_amount"].sum()
    epoch_wise_bribes = bribe_df.groupby(["epoch", "name_pool"], as_index=False)["bribe_amount"].sum()
    df = pd.merge(epoch_wise_fees, epoch_wise_bribes, on=["epoch", "name_pool"], how="outer")
    df.replace(np.nan, 0, inplace=True)
    df["revenue"] = df["fee_amount"] + df["bribe_amount"]
    bribe_df_offset = epoch_wise_bribes.copy(deep=True)
    bribe_df_offset["epoch"] = bribe_df_offset["epoch"] + 1
    bribe_df_offset.columns = ["epoch", "name_pool", "bribe_amount_offset"]
    df = pd.merge(df, bribe_df_offset, on=["epoch", "name_pool"], how="outer")

    # final_df = pd.merge(df, emissions_df, on=["epoch", "name"], how="outer")
    final_df = df
    final_df.replace(np.nan, 0, inplace=True)
    final_df.sort_values(by="epoch", axis=0, ignore_index=True, inplace=True)
    latest_epoch = final_df["epoch"].iloc[-1]
    latest_data_index = final_df[final_df["epoch"] == latest_epoch].index
    final_df.drop(latest_data_index, inplace=True)

    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["revenue_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    worksheet1.clear()
    set_with_dataframe(
        worksheet=worksheet1,
        dataframe=final_df,
        include_index=False,
        include_column_header=True,
        resize=True,
    )

    logger.info("Revenue Data Ended")
except Exception as e:
    logger.error("Error occurred during Revenue Data process. Error: %s" % e, exc_info=True)
