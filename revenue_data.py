import requests
import pandas as pd
import numpy as np
import yaml
import json
import os
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta, TH
from application_logging.logger import logger
import gspread
from gspread_dataframe import set_with_dataframe
from web3 import Web3
from web3.middleware import validation
import jmespath


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
    pair_data = config["files"]["pair_data_fusion"]
    vote_data = config["files"]["vote_data"]
    emissions_data = config["files"]["emissions_data"]
    provider_url = config["web3"]["provider_url"]
    bribe_abi = config["web3"]["bribe_abi"]
    price_api = config["api"]["price_api"]

    # Read Data
    bribe_df = pd.read_csv(bribe_data)
    fee_df = pd.read_csv(fee_data)
    pair_df = pd.read_csv(pair_data)
    vote_df = pd.read_csv(vote_data)
    emissions_df = pd.read_csv(emissions_data)

    # Get Epoch Timestamp
    todayDate = datetime.utcnow()
    if todayDate.isoweekday() == 4 and todayDate.hour > 1:
        nextThursday = todayDate + relativedelta(weekday=TH(2))
        my_time = datetime.min.time()
        my_datetime = datetime.combine(nextThursday, my_time)
        timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
        print("Yes, The next Thursday date:", my_datetime, timestamp)
    else:
        nextThursday = todayDate + relativedelta(weekday=TH(0))
        my_time = datetime.min.time()
        my_datetime = datetime.combine(nextThursday, my_time)
        timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
        print("No, The next Thursday date:", my_datetime, timestamp)

    # Pull Prices
    response = requests.get(price_api)
    RETRO_price = jmespath.search("data[?name == 'RETRO'].price", response.json())[0]

    # Web3
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60}))

    # Data Wrangling
    pair_df.columns = [
        "id",
        "date",
        "tvlUSD",
        "volumeUSD",
        "volumeToken0",
        "volumeToken1",
        "token0Price",
        "token1Price",
        "feesUSD",
        "__typename",
        "name_pool",
        "underlyingPool",
        "type",
        "epoch",
    ]
    epoch_wise_pair_fees = pair_df.groupby(["epoch", "name_pool"], as_index=False)[
        "feesUSD"
    ].sum()
    epoch_wise_fees = fee_df.groupby(["epoch", "name_pool"], as_index=False)[
        "fee_amount"
    ].sum()
    epoch_wise_bribes = bribe_df.groupby(["epoch", "name_pool"], as_index=False)[
        "bribe_amount"
    ].sum()
    df = pd.merge(
        epoch_wise_fees, epoch_wise_pair_fees, on=["epoch", "name_pool"], how="outer"
    )
    df = pd.merge(df, epoch_wise_bribes, on=["epoch", "name_pool"], how="outer")
    df.replace(np.nan, 0, inplace=True)
    df.columns = ["epoch", "name_pool", "fee_amount", "total_feesUSD", "bribe_amount"]
    df["voter_share"] = df["fee_amount"] + df["bribe_amount"]
    df["revenue"] = df["total_feesUSD"] + df["bribe_amount"]
    bribe_df_offset = epoch_wise_bribes.copy(deep=True)
    bribe_df_offset["epoch"] = bribe_df_offset["epoch"] + 1
    bribe_df_offset.columns = ["epoch", "name_pool", "bribe_amount_offset"]
    df = pd.merge(df, bribe_df_offset, on=["epoch", "name_pool"], how="outer")
    final_df = pd.merge(df, vote_df, on=["epoch", "name_pool"], how="outer")
    final_df.replace(np.nan, 0, inplace=True)
    final_df["vote_apr"] = final_df["voter_share"] / final_df["votevalue"] * 100 * 52
    emissions_df.columns = [
        "epoch",
        "name_pool",
        "emissions",
        "emissions_value",
        "RETRO_price",
        "oRETRO_price"
    ]
    emissions_df = emissions_df[["epoch", "name_pool", "emissions", "emissions_value", "oRETRO_price"]]
    final_df = pd.merge(final_df, emissions_df, on=["epoch", "name_pool"], how="outer")
    final_df.replace(np.nan, 0, inplace=True)
    final_df.replace([np.inf, -np.inf], 0, inplace=True)
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
    logger.error(
        "Error occurred during Revenue Data process. Error: %s" % e, exc_info=True
    )
