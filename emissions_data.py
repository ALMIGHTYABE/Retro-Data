import requests
import pandas as pd
import numpy as np
import yaml
import json
import os
from datetime import datetime, timezone
from application_logging.logger import logger
import jmespath
import gspread

# Params
params_path = "params.yaml"


def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


config = read_params(params_path)

try:
    logger.info("Emissions Data Started")

    # Params Data
    id_data = config["files"]["id_data"]
    epoch_csv = config["files"]["epoch_data"]
    price_api = config["api"]["price_api"]
    dune_data = config["files"]["dune_emission_data"]


    # Get Epoch Timestamp
    todayDate = datetime.utcnow()
    my_time = datetime.min.time()
    my_datetime = datetime.combine(todayDate, my_time)
    timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
    print("Today's date:", my_datetime, timestamp)

    # Read Epoch Data
    epoch_data = pd.read_csv(epoch_csv)
    epoch = epoch_data[epoch_data["timestamp"] == timestamp]["epoch"].values[0]
    epoch = 1
    # Read IDS Data
    ids_df = pd.read_csv(id_data)
    ids_df["epoch"] = epoch
    ids_df["gauge.address"] = ids_df["gauge.address"].str.lower()

    # Read Dune Data and wrangling
    dune_credentials = os.environ["DUNE"]
    dune_data = dune_data + dune_credentials
    df = pd.read_csv(dune_data)
    df.drop(labels=["evt_tx_hash", "evt_index", "evt_block_time", "evt_block_number"], axis=1, inplace=True)
    df['reward'] = df['reward'].astype(float) / 1e18
    df.columns = ["gauge.address", "emissions"]
    df["gauge.address"] = df["gauge.address"].str.lower()
    ids_df = pd.merge(ids_df, df, on="gauge.address", how="outer")
    ids_df.replace(np.nan, 0, inplace=True)
    ids_df = ids_df[ids_df["emissions"]!=0]

    # Pull Prices
    response = requests.get(price_api)
    RETRO_price = jmespath.search("data[?name == 'RETRO'].price", response.json())[0]

    # Cleanup
    ids_df["RETRO_price"] = RETRO_price
    ids_df["value"] = ids_df["emissions"] * ids_df["RETRO_price"]
    ids_df = ids_df[["epoch", "symbol", "emissions", "value", "RETRO_price"]]
    df_values = ids_df.values.tolist()
    
    # Write to GSheets
    sheet_credentials = os.environ["GKEY"]
    sheet_credentials = json.loads(sheet_credentials)
    gc = gspread.service_account_from_dict(sheet_credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["emissions_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Append to Worksheet
    gs.values_append("Master", {"valueInputOption": "RAW"}, {"values": df_values})

    logger.info("Emissions Data Ended")
except Exception as e:
    logger.error("Error occurred during Emissions Data process. Error: %s" % e, exc_info=True)
