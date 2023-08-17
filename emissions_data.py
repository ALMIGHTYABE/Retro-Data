from requests import get, post
import pandas as pd
import numpy as np
import yaml
import json
import os
from datetime import datetime, timezone
from application_logging.logger import logger
import jmespath
import gspread
import time

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

    # Get Epoch Timestamp
    todayDate = datetime.utcnow()
    my_time = datetime.min.time()
    my_datetime = datetime.combine(todayDate, my_time)
    timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
    print("Today's date:", my_datetime, timestamp)

    # Read Epoch Data
    epoch_data = pd.read_csv(epoch_csv)
    epoch = epoch_data[epoch_data["timestamp"] == timestamp]["epoch"].values[0]

    # Read IDS Data
    ids_df = pd.read_csv(id_data)
    ids_df["epoch"] = epoch
    ids_df["gauge.address"] = ids_df["gauge.address"].str.lower()

    # Read Dune Data and wrangling
    API_KEY = os.environ["DUNE"]
    HEADER = {"x-dune-api-key": API_KEY}

    BASE_URL = "https://api.dune.com/api/v1/"

    def make_api_url(module, action, ID):
        url = BASE_URL + module + "/" + ID + "/" + action
        return url

    def execute_query(query_id, engine="large"):
        url = make_api_url("query", "execute", query_id)
        params = {
            "performance": engine,
        }
        response = post(url, headers=HEADER, params=params)
        execution_id = response.json()["execution_id"]
        return execution_id

    def get_query_status(execution_id):
        url = make_api_url("execution", "status", execution_id)
        response = get(url, headers=HEADER, timeout=60)
        return response

    def get_query_results(execution_id):
        url = make_api_url("execution", "results", execution_id)
        response = get(url, headers=HEADER)
        return response

    def check_query_completion(execution_id):
        while True:
            time.sleep(5)
            response = get_query_status(execution_id)
            if response.json()["state"] == "QUERY_STATE_COMPLETED":
                break
        return True

    execution_id = execute_query("2823233", "large")

    if check_query_completion(execution_id) == True:
        response = get_query_results(execution_id)
        df = pd.DataFrame(response.json()["result"]["rows"])

    df.drop(
        labels=["evt_tx_hash", "evt_index", "evt_block_time", "evt_block_number"],
        axis=1,
        inplace=True,
    )
    df["reward"] = df["reward"].astype(float) / 1e18
    df.columns = ["gauge.address", "emissions"]
    df["gauge.address"] = df["gauge.address"].str.lower()
    ids_df = pd.merge(ids_df, df, on="gauge.address", how="outer")
    ids_df.replace(np.nan, 0, inplace=True)
    ids_df = ids_df[ids_df["emissions"] != 0]

    # Pull Prices
    response = get(price_api)
    RETRO_price = jmespath.search("data[?name == 'RETRO'].price", response.json())[0]
    oRETRO_price = jmespath.search("data[?name == 'oRETRO'].price", response.json())[0]

    # Cleanup
    ids_df["RETRO_price"] = RETRO_price
    ids_df["oRETRO_price"] = oRETRO_price
    ids_df["value"] = ids_df["emissions"] * ids_df["oRETRO_price"]
    ids_df = ids_df[
        ["epoch", "symbol", "emissions", "value", "RETRO_price", "oRETRO_price"]
    ]
    df_values = ids_df.values.tolist()
    print(ids_df)

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
    logger.error(
        "Error occurred during Emissions Data process. Error: %s" % e, exc_info=True
    )
