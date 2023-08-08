import requests
import pandas as pd
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
    # Params Data
    id_data = config["files"]["id_data"]
    provider_url = config["web3"]["provider_url"]
    bribe_abi = config["web3"]["bribe_abi"]
    epoch_csv = config["files"]["epoch_data"]
    price_api = config["api"]["price_api"]
    vote_csv = config["files"]["vote_data"]

    # Pulling Vote Data
    logger.info("Vote Data Started")

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

    # Read Epoch Data
    epoch_data = pd.read_csv(epoch_csv)
    epoch = epoch_data[epoch_data["timestamp"] == timestamp]["epoch"].values[0] - 1

    # Read IDS Data
    vote_df = pd.read_csv(id_data)
    vote_df["epoch"] = epoch

    # Pull Prices
    response = requests.get(price_api)
    RETRO_price = jmespath.search("data[?name == 'RETRO'].price", response.json())[0]

    # Pull Fees Web3
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60}))

    voteweight = []
    for bribe in vote_df["gauge.bribe"]:
        if bribe == "0x0000000000000000000000000000000000000000":
            voteweight.append(0)
        else:
            contract_instance = w3.eth.contract(address=bribe, abi=bribe_abi)
            voteweight.append(
                round(
                    (
                        contract_instance.functions.totalSupplyAt(timestamp).call()
                        / 1000000000000000000
                    ),
                    2,
                )
            )

    vote_df["voteweight"] = voteweight
    vote_df = vote_df[["symbol", "epoch", "voteweight"]]
    vote_df["RETRO_price"] = RETRO_price
    vote_df["votevalue"] = vote_df["voteweight"] * vote_df["RETRO_price"]
    vote_df.columns = ["name_pool", "epoch", "voteweight", "RETRO_price", "votevalue"]

    # Rewriting current Epoch's Vote Data
    voter = pd.read_csv(vote_csv)
    current_vote_index = voter[voter["epoch"] == epoch].index
    voter.drop(current_vote_index, inplace=True)
    vote_df = pd.concat([voter, vote_df], ignore_index=True, axis=0)

    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["vote_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    worksheet1.clear()
    set_with_dataframe(
        worksheet=worksheet1,
        dataframe=vote_df,
        include_index=False,
        include_column_header=True,
        resize=True,
    )

    logger.info("Vote Data Ended")
except Exception as e:
    logger.error(
        "Error occurred during Vote Data process. Error: %s" % e, exc_info=True
    )
