import pandas as pd
import numpy as np
import yaml
import os
from datetime import datetime, timezone
from application_logging.logger import logger
import gspread
from web3 import Web3
from web3.middleware import validation
import json
from requests import get, post

# Params
params_path = "params.yaml"

def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config

config = read_params(params_path)

try:
    logger.info("APR Data Started")

    # Params Data
    epoch_csv = config["files"]["epoch_data"]
    emissions_schedule_data = config["files"]["emissions_schedule"]
    revenue_data = config["files"]["revenue_data"]
    provider_url = os.environ["RPC"]

    # Get Epoch Timestamp
    todayDate = datetime.utcnow()
    my_time = datetime.min.time()
    my_datetime = datetime.combine(todayDate, my_time)
    timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
    print("Today's date:", my_datetime, timestamp)

    # Read Data
    epoch_data = pd.read_csv(epoch_csv)
    epoch_data["epoch"] = epoch_data["epoch"] - 1
    epoch_data = epoch_data[epoch_data["epoch"]>=0]
    current_epoch = epoch_data[epoch_data["timestamp"] == timestamp]["epoch"].values[0]

    emissions_df = pd.read_csv(emissions_schedule_data)
    emissions_df["Epoch"] = emissions_df["Epoch"] - 1
    emissions_df = emissions_df[emissions_df["Epoch"]>= 0]

    revenue_df = pd.read_csv(revenue_data)
    retro_price_df = revenue_df[['epoch', 'RETRO_price']].groupby('epoch').max().reset_index()
    revenue_df = revenue_df[revenue_df['epoch'] != revenue_df['epoch'].max()]

    epoch_wise_df = revenue_df.groupby('epoch')[['fee_amount', 'bribe_amount', 'voter_share', 'voteweight']].sum().reset_index()
    epoch_wise_df = epoch_wise_df.merge(emissions_df[['Epoch', 'Rebase']], left_on='epoch', right_on="Epoch")
    epoch_wise_df.drop("Epoch", axis=1, inplace=True)
    epoch_wise_df = epoch_wise_df[epoch_wise_df['epoch'].isin([current_epoch])]
    epoch_filtered = epoch_data[epoch_data['epoch'].isin(epoch_wise_df['epoch'].unique())]

    # Web3 and more pandas
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60}))

    veRetro_abi = config["web3"]["veRETRO_abi"]
    veRetro_ca = config["web3"]["veRETRO_ca"]
    contract_instance1 = w3.eth.contract(address=veRetro_ca, abi=veRetro_abi)

    total_veretro_list = []
    for t in epoch_filtered['timestamp']:
        response = get("https://coins.llama.fi/block/polygon/" + str(t))
        block = response.json()["height"]
        total_veretro_list.append(contract_instance1.functions.totalSupplyAt(block).call() / 1000000000000000000)

    epoch_wise_df['total_veretro'] = total_veretro_list
    epoch_wise_df = epoch_wise_df.merge(retro_price_df)

    epoch_wise_df['fee_apr'] = epoch_wise_df['fee_amount'] / (epoch_wise_df['voteweight'] * epoch_wise_df['RETRO_price']) * 100 * 52
    epoch_wise_df['bribe_apr'] = epoch_wise_df['bribe_amount'] / (epoch_wise_df['voteweight'] * epoch_wise_df['RETRO_price']) * 100 * 52
    epoch_wise_df['voting_apr'] = epoch_wise_df['voter_share'] / (epoch_wise_df['voteweight'] * epoch_wise_df['RETRO_price']) * 100 * 52
    epoch_wise_df['rebase_apr'] = epoch_wise_df['Rebase'] / epoch_wise_df['total_veretro'] * 100 * 52

    pd.set_option('display.float_format', lambda x: '%.5f' % x)

    epoch_wise_df = epoch_wise_df[['epoch', 'fee_apr', 'bribe_apr',
       'voting_apr', 'rebase_apr']]

    print(epoch_wise_df)
    df_values = epoch_wise_df.values.tolist()
    
    # Write to GSheets
    sheet_credentials = os.environ["GKEY"]
    sheet_credentials = json.loads(sheet_credentials)
    gc = gspread.service_account_from_dict(sheet_credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["apr_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Append to Worksheet
    gs.values_append("Master", {"valueInputOption": "USER_ENTERED"}, {"values": df_values})

    logger.info("APR Data Ended")
except Exception as e:
    logger.error("Error occurred during APR Data process. Error: %s" % e, exc_info=True)
