import pandas as pd
import numpy as np
import yaml
import json
import os
from datetime import datetime, timezone
from application_logging.logger import logger
import gspread
import itertools
import concurrent.futures
from web3 import Web3
from web3.middleware import validation

# Params
params_path = "params.yaml"


def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


config = read_params(params_path)

try:
    logger.info("Partner Vote Data Started")

    # Params Data
    id_data = config["files"]["id_data"]
    provider_url = os.environ["RPC"]
    bribe_abi = config["web3"]["bribe_abi"]
    epoch_csv = config["files"]["epoch_data"]
    partner_data = config["files"]["partner_data"]
    revenue_data = config["files"]["revenue_data"]

    # Get Epoch Timestamp
    todayDate = datetime.utcnow()
    my_time = datetime.min.time()
    my_datetime = datetime.combine(todayDate, my_time)
    timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
    print("Today's date:", my_datetime, timestamp)

    # Read Data and wrangling
    ids_df = pd.read_csv(id_data)
    ids_df = ids_df[["symbol", "gauge.bribe"]]
    ids_df = ids_df[ids_df["gauge.bribe"] != "0x0000000000000000000000000000000000000000"]

    epoch_data = pd.read_csv(epoch_csv)
    current_epoch = epoch_data[epoch_data["timestamp"] == timestamp]["epoch"].values[0]
    epoch_data = epoch_data[epoch_data["epoch"]>=0]

    partners_df = pd.read_csv(partner_data)
    revenue_df = pd.read_csv(revenue_data)

    # Web3 and more pandas
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60}))

    def get_vote_data(partner_name, timestamp, bribe_ca):
        try:
            partner_address = Web3.toChecksumAddress(partners_df.loc[(partners_df["partner_name"] == partner_name), ['nft_address']].values[0][0])
            contract_instance = w3.eth.contract(address=bribe_ca, abi=bribe_abi)
            voteweight = contract_instance.functions.balanceOfOwnerAt(partner_address, timestamp).call()
            symbol = ids_df.loc[(ids_df["gauge.bribe"] == bribe_ca), ['symbol']].values[0][0]
            if voteweight != 0:
                vote_data.append({'partner_name': partner_name, 'partner_address': partner_address, 'epoch': current_epoch-1, 'symbol': symbol, 'voteweight': voteweight / 1e18})
        except Exception as e:
            print(f"Error processing {partner_name}: {e}")

    vote_data = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for args in itertools.product(partners_df['partner_name'], [timestamp], ids_df['gauge.bribe']):
            futures.append(executor.submit(get_vote_data, *args))
    
    vote_df = pd.DataFrame(vote_data)
    vote_df.sort_values("epoch", ascending=False, inplace=True)
    total_vote = vote_df.groupby(['partner_name', 'epoch'])['voteweight'].transform(lambda g: g.sum())
    vote_df['Vote %'] = vote_df['voteweight']/total_vote * 100

    revenue_df.rename(columns = {'name_pool':'symbol', 'voteweight':'total_voteweight'}, inplace = True)
    revenue_df_offset = revenue_df.copy(deep=True)
    revenue_df_offset["epoch"] = revenue_df_offset["epoch"] - 1
    revenue_df_offset = revenue_df_offset[revenue_df_offset["epoch"] == current_epoch-1]
    revenue_df_offset = revenue_df_offset[['epoch', 'symbol', 'emissions', 'emissions_value', 'oRETRO_price']]
    revenue_df = revenue_df[['epoch', 'symbol', 'bribe_amount', 'total_voteweight']]
    revenue_df = revenue_df[revenue_df["epoch"] == current_epoch-1]

    vote_df = pd.merge(vote_df, revenue_df, on=["epoch", "symbol"], how="left")
    vote_df = pd.merge(vote_df, revenue_df_offset, on=["epoch", "symbol"], how="left")
    vote_df['Voting Revenue'] = vote_df['bribe_amount']*vote_df['voteweight']/(vote_df['total_voteweight']+0.001)
    vote_df['Spend'] = vote_df['bribe_amount'] - vote_df['Voting Revenue']
    vote_df['Bribe ROI'] = vote_df['emissions_value']/vote_df['Spend']
    vote_df.replace(np.nan, 0, inplace=True)
    vote_df.replace(np.inf, 0, inplace=True)
    df_values = vote_df.values.tolist()
    print(vote_df)
    
    # Write to GSheets
    sheet_credentials = os.environ["GKEY"]
    sheet_credentials = json.loads(sheet_credentials)
    gc = gspread.service_account_from_dict(sheet_credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["partner_vote_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Append to Worksheet
    gs.values_append("Master", {"valueInputOption": "RAW"}, {"values": df_values})

    logger.info("Partner Vote Data Ended")
except Exception as e:
    logger.error("Error occurred during Partner Vote Data process. Error: %s" % e, exc_info=True)
