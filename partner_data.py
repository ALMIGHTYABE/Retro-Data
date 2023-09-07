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
import time
from requests import get, post

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
    price_api = config["api"]["price_api"]
    provider_url = os.environ["RPC"]
    bribe_abi = config["web3"]["bribe_abi"]
    epoch_csv = config["files"]["epoch_data"]
    partner_data = config["files"]["partner_data"]
    revenue_data = config["files"]["revenue_data"]

    # Pull Prices
    response = get(price_api)
    pricelist = []
    for i in response.json()["data"]:
        pricelist.append([i["name"], i["address"], i["price"], i["decimals"]])
    price_df = pd.DataFrame(pricelist, columns=["name", "address", "price", "decimals"])
    
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
    partners_df["nft_address"] = partners_df["nft_address"].str.lower()
    revenue_df = pd.read_csv(revenue_data)

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

    execution_id = execute_query("2944562", "large")

    if check_query_completion(execution_id) == True:
        response = get_query_results(execution_id)
        bribe_df = pd.DataFrame(response.json()["result"]["rows"])

    bribe_df["reward_provider"] = bribe_df["reward_provider"].str.lower()
    bribe_df = pd.merge(bribe_df, partners_df, left_on="reward_provider", right_on="nft_address")
    bribe_df = bribe_df.merge(price_df[["name", "address", "price", "decimals"]], left_on="reward_token", right_on="address", how="left")
    bribe_df.dropna(axis=0, inplace=True)
    bribe_df.reset_index(drop=True, inplace=True)
    bribe_df["reward_amount"] = bribe_df["reward_amount"].astype(float)
    bribe_df["bribe_amount"] = bribe_df["price"] * bribe_df["reward_amount"]
    bribe_df["decimals"] = bribe_df["decimals"].astype(int)

    bribe_amount = []
    for dec, amt in zip(bribe_df["decimals"], bribe_df["bribe_amount"]):
        decimal = "1"
        decimal = decimal.ljust(dec + 1, "0")
        bribe_amount.append((amt / int(decimal)))

    bribe_df["bribe_amount"] = bribe_amount
    ids_df["gauge.bribe"] = ids_df["gauge.bribe"].str.lower()
    bribe_df = bribe_df.merge(ids_df, left_on="reward_pool", right_on="gauge.bribe")
    bribe_df.drop(labels=['reward_amount', 'reward_provider', 'reward_token', 'name', 'address', 'price', 'decimals', 'gauge.bribe'], axis=1, inplace=True)
    bribe_df["epoch"] = current_epoch-1
    bribe_df = bribe_df[['partner_name', 'nft_address', 'epoch', 'symbol', 'reward_pool', 'bribe_amount']]

    # Web3 and more pandas
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60}))

    def get_vote_data(partner_name, timestamp, bribe_ca):
        try:
            partner_address = Web3.toChecksumAddress(partners_df.loc[(partners_df["partner_name"] == partner_name), ['nft_address']].values[0][0])
            bribe_ca = Web3.toChecksumAddress(bribe_ca)
            contract_instance = w3.eth.contract(address=bribe_ca, abi=bribe_abi)
            voteweight = contract_instance.functions.balanceOfOwnerAt(partner_address, timestamp).call()
            symbol = ids_df.loc[(ids_df["gauge.bribe"] == bribe_ca.lower()), ['symbol']].values[0][0]
            if voteweight != 0:
                vote_data.append({'partner_name': partner_name, 'nft_address': partner_address.lower(), 'epoch': current_epoch-1, 'symbol': symbol, 'reward_pool': bribe_ca.lower(), 'voteweight': voteweight / 1e18})
        except Exception as e:
            print(f"Error processing {partner_name}: {e}")

    vote_data = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for args in itertools.product(bribe_df['partner_name'].unique(), [timestamp], ids_df['gauge.bribe']):
            futures.append(executor.submit(get_vote_data, *args))
    
    vote_df = pd.DataFrame(vote_data)
    vote_df.sort_values("epoch", ascending=False, inplace=True)
    total_vote = vote_df.groupby(['partner_name', 'epoch'])['voteweight'].transform(lambda g: g.sum())
    vote_df['Vote %'] = vote_df['voteweight']/total_vote * 100
    vote_df = pd.merge(vote_df, bribe_df[["nft_address", "reward_pool", "bribe_amount"]], on=["nft_address", "reward_pool"], how="outer")
    vote_df['partner_name'] = pd.merge(vote_df[['nft_address']], partners_df, on="nft_address", how="left")['partner_name']
    vote_df['epoch'] = current_epoch-1
    vote_df['symbol'] = pd.merge(vote_df[['reward_pool']], ids_df, left_on="reward_pool", right_on="gauge.bribe", how="left")["symbol"]
    vote_df.replace(np.nan, 0, inplace=True)

    revenue_df.rename(columns = {'name_pool':'symbol', 'voteweight':'total_voteweight'}, inplace = True)
    revenue_df_offset = revenue_df.copy(deep=True)
    revenue_df_offset["epoch"] = revenue_df_offset["epoch"] - 1
    revenue_df_offset = revenue_df_offset[revenue_df_offset["epoch"] == current_epoch-1]
    revenue_df_offset = revenue_df_offset[['epoch', 'symbol', 'emissions', 'emissions_value', 'oRETRO_price']]
    revenue_df = revenue_df[['epoch', 'symbol', 'total_voteweight']]
    revenue_df = revenue_df[revenue_df["epoch"] == current_epoch-1]

    vote_df = pd.merge(vote_df, revenue_df, on=["epoch", "symbol"], how="left")
    vote_df = pd.merge(vote_df, revenue_df_offset, on=["epoch", "symbol"], how="left")
    vote_df.replace(np.nan, 0, inplace=True)
    vote_df['Voting Revenue'] = vote_df['bribe_amount']*vote_df['voteweight']/(vote_df['total_voteweight']+0.001)
    vote_df['Spend'] = vote_df['bribe_amount'] - vote_df['Voting Revenue']
    vote_df['Bribe ROI'] = vote_df['emissions_value']/vote_df['Spend']
    vote_df.drop("reward_pool", axis=1, inplace=True)
    vote_df.replace(np.inf, 0, inplace=True)
    print(vote_df)
    df_values = vote_df.values.tolist()
    
    # Write to GSheets
    sheet_credentials = os.environ["GKEY"]
    sheet_credentials = json.loads(sheet_credentials)
    gc = gspread.service_account_from_dict(sheet_credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["partner_vote_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Append to Worksheet
    gs.values_append("Master", {"valueInputOption": "USER_ENTERED"}, {"values": df_values})

    logger.info("Partner Vote Data Ended")
except Exception as e:
    logger.error("Error occurred during Partner Vote Data process. Error: %s" % e, exc_info=True)