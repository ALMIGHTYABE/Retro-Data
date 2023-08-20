import requests
import pandas as pd
import yaml
from application_logging.logger import logger
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
    logger.info("Pool Data Started")

    # Params Data
    price_api = config["api"]["price_api"]
    fusion_api = config["api"]["fusion_api"]
    provider_url = config["web3"]["provider_url"]
    merkl_abi = config["web3"]["merkl_abi"]
    merkl_ca = config["web3"]["merkl_ca"]

    # Pull Price
    try:
        response = requests.get(price_api)
        pricelist = []
        for i in response.json()["data"]:
            pricelist.append([i["name"], i["address"], i["price"], i["decimals"]])

        price_df = pd.DataFrame(pricelist, columns=["name", "address", "price", "decimals"])
    except Exception as e:
        print(e)

    # Request
    response = requests.get(url=fusion_api)
    data = response.json()["data"]
    pool_df = pd.json_normalize(response.json()["data"])[["symbol", "underlyingPool"]]
    pool_df = pool_df[pool_df["symbol"] != "sAMM-USDC/USDT"]

    # Web3
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60}))
    contract_instance = w3.eth.contract(address=merkl_ca, abi=merkl_abi)

    pool_data_list = []
    for pool_name, pool_address in zip(pool_df["symbol"], pool_df["underlyingPool"]):
        pool_data = contract_instance.functions.getActivePoolDistributions(pool_address).call()
        if pool_data != []:
            token_df = pd.DataFrame(list(zip(pool_data[0][2], pool_data[0][3]))).T
            token_df.columns = ["address", "decimals", "name", "amount"]
            token_df["address"] = token_df["address"].str.lower()
            token_df = token_df.merge(price_df[["address", "price"]], on="address", how="left")
            token_df["token_amount"] = token_df["price"] * token_df["amount"]
            token_amount = []
            for dec, amt in zip(token_df["decimals"], token_df["token_amount"]):
                decimal = "1"
                decimal = decimal.ljust(dec + 1, "0")
                token_amount.append((amt / int(decimal)))
            token_df["token_amount"] = token_amount
            
            pool_data_list.append(
                {
                    "pool_name": pool_name,
                    "pool_address": pool_address,
                    "pool_data": pool_data,
                    "total_liquidity": round(token_df["token_amount"].sum(),2)
                }
            )

    pool_data = pd.DataFrame(pool_data_list)

    pool_data.to_csv("data/pool_data.csv", index=False)

    logger.info("Pool Data Ended")
except Exception as e:
    logger.error(
        "Error occurred during Pool Data process. Error: %s" % e, exc_info=True
    )
