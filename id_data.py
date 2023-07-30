import requests
import pandas as pd
import yaml
from application_logging.logger import logger


# Params
params_path = "params.yaml"


def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


config = read_params(params_path)

try:
    logger.info("ID Data Started")

    # Params Data
    fusion_api = config["api"]["fusion_api"]

    # Request
    response = requests.get(url=fusion_api)
    data = response.json()["data"]
    ids_df = pd.json_normalize(response.json()['data'])[['symbol', 'address', 'isGamma', 'feeLevel', 'underlyingPool', 'type', 'gauge.address', 'gauge.fee', 'gauge.bribe']]
    ids_df.to_csv("data/ids_data.csv", index=False)

    logger.info("ID Data Ended")
except Exception as e:
    logger.error("Error occurred during ID Data process. Error: %s" % e, exc_info=True)
