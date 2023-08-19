import requests
import pandas as pd
import yaml
import json
import os
from datetime import datetime, timezone, timedelta
from application_logging.logger import logger
import gspread


# Params
params_path = "params.yaml"

def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config

config = read_params(params_path)


# Fusion   
try:
    logger.info("Day Data Fusion Started")

    # Params Data
    subgraph = config["query"]["fusion_subgraph"]
    day_data_fusion_query = config["query"]["day_data_fusion_query"]
    daily_data_fusion_csv = config["files"]["daily_data_fusion"]
    
    # Date Stuff
    todayDate = datetime.utcnow()
    twodayago = todayDate - timedelta(2)
    my_time = datetime.min.time()
    my_datetime = datetime.combine(twodayago, my_time)
    timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
    
    # Request
    day_data_fusion_query["variables"]["startTime"] = timestamp
    response = requests.post(url=subgraph, json=day_data_fusion_query)
    data = response.json()["data"]["uniswapDayDatas"]
    day_data_fusion_df = pd.DataFrame(data)
    day_data_fusion_df["date"] = day_data_fusion_df["date"].apply(lambda timestamp: datetime.utcfromtimestamp(timestamp).date())
    day_data_fusion_df["date"] = day_data_fusion_df["date"].apply(lambda date: datetime.strftime(date, "%Y-%m-%d"))

    day_data_fusion_old = pd.read_csv(daily_data_fusion_csv)
    drop_index = day_data_fusion_old[day_data_fusion_old['date']>datetime.fromtimestamp(timestamp).strftime(format='%Y-%m-%d')].index
    index_list = drop_index.to_list()
    index_list = list(map(lambda x: x + 2, index_list))
    day_data_fusion_df['__typename'] = 'Fusion'
    day_data_fusion_df = day_data_fusion_df.astype({'id':'int','volumeUSD':'float', 'feesUSD':'float',	'tvlUSD':'float'})
    df_values = day_data_fusion_df.values.tolist()
    
    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["daily_data_fusion_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    if index_list != []:
        worksheet1.delete_rows(index_list[0], index_list[-1])

    # Append to Worksheet
    gs.values_append("Master", {"valueInputOption": "USER_ENTERED"}, {"values": df_values})

    logger.info("Day Data Fusion Ended")
except Exception as e:
    logger.error("Error occurred during Day Data Fusion process. Error: %s" % e, exc_info=True)