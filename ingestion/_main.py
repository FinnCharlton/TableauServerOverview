"""
This script authorises through the tableau server client, 
fetches workbook and datasources information, parses it
and pushes it to snowflake
"""

#Import packages
import tableauserverclient as tsc
from tsc import tableauServer
from parser import objectList
import pandas as pd
import numpy as np
import csv as csv
import snowflake as snow 
from snowflake.connector.pandas_tools import write_pandas
from snowflake_connector import SnowflakeConnector

#Import tableau server credentials
with open(r"C:\Users\FinnCharlton\credentials.csv") as creds:
    file = csv.reader(creds)
    dicTS = {}
    for line in file:
        dicTS = dicTS|({line[0]:line[1]})

TSurl = dicTS['ï»¿url']
TSpat = dicTS['pat']
TSpatSecret = dicTS['patSecret']
TSsite = dicTS['site']


def fetch(instance,method):
    objects = objectList(method)
    df = objects.dfParse()
    return df

#Import snowflake credentials
with open(r"C:\Users\FinnCharlton\snowflake_credentials.csv") as creds:
    file = csv.reader(creds)
    dicSnow = {}
    for line in file:
        dicSnow = dicSnow|({line[0]:line[1]})

snowUsername = dicSnow["username"]
snowPassword = dicSnow["password"]
snowAccount = dicSnow["account"]
snowWarehouse = dicSnow["warehouse"]
snowDatabase = dicSnow["database"]
snowSchema = dicSnow["schema"]

loginInstance = tableauServer(TSurl,TSsite,TSpat,TSpatSecret)

# dfWorkbooks = fetch(loginInstance,loginInstance.get_workbooks())
# dfDatasources = fetch(loginInstance,loginInstance.get_datasources())
dfUsers = fetch(loginInstance,loginInstance.get_users())

# print(dfUsers.head())

snowConnection = SnowflakeConnector(
    username=snowUsername,
    password=snowPassword,
    account=snowAccount,
    warehouse=snowWarehouse,
    database=snowDatabase,
    schema=snowSchema
    )

snowConnection.ingest(
    dfUsers,
    "src_users"
)


