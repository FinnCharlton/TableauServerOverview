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



#Define function for pulling fact tables
def fetch(instance,method):
    objects = objectList(method)
    df = objects.dfParse()
    return df



#Tableau Server Client Login
loginInstance = tableauServer(TSurl,TSsite,TSpat,TSpatSecret)

#Get fact tables
try:
    df_workbooks = fetch(loginInstance,loginInstance.get_workbooks())
    print("Workbook information retrieved")
    df_datasources = fetch(loginInstance,loginInstance.get_datasources())
    print("Datasource information retrieved")
    df_users = fetch(loginInstance,loginInstance.get_users())
    print("User information retrieved")
    df_views = fetch(loginInstance,loginInstance.get_views())
    print("View information retrieved")

except Exception as e:
    print(f"Error getting fact tables : {e}")

#Get mapping tables
try:
    df_datasource_mappings = pd.DataFrame(loginInstance.get_datasource_mappings()).explode(["datasource_ids"])
    print("Datasource mappings retrieved")
    df_view_mappings = pd.DataFrame(loginInstance.get_view_mappings()).explode(["view_ids"])
    print("View mappings retrieved")

except Exception as e:
    print(f"Error getting mapping tables : {e}")

#Set upload information
upload_info = [
    { "name":"src_workbooks", "content":df_workbooks },
    { "name":"src_datasources", "content":df_datasources },
    { "name":"src_users", "content":df_users },
    { "name":"src_views", "content":df_views },
    { "name":"src_datasource_mappings", "content":df_datasource_mappings },
    { "name":"src_view_mappings", "content":df_view_mappings },
]



#Snowflake Login
snowConnection = SnowflakeConnector(
    username=snowUsername,
    password=snowPassword,
    account=snowAccount,
    warehouse=snowWarehouse,
    database=snowDatabase,
    schema=snowSchema
    )

#Snowflake upload
snowConnection.ingest(upload_info)