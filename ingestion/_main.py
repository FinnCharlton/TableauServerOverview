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
import datetime
from snowflake.connector.pandas_tools import write_pandas
from snowflake_connector import SnowflakeConnector




#Import tableau server credentials
with open(r"C:\Users\FinnCharlton\credentials.csv") as creds:
    file = csv.reader(creds)
    ts_dict = {}
    for line in file:
        ts_dict = ts_dict|({line[0]:line[1]})

ts_url = ts_dict['ï»¿url']
ts_pat = ts_dict['pat']
ts_pat_secret = ts_dict['patSecret']
ts_site = ts_dict['site']

#Import snowflake credentials
with open(r"C:\Users\FinnCharlton\snowflake_credentials.csv") as creds:
    file = csv.reader(creds)
    snow_dict = {}
    for line in file:
        snow_dict = snow_dict|({line[0]:line[1]})

snow_username = snow_dict["username"]
snow_password = snow_dict["password"]
snow_account = snow_dict["account"]
snow_warehouse = snow_dict["warehouse"]
snow_database = snow_dict["database"]
snow_schema = snow_dict["schema"]



#Define function for pulling fact tables
def fetch(instance,method):
    objects = objectList(method)
    df = objects.dfParse()
    return df

#Define function for adding time of creation
def add_updated_time(df):
    df["_src__updated_at"] = datetime.datetime.now()

#Tableau Server Client Login
ts_login_instance = tableauServer(ts_url,ts_site,ts_pat,ts_pat_secret)

#Get fact tables
try:
    df_workbooks = fetch(ts_login_instance,ts_login_instance.get_workbooks())
    print("Workbook information retrieved")
    df_datasources = fetch(ts_login_instance,ts_login_instance.get_datasources())
    print("Datasource information retrieved")
    df_users = fetch(ts_login_instance,ts_login_instance.get_users())
    print("User information retrieved")
    df_views = fetch(ts_login_instance,ts_login_instance.get_views())
    print("View information retrieved")

except Exception as e:
    print(f"Error getting fact tables : {e}")

print(df_views)

#Get mapping tables
try:
    df_datasource_mappings = pd.DataFrame(ts_login_instance.get_datasource_mappings()).explode(["datasource_ids"])
    print("Datasource mappings retrieved")

except Exception as e:
    print(f"Error getting mapping tables : {e}")

# #Set upload information
upload_info = [
    { "name":"src_workbooks", "content":df_workbooks },
    { "name":"src_datasources", "content":df_datasources },
    { "name":"src_users", "content":df_users },
    { "name":"src_views", "content":df_views },
    { "name":"src_datasource_mappings", "content":df_datasource_mappings }
]

# #Add updated_at column
for dict in upload_info:
    add_updated_time(dict["content"])

# #Snowflake Login
snow_connection = SnowflakeConnector(
    username=snow_username,
    password=snow_password,
    account=snow_account,
    warehouse=snow_warehouse,
    database=snow_database,
    schema=snow_schema
    )

# #Snowflake upload
snow_connection.ingest(upload_info)