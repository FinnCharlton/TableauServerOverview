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

#Define function for adding time of creation
def add_updated_time(df):
    df["_src__updated_at"] = datetime.datetime.now()

#List of Tableau Server Sites
sites = ['til2','DataSchool','TILIE','TILUS']

#Create dataframes from API calls
for index, site in enumerate(sites):
    
#Tableau Server Client Login
    ts_login_instance = tableauServer(ts_url,site,ts_pat,ts_pat_secret)

    if index == 0:

        #Get fact tables
        try:
            df_workbooks = ts_login_instance.get_workbooks()
            print(f"{site} Workbook information retrieved")
            df_datasources = ts_login_instance.get_datasources()
            print(f"{site} Datasource information retrieved")
            df_users = ts_login_instance.get_users()
            print(f"{site} User information retrieved")
            df_views = ts_login_instance.get_views()
            print(f"{site} View information retrieved")

        except Exception as e:
            print(f"Error getting fact tables : {e}")


        #Get mapping tables
        try:
            df_datasource_mappings = pd.DataFrame(ts_login_instance.get_datasource_mappings()).explode(["datasource_name"])
            df_datasource_mappings["site"] = site
            print(f"{site} Datasource mappings retrieved")

        except Exception as e:
            print(f"Error getting mapping tables : {e}")

    else:

        #Get fact tables
        try:
            df_workbooks = pd.concat([df_workbooks,ts_login_instance.get_workbooks()])
            print(f"{site} Workbook information retrieved")

            df_datasources = pd.concat([df_datasources,ts_login_instance.get_datasources()])
            print(f"{site} Datasource information retrieved")

            df_users = pd.concat([df_users,ts_login_instance.get_users()])
            print(f"{site} User information retrieved")

            df_views = pd.concat([df_views,ts_login_instance.get_views()])
            print(f"{site} View information retrieved")

        except Exception as e:
            print(f"Error getting fact tables : {e}")


        #Get mapping tables
        try:
            site_ds_mappings = pd.DataFrame(ts_login_instance.get_datasource_mappings()).explode(["datasource_name"])
            site_ds_mappings["site"] = site
            df_datasource_mappings = pd.concat([df_datasource_mappings,site_ds_mappings])

            print(f"{site} Datasource mappings retrieved")

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

# # #Snowflake upload
snow_connection.ingest(upload_info)