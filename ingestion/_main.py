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

#Import credentials
with open(r"C:\Users\FinnCharlton\credentials.csv") as creds:
    file = csv.reader(creds)
    dic = {}
    for line in file:
        dic = dic|({line[0]:line[1]})

url = dic['ï»¿url']
pat = dic['pat']
patSecret = dic['patSecret']
site = dic['site']


def fetch(instance,method):
    objects = objectList(method)
    df = objects.dfParse()
    return df

loginInstance = tableauServer(url,site,pat,patSecret)

dfWorkbooks = fetch(loginInstance,loginInstance.get_workbooks())
dfDatasources = fetch(loginInstance,loginInstance.get_datasources())
dfUsers = fetch(loginInstance,loginInstance.get_users())

print(dfUsers)