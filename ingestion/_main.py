"""
This script authorises through the tableau server client, 
fetches workbook and datasources information, parses it
and pushes it to snowflake
"""

#Import packages
import tableauserverclient as tsc
from tsc import tableauServer
import pandas as pd
import numpy as np

url = 'https://tableauserver.theinformationlab.co.uk'
pat = 'token123'
patSecret = 'ucGM4cVsSOGniB2bWYDz3w==:dShpU8QnXa6gfEIieK907ORzzzMUTQRS'
site = 'til2'

loginInstance = tableauServer(url,site,pat,patSecret)

wbs = loginInstance.get_workbooks()
ls = [{"name":wb.name,"id":wb.id} for wb in wbs]
df = pd.DataFrame(ls)
print(df)