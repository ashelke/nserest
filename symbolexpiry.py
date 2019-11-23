# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 15:16:11 2019

@author: AS230412
"""
import requests
import json
import pandas as pd
import collections
import pyodbc



with open("nse.json") as f:
    config = json.loads(f.read())

nse_host = config["nse_host"]
nse_database = config["nse_database"]
nse_user = config["nse_user"]
nse_password = config["nse_password"]
api_url = config["api_url"]


connection = pyodbc.connect(Driver='{SQL Server}', server=nse_host,database=nse_database,user=nse_user,password=nse_password)
    
cursor = connection.cursor()  

cursor.execute("TRUNCATE TABLE [dbo].[ExpiryDates]")
connection.commit()

sql_select_Query = """
                    select SymbolID, SymbolName from [dbo].[Symbol]
                     """
cursor.execute(sql_select_Query)
try:
    
    records = cursor.fetchall()

    df = pd.DataFrame(records)

    for i,row in df.iterrows():
        fk_id = row[0][0]
        symbol = row[0][1] 
        url = api_url + symbol
        r = requests.get(url).json()
        lists = []
        for i in r['records']['expiryDates']:
            cursor.execute("INSERT INTO [dbo].[ExpiryDates] (SymbolID,ExpiryDates) VALUES(?,?)", fk_id,i)
            connection.commit()
except:
    print("No record found")

cursor.close()
connection.close()