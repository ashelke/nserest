# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 20:27:01 2019

@author: AS230412
"""

import requests
import json
import pandas as pd
import xlwings as xw
from time import sleep
from datetime import datetime, time, timedelta
import os
import numpy as np
import collections
import pymssql
import symbolexpiry

#url = 'https://beta.nseindia.com/api/option-chain-indices?symbol=NIFTY'
#symbol = 'NIFTY'
#expiry = "31-Oct-2019"
#xl = "OptionChainData.xlsx"
#wb = xw.Book(xl)
#sh_live = wb.sheets("data")

class nse(object):
    
    def __init__(self, config):
        self.nse_host = config["nse_host"]
        self.nse_database = config["nse_database"]
        self.nse_user = config["nse_user"]
        self.nse_password = config["nse_password"]
        self.api_url = config["api_url"]
        
    def nsedata(self, **values):
        try:
            
            connection = pymssql.connect(host=self.nse_host, user=self.nse_user, password=self.nse_password, database=self.nse_database)
                
            cursor = connection.cursor()  
            
    #            sql_select_Query = """
    #                                WITH cte AS
    #                                (
    #                                   SELECT *,
    #                                   ROW_NUMBER() OVER (PARTITION BY [SymbolName] ORDER BY convert(datetime,[ExpiryDates], 103) asc,ExpiryDates desc) AS rn
    #                                   FROM ( select  SymbolName,ExpiryDates from [dbo].[Symbol] a left join
    #                                   [dbo].[ExpiryDates] b on a.SymbolID = b.SymbolID) T
    #                                )
    #                                SELECT  SymbolName,ExpiryDates
    #                                FROM cte
    #                                WHERE rn = 1
    #                                 """
            sql_select_Query = """
                                select  SymbolName,ExpiryDates from [dbo].[Symbol] a left join
                                [dbo].[ExpiryDates] b on a.SymbolID = b.SymbolID
                                
                                """
            cursor.execute(sql_select_Query)
            records = cursor.fetchall()
            
            df = pd.DataFrame(records)
            print(df)
            
            for i,row in df.iterrows():
                df_list = []
                symbol = row[0]
                expiry = row[1]
                print(expiry)
                url = self.api_url + symbol
                
                r = requests.get(url).json()
                if expiry:
                    ce_values = [data['CE'] for data in r['records']['data'] if 'CE' in data and str(data['expiryDate']).lower() == str(expiry).lower()]
                    pe_values = [data['PE'] for data in r['records']['data'] if 'PE' in data and str(data['expiryDate']).lower() == str(expiry).lower()]
                else: 
                    ce_values = [data['CE'] for data in r['records']['data'] if 'CE' in data]
                    pe_values = [data['PE'] for data in r['records']['data'] if 'PE' in data]
                    
                ce_data = pd.DataFrame(ce_values)
                pe_data = pd.DataFrame(pe_values)
                
                if len(ce_data)!=0 or len(pe_data)!=0:
                    ce_data = ce_data.sort_values(['strikePrice'])
                    pe_data = pe_data.sort_values(['strikePrice'])
                
                    ce_data['type'] = "CE"
                    pe_data['type'] = "PE"
                    
                    df1 = pd.concat([ce_data, pe_data])
                    
                    if len(df_list) > 0 :
                        df1['Time'] = df_list[-1][0]['Time']
                        
                    df1['Time'] =  datetime.now().strftime("%H%M")
                    df1['datetime'] =  datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                    
                    
                    df1 = df1[['strikePrice', 'expiryDate', 'underlying', 'identifier', 'openInterest','changeinOpenInterest','pchangeinOpenInterest', 'totalTradedVolume', 'impliedVolatility', 'lastPrice', 'change','pChange', 'totalBuyQuantity', 'totalSellQuantity', 'bidQty', 'bidprice', 'askQty','askPrice', 'underlyingValue','type','Time','datetime']]
            
                    for i,row in df1.iterrows():
                        sql = "insert into OptionChainData VALUES (" + "%s,"*(len(row)-1) + "%s)"
                        cursor.execute(sql, tuple(row))
                        connection.commit()
                else:
                    pass
                
            connection.close()    
            return df
        except:
            print("No data found")

def main():
    with open("nse.json") as f:
        config = json.loads(f.read())
    
#    nse(config).nsedata()        
    timeframe = config["timeframe"]
    print(timeframe)
    while time(9,15) <= datetime.now().time() <= time(15,30):
        timenow = datetime.now()

        check = True if timenow.minute/timeframe in list(np.arange(0.0,float(60/timeframe))) else False
        if check:
            nextscan = timenow + timedelta(minutes = timeframe)
            nse(config).nsedata()
            waitsecs = int((nextscan - datetime.now()).seconds)
            print("Wait for {0} seconds".format(waitsecs))
            sleep(waitsecs) if waitsecs >0 else sleep(0)
        else:
            print("No data Received")
            sleep(30)

if __name__ == '__main__':
    main()