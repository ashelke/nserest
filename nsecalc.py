# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 15:15:14 2019

@author: AS230412
"""

import json
import logging
import logging.handlers
import threading
import collections
import pyodbc
import pandas as pd
import time 
import os
from werkzeug.wrappers import BaseRequest, BaseResponse, ETagResponseMixin, ETagRequestMixin
from werkzeug.exceptions import HTTPException, BadRequest, MethodNotAllowed, NotFound, Unauthorized
from werkzeug.routing import Map, Rule
from werkzeug.wsgi import SharedDataMiddleware
from wsgiref.handlers import format_date_time
import math
from scipy.stats import norm
from dateutil.parser import parse
from datetime import datetime



# import pwm_dc_power_report

#Matplotlib requires some additional configuration in order for it to run in a
# WSGI environment
os.environ['MPLCONFIGDIR'] = '/tmp'

COOKIE_AGE = 7 * 24 * 60 * 60 #in seconds

#Matplotlib isn't thread safe, create a lock to protect plotting code
_plot_lock = threading.Lock()

#logger for this module
_logger = logging.getLogger(__name__)

def replace_nans(x):
    if isinstance(x, dict):
        return dict((k, replace_nans(v)) for k, v in list(x.items()))
    elif isinstance(x, list) or isinstance(x, tuple):
        return [replace_nans(v) for v in x]
    elif isinstance(x, float) and x != x:
        return None
    else:
        return x

def normdist(cp,sp,dte,iv):
    try:     
        ln = math.log(sp/cp)
        sq = math.sqrt(dte/365)
        fm = ln/((iv/100)*sq)
        dist = norm.cdf(fm)
        return dist
    except ZeroDivisionError:
        return 0

def reformat_date(str, fromformat, toformat):
    return datetime.datetime.strftime(
        datetime.datetime.strptime(str, fromformat), toformat);

def list_to_sql_string(l):
    return '(' + ','.join(('\'' + str(x) + '\'' for x in l)) + ')'

def round_number_list(l, decimals=None):
    return [None if num != num else round(num, decimals) for num in l]


class Response(BaseResponse, ETagResponseMixin):
    pass

class Request(BaseRequest, ETagRequestMixin):
    pass

class DateTimeEncoder(json.JSONEncoder):
    """
    Converts datetime objects into ISO strings
    
    The default JSON encoder doesn't know how to handle datetime objects. This
    class extends the default JSONEncoder class so that it converts all
    datetime objects to ISO 8601 strings which makes it easy to parse in
    JavaScript.
    """
    
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        
        return json.JSONEncoder.default(self, obj)

def RFC1123Encode(val):
    """
    Converts datetime objects into RFC1123 format
    
    This is useful in Response headers that set expiry time
    """
    if isinstance(val, datetime.datetime):
        return format_date_time(time.mktime(val.utctimetuple()))
    else:
        return val


class nse(object):
    
    def __init__(self, config):
        self.nse_host = config["nse_host"]
        self.nse_database = config["nse_database"]
        self.nse_user = config["nse_user"]
        self.nse_password = config["nse_password"]
        #Set up routes
        self.url_map = Map([            
            #Ember-Data REST API
            Rule("/api/", endpoint="hello"),
            Rule("/api/nse/<jsonString>", endpoint="nsecalc"),
            Rule("/api/nse/expiry/", endpoint="expirydd"),
            Rule("/api/nse/symbol/", endpoint="symboldd"),
        ])
        
        # _logger.info("Establishing MongoDB connection")
        # self.conn = pymongo.MongoClient(self.mongodb_uri)
        # _logger.info("MongoDB connection established")
        
        #Ensure that we always close the MongoDB connection    
        # atexit.register(self.conn.close)


    def hello(self, request, **values):
        return Response("Hello, World!")
    
    def symboldd(self, request, **values):
        connection = pyodbc.connect(Driver='{SQL Server}', server=self.nse_host,database=self.nse_database,user=self.nse_user,password=self.nse_password)
            
        cursor = connection.cursor()  

        sql="""select distinct SymbolName from [dbo].[Symbol] """

        cursor.execute(sql)
 
        resultSet = cursor.fetchall()
        connection.close() 
        rowList = []
        for row in resultSet:
            rowDict = {
                'SymbolName': row[0],
                }
            rowList.append(rowDict)
            
        df = pd.DataFrame(rowList, columns=['SymbolName'])
        
        jsonstring =  df.to_dict(orient='record')
        
        response = Response(json.dumps(jsonstring, sort_keys=True, cls=DateTimeEncoder),headers=[('Content-Type','application/json')])
        
        return response
    
    def expirydd(self, request, **values):
        connection = pyodbc.connect(Driver='{SQL Server}', server=self.nse_host,database=self.nse_database,user=self.nse_user,password=self.nse_password)
            
        cursor = connection.cursor()  

        sql="""select a.SymbolName,b.ExpiryDates from [dbo].[Symbol] a left join  [dbo].[ExpiryDates] b
                on a.SymbolID = b.SymbolID
            """

        cursor.execute(sql)
 
        resultSet = cursor.fetchall()
        connection.close() 
        rowList = []
        for row in resultSet:
            rowDict = {
                'SymbolName': row[0],
                'ExpiryDates': row[1],
                }
            rowList.append(rowDict)
            
        df = pd.DataFrame(rowList, columns=['SymbolName','ExpiryDates'])
        
        jsonstring = df.to_dict(orient='record')
        
        response = Response(json.dumps(jsonstring, sort_keys=True, cls=DateTimeEncoder),headers=[('Content-Type','application/json')])
        
        return response
    
    def nsecalc(self, request, **values):
        if request.method == "GET":
            try:
                j = json.loads(values["jsonString"])
                SymbolName    = j['SymbolName']
                dt =  pd.to_datetime(j['ExpiryDates'])
                ExpiryDates = dt.strftime('%Y/%m/%d')
        
                connection = pyodbc.connect(Driver='{SQL Server}', server=self.nse_host,database=self.nse_database,user=self.nse_user,password=self.nse_password)
                    
                cursor = connection.cursor()  
        
                sql="select * from [dbo].[OptionChainData] where underlying = '" + str(SymbolName) + "' and expiryDate = '" + str(ExpiryDates) + "'"
        
                cursor.execute(sql)
         
                resultSet = cursor.fetchall()
                connection.close() 
                rowList = []
                for row in resultSet:
                    rowDict = {
                        'id': row[0],
                        'strikePrice': row[1],
                        'expiryDate': row[2],
                        'underlying': row[3],
                        'identifier': row[4],
                        'openInterest': row[5],
                        'changeinOpenInterest': row[6],
                        'pchangeinOpenInterest': row[7],
                        'totalTradedVolume': row[8],
                        'impliedVolatility': row[9],
                        'lastPrice': row[10],
                        'change': row[11],
                        'pChange': row[12],
                        'totalBuyQuantity': row[13],
                        'totalSellQuantity': row[14],
                        'bidQty': row[15],
                        'bidprice': row[16],
                        'askQty': row[17],
                        'askPrice': row[18],
                        'underlyingValue': row[19],
                        'type': row[20],
                        'Time': row[21],
                        'datetime': row[22],
                        }
                    rowList.append(rowDict)
                    
                df = pd.DataFrame(rowList, columns=['id','strikePrice','expiryDate','underlying','identifier','openInterest','changeinOpenInterest','pchangeinOpenInterest','totalTradedVolume','impliedVolatility','lastPrice','change','pChange','totalBuyQuantity','totalSellQuantity','bidQty','bidprice','askQty','askPrice','underlyingValue','type','Time','datetime'])
                
                grouped = df.groupby(['Time', 'type'])
                premium = []
                for name, group in grouped:
                    largest = group.nlargest(3,['openInterest'])
                    largest=largest[['lastPrice','strikePrice','openInterest','changeinOpenInterest','impliedVolatility','totalTradedVolume']]
                    for index,item in enumerate(zip(largest['lastPrice'],largest['strikePrice'],largest['openInterest'],largest['changeinOpenInterest'],largest['impliedVolatility'],largest['totalTradedVolume'])):
                        premium.append([name[0],name[1],'max'+ str(index+1),item[0],item[1],item[2],item[3],item[4],item[5]])
                premium = pd.DataFrame(premium,columns=['Time','type','maxltp','lastPrice','strikePrice','openInterest','changeinOpenInterest','impliedVolatility','totalTradedVolume'])
            
                df1 = pd.DataFrame(premium)
                ce_max1 = []
                ce_max2 = []
                ce_max3 = []
                pe_max1 = []
                pe_max2 = []
                pe_max3 = []
                
                for index, row in df1.iterrows():
                    if (row['type'] == 'CE' and row['maxltp'] =='max1'):
                        ce_max1.append([row['Time'],row['type'],row['maxltp'],row['lastPrice'],row['strikePrice'],row['openInterest'],row['changeinOpenInterest'],row['impliedVolatility'],row['totalTradedVolume']])
                    elif (row['type'] == 'CE' and row['maxltp'] =='max2'):
                        ce_max2.append([row['Time'],row['type'],row['maxltp'],row['lastPrice'],row['strikePrice'],row['openInterest'],row['changeinOpenInterest'],row['impliedVolatility'],row['totalTradedVolume']])
                    elif (row['type'] == 'CE' and row['maxltp'] =='max3'):
                        ce_max3.append([row['Time'],row['type'],row['maxltp'],row['lastPrice'],row['strikePrice'],row['openInterest'],row['changeinOpenInterest'],row['impliedVolatility'],row['totalTradedVolume']])
                    elif (row['type'] == 'PE' and row['maxltp'] =='max1'):
                        pe_max1.append([row['Time'],row['type'],row['maxltp'],row['lastPrice'],row['strikePrice'],row['openInterest'],row['changeinOpenInterest'],row['impliedVolatility'],row['totalTradedVolume']])
                    elif (row['type'] == 'PE' and row['maxltp'] =='max2'):
                        pe_max2.append([row['Time'],row['type'],row['maxltp'],row['lastPrice'],row['strikePrice'],row['openInterest'],row['changeinOpenInterest'],row['impliedVolatility'],row['totalTradedVolume']])
                    elif (row['type'] == 'PE' and row['maxltp'] =='max3'):
                        pe_max3.append([row['Time'],row['type'],row['maxltp'],row['lastPrice'],row['strikePrice'],row['openInterest'],row['changeinOpenInterest'],row['impliedVolatility'],row['totalTradedVolume']])
                    else:
                        pass
                                  
                df3 =  pd.DataFrame(ce_max1,columns=['Time','type','maxltp','lastPrice','strikePrice','openInterest','changeinOpenInterest','impliedVolatility','totalTradedVolume'])
                df3['decay'] =  round(df3['lastPrice'].sub(df3['lastPrice'].shift()),2)
                df3['decay'].iloc[0]= df3['lastPrice'].iloc[0]
                df3['decayRev'] = round(df3['lastPrice'].iloc[-1] - df3['lastPrice'],2)
                df3 = df3.iloc[1:]
                df3['decayAvg']= round(df3['decay'].mean(),2)
                df3['decayRevSum']= round(df3['decayRev'].sum(),2)
                df3max = df3[df3['Time']==df3['Time'].max()]
                df3max = pd.DataFrame(df3max)
                df3large = df3.nlargest(3,['Time'])
                df3large = df3large[['Time','type','maxltp','changeinOpenInterest','impliedVolatility','totalTradedVolume']]
            
                df4 =  pd.DataFrame(ce_max2,columns=['Time','type','maxltp','lastPrice','strikePrice','openInterest','changeinOpenInterest','impliedVolatility','totalTradedVolume'])
                df4['decay'] =  round(df4['lastPrice'].sub(df4['lastPrice'].shift()),2)
                df4['decay'].iloc[0]= df4['lastPrice'].iloc[0]
                df4['decayRev'] = round(df4['lastPrice'].iloc[-1] - df4['lastPrice'],2)
                df4 = df4.iloc[1:]
                df4['decayAvg']= round(df4['decay'].mean(),2)
                df4['decayRevSum']= round(df4['decayRev'].sum(),2)
                df4max = df4[df4['Time']==df4['Time'].max()]
                df4max = pd.DataFrame(df4max)
                df4large = df4.nlargest(3,['Time'])
                df4large = df4large[['Time','type','maxltp','changeinOpenInterest','impliedVolatility','totalTradedVolume']]
                
                
                
                df5 =  pd.DataFrame(ce_max3,columns=['Time','type','maxltp','lastPrice','strikePrice','openInterest','changeinOpenInterest','impliedVolatility','totalTradedVolume'])
                df5['decay'] =  round(df5['lastPrice'].sub(df5['lastPrice'].shift()),2)
                df5['decay'].iloc[0]= df5['lastPrice'].iloc[0]
                df5['decayRev'] = round(df5['lastPrice'].iloc[-1] - df5['lastPrice'],2)
                df5 = df5.iloc[1:]
                df5['decayAvg']= round(df5['decay'].mean(),2)
                df5['decayRevSum']= round(df5['decayRev'].sum(),2)
                df5max =df5[df5['Time']==df5['Time'].max()]
                df5max = pd.DataFrame(df5max)
                df5large = df5.nlargest(3,['Time'])
                df5large = df5large[['Time','type','maxltp','changeinOpenInterest','impliedVolatility','totalTradedVolume']]
                
                df6 =  pd.DataFrame(pe_max1,columns=['Time','type','maxltp','lastPrice','strikePrice','openInterest','changeinOpenInterest','impliedVolatility','totalTradedVolume'])
                df6['decay'] =  round(df6['lastPrice'].sub(df6['lastPrice'].shift()),2)
                df6['decay'].iloc[0]= df6['lastPrice'].iloc[0]
                df6['decayRev'] = round(df6['lastPrice'].iloc[-1] - df6['lastPrice'],2)
                df6 = df6.iloc[1:]
                df6['decayAvg']= round(df6['decay'].mean(),2)
                df6['decayRevSum']= round(df6['decayRev'].sum(),2)
                df6max = df6[df6['Time']==df6['Time'].max()]
                df6max = pd.DataFrame(df6max) 
                df6large = df6.nlargest(3,['Time'])
                df6large = df6large[['Time','type','maxltp','changeinOpenInterest','impliedVolatility','totalTradedVolume']]
                
                
                df7 =  pd.DataFrame(pe_max2,columns=['Time','type','maxltp','lastPrice','strikePrice','openInterest','changeinOpenInterest','impliedVolatility','totalTradedVolume'])
                df7['decay'] =  round(df7['lastPrice'].sub(df7['lastPrice'].shift()),2)
                df7['decay'].iloc[0]= df7['lastPrice'].iloc[0]
                df7['decayRev'] = round(df7['lastPrice'].iloc[-1] - df7['lastPrice'],2)
                df7 = df7.iloc[1:]
                df7['decayAvg']= round(df7['decay'].mean(),2)
                df7['decayRevSum']= round(df7['decayRev'].sum(),2)
                df7max = df7[df7['Time']==df7['Time'].max()]
                df7max = pd.DataFrame(df7max) 
                df7large = df7.nlargest(3,['Time'])
                df7large = df7large[['Time','type','maxltp','changeinOpenInterest','impliedVolatility','totalTradedVolume']]
                
                
                df8 =  pd.DataFrame(pe_max3,columns=['Time','type','maxltp','lastPrice','strikePrice','openInterest','changeinOpenInterest','impliedVolatility','totalTradedVolume'])
                df8['decay'] =  round(df8['lastPrice'].sub(df8['lastPrice'].shift()),2)
                df8['decay'].iloc[0]= df8['lastPrice'].iloc[0]
                df8['decayRev'] = round(df8['lastPrice'].iloc[-1] - df8['lastPrice'],2)
                df8 = df8.iloc[1:]
                df8['decayAvg']= round(df8['decay'].mean(),2)
                df8['decayRevSum']= round(df8['decayRev'].sum(),2)
                df8max = df8[df8['Time']==df8['Time'].max()]
                df8max = pd.DataFrame(df8max) 
                df8large = df8.nlargest(3,['Time'])
                df8large = df8large[['Time','type','maxltp','changeinOpenInterest','impliedVolatility','totalTradedVolume']]
            
                
                df9 = df3large.append([df4large, df5large,df6large,df7large,df8large])
                df10 =  [pd.DataFrame(y) for x, y in df9.groupby('Time', as_index=False)]
                df10max1 = df10[2][['type','maxltp','changeinOpenInterest','impliedVolatility','totalTradedVolume']]
                df10max1.columns = ['typemax1','maxltpmax1','changeinOpenInterestmax1', 'impliedVolatilitymax1','totalTradedVolumemax1']
                df10max1.reset_index(inplace = True)
                df10max2 = df10[1][['type','maxltp','changeinOpenInterest','impliedVolatility','totalTradedVolume']]
                df10max2.columns = ['typemax2','maxltpmax2','changeinOpenInterestmax2', 'impliedVolatilitymax2','totalTradedVolumemax2']
                df10max2.reset_index(inplace = True)
                df10max3 = df10[0][['type','maxltp','changeinOpenInterest','impliedVolatility','totalTradedVolume']]
                df10max3.columns = ['typemax3','maxltpmax3','changeinOpenInterestmax3', 'impliedVolatilitymax3','totalTradedVolumemax3']
                df10max3.reset_index(inplace = True)
                
                df10max1 = pd.DataFrame(df10max1)
                df10max1 = df10max1[['changeinOpenInterestmax1','impliedVolatilitymax1','totalTradedVolumemax1']]
                df10max2 = pd.DataFrame(df10max2) 
                df10max2 = df10max2[['changeinOpenInterestmax2','impliedVolatilitymax2','totalTradedVolumemax2']]
                df10max3 = pd.DataFrame(df10max3) 
                df10max3 = df10max3[['changeinOpenInterestmax3','impliedVolatilitymax3','totalTradedVolumemax3']]
                
                df10max = pd.concat([df10max1,df10max2,df10max3], axis=1, join='inner')
            
                df10max = df10max[['changeinOpenInterestmax1','impliedVolatilitymax1','totalTradedVolumemax1','changeinOpenInterestmax2','impliedVolatilitymax2','totalTradedVolumemax2','changeinOpenInterestmax3','impliedVolatilitymax3','totalTradedVolumemax3']]
                df10max = pd.DataFrame(df10max)
                df11decay = pd.concat([df3max,df4max,df5max,df6max,df7max,df8max])
                df11decay =pd.DataFrame(df11decay,columns=['Time','type','maxltp','lastPrice','strikePrice','openInterest','decay','decayAvg','decayRev','decayRevSum'])
                df11decay.reset_index(inplace = True)
                
                df11 = pd.concat([df11decay,df10max], axis=1, join='inner')
                
                uvCurr = df['underlyingValue'].tail(1).item()
                maxTime = df['Time'].tail(1).item()
                
                maxDatetime = df['datetime'].tail(1).item()
            
                dt = pd.to_datetime((df['expiryDate'].tail(1).item()))
                
                expiryDate = dt.strftime('%m/%d/%Y')
                
                NowDate  = datetime.today().strftime("%m/%d/%Y")
                
                date_format = "%m/%d/%Y"
                a = datetime.strptime(NowDate, date_format).date()
                b = datetime.strptime(expiryDate, date_format).date()
                dte = abs((b - a).days)
                
                for i, row in df11.iterrows():
                    strikePrice =  df11['strikePrice'][i]
                    typeData  = df11['type'][i]
                    data_at_maxtime = df[(df["type"]== typeData) & (df["Time"]== maxTime)]
                    atm_iv =  data_at_maxtime.iloc[(data_at_maxtime['strikePrice']-uvCurr).abs().argsort()[:1]]
                    atm_iv_val = atm_iv['impliedVolatility'].tail(1).item()
                    df11.loc[i,'atm_iv'] = atm_iv_val
                    if typeData == 'PE':
                        df11.loc[i,'winningProb'] = round((normdist(uvCurr,strikePrice,dte,atm_iv_val) * 100),2)
                    else:
                        df11.loc[i,'winningProb'] = round(1-(normdist(uvCurr,strikePrice,dte,atm_iv_val) * 100),2)
                
                atm_call_iv = df11[(df11["type"]== 'CE')]
                atm_put_iv  = df11[(df11["type"]== 'PE')]
            
                df11finalAvg = (df11[['type','decayAvg','decayRevSum']].groupby(['type']).mean().reset_index()).round(2)
                
                
                jsonstring = {'data': df11.to_dict(orient='record'),'dataAvg':df11finalAvg.to_dict(orient='record'),'uvCurr': uvCurr,'atm_call_iv':atm_call_iv['atm_iv'].tail(1).item(),'atm_put_iv':atm_put_iv['atm_iv'].tail(1).item(),'maxDatetime':maxDatetime}
        
                response = Response(json.dumps(jsonstring, sort_keys=True, cls=DateTimeEncoder),headers=[('Content-Type','application/json')])
                return response
            except:
                jsonstring = {}
                response = Response(json.dumps(jsonstring, sort_keys=True, cls=DateTimeEncoder),headers=[('Content-Type','application/json')])
                return response

    def dispatch_request(self, request):
        """
        This appliction's dispatcher; how we resolve different URLs to methods
        """ 
        
        #Get an adaptor from the url map we defined in __init__
        adapter = self.url_map.bind_to_environ(request.environ)
        
        try:
            #See if the URL matches a defined route
            endpoint, values = adapter.match()
            
            #Call the method that has the same name as the route's end point
            response = getattr(self, endpoint)(request, **values)
            response.headers.add("Access-Control-Allow-Origin", "*")
            response.headers.add("Access-Control-Expose-Headers", "Content-Disposition")
            
            return response
        except HTTPException as e:
            return e
        except:
            _logger.exception("An exception was raised while serving the "
                              "request: %s", request)
            
            #return self.error_500()
            raise
    
    def wsgi_app(self, environ, start_response):
        """
        Method that follows the wsgi convention
        """
        
        request = Request(environ)
        response = self.dispatch_request(request)
        return response(environ, start_response)

    def __call__(self, environ, start_response):
        return self.wsgi_app(environ, start_response) 
    
    
#def main():
#    nsecalc()
    
if __name__ == '__main__':
    from werkzeug.serving import run_simple
    
    formatter = logging.Formatter("%(asctime)s %(levelname)s - %(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    
    _logger.setLevel(logging.DEBUG)
    _logger.addHandler(handler)
    
    with open("nse.json") as f:
        config = json.loads(f.read())

    if 'exception_receiver' in config:
        smtp_handler = logging.handlers.SMTPHandler(
            mailhost=("app", 25),
            subject="Exception")
        smtp_handler.setFormatter(formatter)
        smtp_handler.setLevel(logging.ERROR)
        # hack to make the email handler work
        # or else it will timeout always
        smtp_handler._timeout = None
        _logger.addHandler(smtp_handler)    

    app = nse(config)
    
    #Configure static serving for when running in debug mode. In production, 
    # this should be configured on the Apache side.
    static_mapping = {}
    
    app.wsgi_app = SharedDataMiddleware(app.wsgi_app, static_mapping, cache_timeout=3600)
    
    run_simple("127.0.0.1", 4444, app, use_debugger=True, use_reloader=True)