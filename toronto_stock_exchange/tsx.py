#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 13 17:14:46 2019

@author: grant
"""

import numpy as np
import pandas as pd
import datetime
import pandas_datareader.data as web
import requests
import io
import json
from datetime import datetime, timedelta
import networkx as nx
import matplotlib.pyplot as plt
%matplotlib inline
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


def get_constituents(tsx_url):
    r = requests.get(url, allow_redirects=True, stream=True, headers=headers)
    if r.status_code == requests.codes.ok:
        s = r.content
    
    df=pd.read_csv(io.BytesIO(s), header = 3, error_bad_lines=False)
    df['yahoo'] = df['Symbol']+'.TO'
    return(df)
    
    
def yahoo_data(df,start, end = datetime.now()):
    
    stock_list = []
    index_dict = {}
    
    success, sucess_list = 0,[]
    failure, failure_list = 0,[]
    
    year = start[0]
    month = start[1]
    day = start[2]
    pop_list = []
    
    if end.date() != datetime.now().date():
        year_end = end[0]
        month_end = end[1]
        day_end = end[2]
        end = datetime(year_end, month_end, day_end)
        
    for index, row in df.iterrows():
        index_dict[row['Symbol']] = pd.DataFrame()

        start = datetime(year, month, day)
        
        try:
            
            index_dict[row['Symbol']] = web.DataReader(row['yahoo'], 'yahoo', start, end)
            idx = pd.date_range(start, end)
            index_dict[row['Symbol']] = index_dict[row['Symbol']].reindex(idx, fill_value=np.nan)
            index_dict[row['Symbol']]['Date'] = index_dict[row['Symbol']].index
            index_dict[row['Symbol']]['Symbol'] = row['Symbol']
            index_dict[row['Symbol']]['Company'] = row['Constituent Name']
            stock_list.append(index_dict[row['Symbol']])
            #print('got data for symbol:'+str(row['Symbol']))
            sucess = success+1 #check how many stocks the datareader was able to get
            sucess_list.append(row['Symbol']) #add stock symbol to list if the datareader can get it
            #print(row['Constituent Name'])
            
        except:
            failure = failure+1
            failure_list.append(row['Symbol'])
            #print('couldnt get data for symbol:'+str(row['Symbol']))
    
    tsx_constituents = pd.concat(stock_list, axis=0, sort=False, ignore_index=True)
    success_rate = sucess/(failure+sucess)
        
    return(tsx_constituents, success_rate,sucess_list,failure_list) 


start = [2015,1,1]
#default end is today
#end = [2017,1,1]

url = 'https://web.tmxmoney.com/constituents_data.php?index=^TSX&index_name=S%26P%2FTSX+Composite+Index'
tmx = get_constituents(tsx_url = url)
tsx_data, sucess_rate, sucess, failure = yahoo_data(tmx,start)