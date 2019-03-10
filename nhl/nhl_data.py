#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import os
import datetime
import numpy as np
import dateutil
import json
import sys
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0'}
#%%

def replace_dates(input_start,string_start,input_end,string_end,link):
    #all lists should be the same length
    for i in range(0,len(input_start)):
        link = link.replace(string_start[i],input_start[i])
        link = link.replace(string_end[i],input_end[i])
    
    return(link)

#%%
def games(var_link,start_date,end_date):
    start = ['SYYYY','SMM','SDD']
    end = ['EYYYY','EMM','EDD']
    link = replace_dates(input_start = start_date, string_start = start, input_end = end_date, string_end = end, link=var_link)
    r = requests.get(link, allow_redirects=True, stream=True, headers=headers)
    df = pd.DataFrame(r.json()['data'])
    df['gameDate'] = pd.to_datetime(df['gameDate'])
    df.sort_values(by=['gameDate'],inplace=True)
    return(df)
    
#%%
games_link = 'http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=teamsummary&sort=[{%22property%22:%22points%22,%22direction%22:%22DESC%22},{%22property%22:%22wins%22,%22direction%22:%22DESC%22}]&cayenneExp=gameDate%3E=%22SYYYY-SMM-SDD%22%20and%20gameDate%3C=%22EYYYY-EMM-EDD%22%20and%20gameTypeId=2'
start_date = ['2018','10','04']
end_date = ['2019','05','16']

hockey_games = games(games_link, start_date, end_date)
#%%
