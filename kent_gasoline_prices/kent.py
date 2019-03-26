# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 16:12:33 2019

@author: mossgran
"""


import requests
from requests.auth import HTTPBasicAuth
import io
import pandas as pd
import re
import os
import datetime
import numpy as np
import sys
from sqlalchemy import create_engine
import urllib

#%%

#params = urllib.parse.quote_plus(r'DRIVER={SQL Server Native Client 11.0};SERVER=dSQL22CAP;DATABASE=EnergyData;Trusted_Connection=yes')
#conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
#conn = create_engine(conn_str)
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


def prod_oil(h1,h2):
   
    products = [{'url_file':'Crude/Crude/DAILY/YYYY/Crude_Crude_DAILY_YYYY.EXT', 
             'headers':(h1,h2), 'dates':[0], 'product':'Crude', 'type':'$CDN / m3','frequency':'Daily'},                
               ]
    return (products)

#This function captures the structure of the kent website, and contains all the neccecary data to generate download links automatically
#TODO add the option to pass in the frequency, instead of having it hard coded into the data strucure
def prod(heading):
    
    products = [{'url_file':'Unleaded/Retail%20(Incl.%20Tax)/DAILY/YYYY/Unleaded_Retail%20(Incl.%20Tax)_DAILY_YYYY.EXT', 
             'headers':(heading), 'dates':[0], 'product':'Unleaded', 'type':'Retail','frequency':'Daily'}, 
                
           {'url_file':'Midgrade/Retail%20(Incl.%20Tax)/DAILY/YYYY/Midgrade_Retail%20(Incl.%20Tax)_DAILY_YYYY.EXT', 
            'headers':(heading), 'dates':[0], 'product':'Midgrade','type':'Retail','frequency':'Daily'},
                
           {'url_file':'Premium/Retail%20(Incl.%20Tax)/DAILY/YYYY/Premium_Retail%20(Incl.%20Tax)_DAILY_YYYY.EXT', 
            'headers':(heading), 'dates':[0], 'product':'Premium','type':'Retail','frequency':'Daily'},
                
           {'url_file':'Diesel/Retail%20(Incl.%20Tax)/DAILY/YYYY/Diesel_Retail%20(Incl.%20Tax)_DAILY_YYYY.EXT', 
            'headers':(heading), 'dates':[0], 'product':'Diesel', 'type':'Retail','frequency':'Daily'},
                
           {'url_file':'Unleaded/Retail%20(Excl.%20Tax)/DAILY/YYYY/Unleaded_Retail%20(Excl.%20Tax)_DAILY_YYYY.EXT', 
            'headers':(heading), 'dates':[0], 'product':'Unleaded', 'type':'Retail excl tax','frequency':'Daily'},
                
           {'url_file':'Midgrade/Retail%20(Excl.%20Tax)/DAILY/YYYY/Midgrade_Retail%20(Excl.%20Tax)_DAILY_YYYY.EXT', 
            'headers':(heading), 'dates':[0], 'product':'Midgrade', 'type':'Retail excl tax','frequency':'Daily'},
                
           {'url_file':'Premium/Retail%20(Excl.%20Tax)/DAILY/YYYY/Premium_Retail%20(Excl.%20Tax)_DAILY_YYYY.EXT', 
            'headers':(heading), 'dates':[0], 'product':'Premium', 'type':'Retail excl tax','frequency':'Daily'},
                
           {'url_file':'Diesel/Retail%20(Excl.%20Tax)/DAILY/YYYY/Diesel_Retail%20(Excl.%20Tax)_DAILY_YYYY.EXT', 
            'headers':(heading), 'dates':[0], 'product':'Diesel', 'type':'Retail excl tax','frequency':'Daily'},
                
           {'url_file':'Unleaded/Wholesale/DAILY/YYYY/Unleaded_Wholesale_DAILY_YYYY.EXT', 
            'headers':(heading), 'dates':[0], 'product':'Unleaded', 'type':'Wholesale','frequency':'Daily'},
                
           {'url_file':'Midgrade/Wholesale/DAILY/YYYY/Midgrade_Wholesale_DAILY_YYYY.EXT', 
            'headers':(heading), 'dates':[0], 'product':'Midgrade', 'type':'Wholesale','frequency':'Daily'},
                
           {'url_file':'Premium/Wholesale/DAILY/YYYY/Premium_Wholesale_DAILY_YYYY.EXT', 
            'headers':(heading), 'dates':[0], 'product':'Premium', 'type':'Wholesale','frequency':'Daily'},
                
           {'url_file':'Diesel/Wholesale/DAILY/YYYY/Diesel_Wholesale_DAILY_YYYY.EXT', 
            'headers':(heading), 'dates':[0], 'product':'Diesel', 'type':'Wholesale','frequency':'Daily'},
                
            # add furnace oil
           {'url_file':'Furnace%20Oil/Retail%20(Incl.%20Tax)/WEEKLY/YYYY/Furnace%20Oil_Retail%20(Incl.%20Tax)_WEEKLY_YYYY.EXT', 
            'headers':(heading), 'dates':[0], 'product':'Furnace Oil', 'type':'Retail','frequency':'Weekly'},
                
           {'url_file':'Furnace%20Oil/Retail%20(Excl.%20Tax)/WEEKLY/YYYY/Furnace%20Oil_Retail%20(Excl.%20Tax)_WEEKLY_YYYY.EXT', 
            'headers':(heading), 'dates':[0], 'product':'Furnace Oil', 'type':'Retail excl tax','frequency':'Weekly'},
            
           {'url_file':'Furnace%20Oil/Wholesale/DAILY/YYYY/Furnace%20Oil_Wholesale_DAILY_YYYY.EXT', 
            'headers':(heading), 'dates':[0], 'product':'Furnace Oil', 'type':'Wholesale','frequency':'Weekly'},
                
               ]
    
    return (products)


#%%
#here are the new function to build the excel or html link:
    
#build a function that gets all of the links first, and then requests them...
    
#exclude furnace oil for now. These lists contain the structure of the kent website
product_list = ['Unleaded','Midgrade','Premium','Diesel']
report_list = ['Retail','Retail excl tax','Wholesale']
frequency_list = ['Daily','Weekly','Monthly']
    
def gather_years(start = 2000):
    current = datetime.datetime.now()
    current_year = current.year
    year_list = [i for i in range(start,current_year+1)]
    return(year_list)      
    
example = 'Unleaded/Retail%20(Incl.%20Tax)/DAILY/YYYY/Unleaded_Retail%20(Incl.%20Tax)_DAILY_YYYY.EXT'
def kent_links(product_list,report_list,frequency_list, year_list,base_url='https://charting.kentgroupltd.com/Charting/DownloadExcel?file=/WPPS/'):
    
    for product in product_list:
        for report in report_list:
            for frequency in frequency_list:
                for year in year_list:
                    
                    if report != 'Wholesale':
                    
                        url = 'PRODUCT/REPORT%20(INCLUDETAX.%20Tax)/FREQUENCY/YYYY/PRODUCT_REPORT%20(INCLUDETAX.%20Tax)_FREQUENCY_YYYY.EXT'
                  
                        if report == 'Retail':
                            url = url.replace('INCLUDETAX','Incl')
                        else:
                            url = url.replace('INCLUDETAX','Excl')
                    else:
                        url = 'PRODUCT/REPORT/FREQUENCY/YYYY/PRODUCT_REPORT_FREQUENCY_YYYY.EXT'
                    
                    
                    url = url.replace('PRODUCT',product)
                    url = url.replace('REPORT',report)
                    url = url.replace('FREQUENCY',frequency)
                    url = url.replace('YYYY',str(year))
                    
                    print(url)    
                    
                    #up to here takes care of everything except for the link extension
                 
    return(None)


#%%    
year_list = gather_years()                   
x = kent_links(product_list,report_list,frequency_list,year_list)


#%%


#if the year is below 2016, automatically send to html
#break the function into two functions. one for excel and one for html
def get_kent(url_base,p,year):
    
    if p['type'] in ['Retail','Retail excl tax',] and year < 2016: #this is to reduce unnececary requests
        pass
    else:
    
        sheet_name = 'Prices'   
        ret = False    
        url_ext = p['url_file'].replace('YYYY', str(year))
        url_ext = url_ext.replace('EXT', 'xlsx')
        url_excel = url_base + url_ext
        r = requests.get(url_excel, allow_redirects=True, stream=True, headers=headers)
        #print(url_excel, r.status_code)
        if r.status_code == requests.codes.ok:
        
            df_x = pd.read_excel(io.BytesIO(r.content), sheet_name=sheet_name, 
                            header=p['headers'], parse_dates=p['dates'])
            ret = True
            if 'Average' in df_x.columns:
                df_x.drop('Average',axis=1,inplace = True)
        
            print('got excel for'+p['product']+p['type']+' '+str(year))
            return(df_x)
        
        elif r.status_code != requests.codes.ok:
        
            url_ext = p['url_file'].replace('YYYY', str(year))
            url_ext = url_ext.replace('EXT', 'htm')
            url_html = 'https://charting.kentgroupltd.com/WPPS/' + url_ext
            b = requests.get(url_html, allow_redirects=True, stream=True, headers=headers)
            #print(url_html, b.status_code)
            if b.status_code == requests.codes.ok:
            
                df_h = pd.read_html(url_html, header = p['headers'],parse_dates=p['dates'])[0]
           
                ret = True
                if p['product'] == 'Crude':
                    df_h=df_h.reset_index(drop=True)
                else:
                    df_h.set_index('Unnamed: 0',inplace=True)
                
                if 'Average' in df_h.columns:
                    df_h.drop('Average',axis=1,inplace = True)
                print('got html for'+p['product']+p['type']+' '+str(year))
                return(df_h)
            else:
                pass
    
        if not ret:
            print('could not get data for'+ p['product']+p['type']+' '+str(year))
            print(url_excel,url_html)
            
            
def rpp_scrape(test):
    
    rpp = pd.DataFrame()

    url_base = 'https://charting.kentgroupltd.com/Charting/DownloadExcel?file=/WPPS/'
    
    if test:
        years=[2015,2018]
    else:
        years = [2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019]
    
    rpp_data = []

    for y in years:
    
        #this is to accomodate for the different file format in 2016. 2016 data has different headers
        if y == 2016:
            products = prod(0)
        else:
            products = prod(2)
    
        for product in products:
        
            try:
                #try to get the excel and then the HTML
                
                df = get_kent(url_base,product,y)
                 
                if product['product'] in ['Unleaded','Midgrade', 'Premium','Diesel','Furnace Oil']:
            
                    if product['type'] in ['Retail', 'Retail excl tax']:
                    
                        df['Location'] = df.index
                        df = df.reset_index(drop=True)
                        df = pd.melt(df, id_vars=['Location'], var_name = 'Date_1', value_name = 'Price')
                        df['Year'] = y
                        df['Product'] = product['product']
                        df['Type'] = product['type']
                        df['Frequency'] = product['frequency']
                        df['Date'] = str(y)+'/'+df['Date_1']
                        df = df.drop('Date_1',axis=1)
                        rpp_data.append(df)
                
            
                    elif product['type'] in ['Wholesale']:
    
                        df['Date'] = df.index
                        df = df.reset_index(drop=True)
                        df = pd.melt(df, id_vars=['Date'], var_name = 'Location', value_name = 'Price')
                        df['Year'] = y
                        df['Product'] = product['product']
                        df['Type'] = product['type']
                        df['Frequency'] = product['frequency']
                        rpp_data.append(df)
        
            except:
                pass
                           
    # concat the data when all the columns are same
    rpp = pd.concat(rpp_data, axis=0, sort=False, ignore_index=True)
    rpp['Date'] = pd.to_datetime(rpp['Date'])
    rpp = rpp.dropna(axis=0)
    
    return(rpp)
    
    
def oil_scrape(test):
    
    oil = pd.DataFrame()

    url_base = 'https://charting.kentgroupltd.com/Charting/DownloadExcel?file=/WPPS/'
    
    if test:
        years=[2001,2017]
    else:
        years = [2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019]
    
    oil_data = []

    for y in years:
        
        products = prod_oil(2,3)
    
        for product in products:
        
            try:
                df = get_kent(url_base,product,y)
    
                if y >= 2017:
                    
                    df=df.reset_index(drop=False)
                    df = pd.melt(df,id_vars = ['index'],value_name='Price')
                    df['Units >'] = df['Units >'].replace("Unnamed: 0_level_1",np.nan)
                    df.rename(columns={'index': 'Date', 'Date': 'Product', 'Units >': 'Units'}, inplace=True)
                
                else:
                    
                    df=df.reset_index(drop=True)
                    df['Date'] = df['DATE']['Units>']
                    df.drop(['DATE'], axis=1,inplace=True)
                    df = pd.melt(df, id_vars = ['Date'],value_name='Price')
                    df['variable_1'] = df['variable_1'].replace("Unnamed: 1_level_1",np.nan)
                    df.rename(columns={'Date': 'Date', 'variable_0': 'Product', 'variable_1': 'Units'}, inplace=True)
        
                oil_data.append(df)
            
            except:
                pass
            
    oil = pd.concat(oil_data, axis=0,sort=False, ignore_index=True)
    oil['Date'] = pd.to_datetime(oil['Date'])
    
    return(oil)
    

#this is the main program    
def kent_group(rpp,crude):
    
    if rpp and not crude:
        petroleum = rpp_scrape(test = True)
        return (petroleum)
    
    elif crude and not rpp:
        crude_oil = oil_scrape(test=True)
        return(crude_oil)
    
    elif rpp and crude:
        petroleum = rpp_scrape(test = True)
        crude_oil = oil_scrape(test=True)
        return(petroleum,crude_oil)
    else:
        
        print('no data')
        
#run this line to scrape the data       
#p,c = kent_group(rpp=True,crude=True)

#%%

def years(start = 2000):
    current = datetime.datetime.now()
    current_year = current.year
    year_list = [i for i in range(start,current_year+1)]
    return(year_list)    
         
        
#%%
x = years()
print(x)




