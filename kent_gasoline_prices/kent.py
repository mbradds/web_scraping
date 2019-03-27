# -*- coding: utf-8 -*-
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
import time
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
import scraping as sc
#%%
#build a function that gets all of the links first, and then requests them...
    
#exclude furnace oil for now. These lists contain the structure of the kent website
    
def gather_years(start = 2000):
    current = datetime.datetime.now()
    current_year = current.year
    year_list = [i for i in range(start,current_year+1)]
    return(year_list)      

  
def kent_links(product_list,report_list,frequency_list, year_list):
    
    url_list = []
    url_dict = {}
    
    #these for loops generate every possible combination from the drop down menues of the kent website.
    for product in product_list:
        for report in report_list:
            for frequency in frequency_list:
                for year in year_list:
                    
                    if report in ['Retail','Retail excl tax'] and year >= 2016 and frequency == 'Daily' or report == 'Wholesale': 
                    
                        if report != 'Wholesale':
                        
                            url = 'PRODUCT/REPORT%20(INCLUDETAX.%20Tax)/FREQUENCY/YYYY/PRODUCT_REPORT%20(INCLUDETAX.%20Tax)_FREQUENCY_YYYY.EXT'
                      
                            if report == 'Retail':
                                url = url.replace('INCLUDETAX','Incl')
                            else:
                                url = url.replace('INCLUDETAX','Excl')
                                #the report part of the link is the same for retail, and retail excluding tax
                                url = url.replace('REPORT','Retail')
                        else:
                            url = 'PRODUCT/REPORT/FREQUENCY/YYYY/PRODUCT_REPORT_FREQUENCY_YYYY.EXT'
                        
                        #replace the url with proper specification
                        url = url.replace('PRODUCT',product)
                        url = url.replace('REPORT',report)
                        url = url.replace('FREQUENCY',frequency)
                        url = url.replace('YYYY',str(year))
                        
                        #handle the file extension
                        if year >= 2015:
                            url = url.replace('EXT','xlsx')
                            base_url = 'https://charting.kentgroupltd.com/Charting/DownloadExcel?file=/WPPS/'
                        else:
                            url = url.replace('EXT','htm')
                            base_url = 'https://charting.kentgroupltd.com/WPPS/'
                        
                        url = base_url+url
                        #url_list.append(url)
                        url_dict.update({'url':url,'product':product,'report':report,'frequency':frequency,'year':year})
                        url_list.append(dict(url_dict)) #adding dict() wrapper forces it to append, instead of overwrite existing list
                    
                    else:
                        #the year is out of range
                        pass
                 
    return(url_list)

#%%    

def get_kent_excel(link, head):
    sheet_name = 'Prices'
    r = requests.get(link, allow_redirects=True, stream=True, headers=headers)
    
    if r.status_code == requests.codes.ok:
        
            df_x = pd.read_excel(io.BytesIO(r.content), sheet_name=sheet_name, header = head, parse_dates=[0])

            if 'Average' in df_x.columns:
                df_x.drop('Average',axis=1,inplace = True)
                
            #print('got excel for '+str(link))
            #put logger here
            return(df_x)
    else:
        return(None)

def get_kent_html(link):
    b = requests.get(link, allow_redirects=True, stream=True, headers=headers)
    if b.status_code == requests.codes.ok:
            
        df_h = pd.read_html(link, header = 2,parse_dates=[0])[0]           
                
        if 'Average' in df_h.columns:
            df_h.drop('Average',axis=1,inplace = True)
            #print('got html for'+p['product']+p['type']+' '+str(year))
            #put logger here
        return(df_h)
    
#%%
#test the requests

def request_df(links):
    
    for link in links:
        url, product, report, frequency, year = link['url'], link['product'], link['report'], link['frequency'], link['year']
        
        if year >= 2015:
            #this accomodates the different file format in 2016
            if year == 2016:
                df = get_kent_excel(url, head=0)
            else:
                df = get_kent_excel(url, head=2)
        else:
            df = get_kent_html(url)
        
        #reindex the dataframe
        df.reset_index(inplace=True)
          
        if report != 'Wholesale':
            df = df[df['index']!= 'S-Simple V-Volume Weighted P-Population Weighted']
            df = df.dropna(axis=0, how = 'all')
            df = df.dropna(axis=1, how = 'all')
            df = pd.melt(df, id_vars=['index'], var_name = 'Date', value_name = 'Price')
            df.rename(index=str, columns={"index": "Region"},inplace=True)
            #convert date (month/day) to date (month/day/year)
            df['Date'] = df['Date']+'/'+str(year)

        else:
            df = pd.melt(df, id_vars=['index'], var_name = 'Region', value_name = 'Price')
            df = df.dropna(axis=0, how = 'any')
            df.rename(index=str, columns={"index": "Date"}, inplace = True)
            
        #these columns are common regardless of report type
        df['Year'] = int(year)
        df['Product'] =str(product)
        df['Report'] = str(report)
        df['Frequency'] = str(frequency)
        
        #convert the dtypes
        df['Date'] = pd.to_datetime(df['Date'], errors = 'coerce')
        df['Price'] = pd.to_numeric(df['Price'],errors = 'coerce')
        
        #add the url. This can be used as a lookup to avoid unnececary database/csv inserts
        df['url'] = str(url)
        
    return(df)
    
    
#%%
#gather all of the dataframes in a try

def gather_prices(link_structure,logger,connection,insert_obj):
    
    for ls in link_structure:
        
        try:
            df = request_df([ls])
            print(df.head())
            time.sleep(2)
            insert_obj.insert_csv(df,logger)
            print('got csv'+str(ls))
            insert_obj.insert_database(df,'kent',logger,connection)
            print('got db'+str(ls))
        except:
            print('failed'+str(ls))
    
#%%
   
#insert the data into the csv/db.
#each df should be inserted after it is scraped
#here are the steps:
#1 scrape the df
#2 check if it is already saved

data_file = 'kent.csv'
direc = r'/home/grant/Documents/web_scraping/kent_gasoline_prices'
kent = sc.scrape(direc)  
logger = kent.scrape_logger('kent.log')
connection = kent.scrape_database('database.json',logger)
ins = sc.insert(direc, csv_path = data_file) 
#%%
         
product_list = ['Unleaded','Midgrade','Premium','Diesel']
report_list = ['Retail','Retail excl tax','Wholesale']
frequency_list = ['Daily','Weekly','Monthly']   

year_list = gather_years()   
#year_list = [year_list[-1]]            
links = kent_links(product_list,report_list,frequency_list,year_list)
#links_test = links[:1]
gather_prices(links,logger,connection,ins)
  
#%%

#midgrade wholsale only has 2016 and up!
 