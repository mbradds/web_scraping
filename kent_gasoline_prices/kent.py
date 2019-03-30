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
                    
                    if (report in ['Retail','Retail excl tax'] and year >= 2016 and frequency == 'Daily') or \
                    (report == 'Wholesale' and product == 'Midgrade' and year >= 2016) or \
                    (report=='Wholesale' and product in ['Unleaded','Premium','Diesel']) or\
                    (report in ['Retail','Retail excl tax'] and frequency in ['Weekly','Monthly']):
                    
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
                        if year > 2015:
                            url = url.replace('EXT','xlsx')
                            base_url = 'https://charting.kentgroupltd.com/Charting/DownloadExcel?file=/WPPS/'
                        elif year == 2015:
                            url = url.replace('EXT','xls')
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

def get_kent_html(link,head):
    b = requests.get(link, allow_redirects=True, stream=True, headers=headers)
    if b.status_code == requests.codes.ok:
            
        df_h = pd.read_html(link, header = head,parse_dates=[0])[0]           
                
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
            if year in [2016,2015]:
                df = get_kent_excel(url, head=0)
            else:
                df = get_kent_excel(url, head=2)
        else:
            if year <= 2000:
                df = get_kent_html(url, head=2)
            else:
                df = get_kent_html(url,head=2)
            
            
        #get rid of garbage rows
        try:
            df = df[df[df.columns[0]]!= str(df.columns[0])]
            df = df[df[df.columns[0]]!= 'S-Simple V-Volume Weighted P-Population Weighted']
            df = df[df[df.columns[0]]!= 'S-Simple  V-Volume Weighted P-Population Weighted To print, copy contents to, and print from a spreadsheet']
        
        except:
            None
            
        
        if report != 'Wholesale' and frequency != 'Monthly':

            df = df.dropna(axis=0, how = 'all')
            df = df.dropna(axis=1, how = 'all')
            df = pd.melt(df, id_vars=[df.columns[0]], var_name = 'Date', value_name = 'Price')
            df.rename(index=str, columns={str(df.columns[0]): "Region"},inplace=True)
            #convert date (month/day) to date (month/day/year)
            df['Date'] = df['Date']+'/'+str(year)

        elif report == 'Wholesale' and frequency == 'Daily':
            
            df = pd.melt(df, id_vars=[str(df.columns[0])], var_name = 'Region', value_name = 'Price')
            df = df.dropna(axis=0, how = 'any')
            #df['Date'] = df['Date']+'/'+str(year)
            df.rename(columns={str(df.columns[0]): "Date"}, inplace = True)
        
        elif report == 'Wholesale' and frequency == 'Weekly':
            df = pd.melt(df, id_vars = [df.columns[0]], var_name='Date',value_name='Price')
            df['Date'] = df['Date']+'/'+str(year)
            df.rename(columns={str(df.columns[0]): "Region"}, inplace = True)
        
        elif frequency == 'Monthly':
            df = pd.melt(df, id_vars=str(df.columns[0]), var_name = 'Date', value_name = 'Price')
            df['Date'] = [x.split('/')[0]+'/'+'1'+'/'+str(year) for x in df['Date']]
            df.rename(index=str, columns={str(df.columns[0]): "Region"},inplace=True)

        else:
            None
        
            
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
#TODO: look at recording the length of the scraped data, and then recording an error if the same length isnt saved
def gather_prices(link_structure,logger,connection,insert_obj,verify):
    #this lambda gets the number of rows that have been saved in csv or database
    
    saved_length = lambda x : x.shape[0] if not x.empty else 0
    
    for ls in link_structure:
                
        try:
            #this makes sure that only the neccecary requests are made
            print(ls)
            df = request_df([ls])
            print(df.head())
            time.sleep(2)
            logger.info('scraped '+str(df.shape[0])+' rows')
        except:
            logger.info('scrape failed '+str(ls), exc_info=True)
            
        
        #database
        try:
            df_database = insert_obj.return_saved_table('kent',logger,connection)
            insert_obj.insert_database(df,'kent',logger,connection,df_database=df_database, verify_data=verify)
            print('db length= '+str(saved_length(df_database)))
            print('got db'+str(ls))
            logger.info('got db '+str(ls)+' db length= '+str(saved_length(df_database)))
        except:
            print('failed database '+str(ls))
            logger.info('failed database '+str(ls))            
    
#%%
   
data_file = 'kent.csv'
direc = r'/home/grant/Documents/web_scraping/kent_gasoline_prices'
kent = sc.scrape(direc)  
logger = kent.scrape_logger('kent.log')
connection = kent.scrape_database('database.json',logger,work=False)
ins = sc.insert(direc, csv_path = data_file) 
#%%
         
product_list = ['Unleaded','Midgrade','Premium','Diesel']
report_list = ['Retail','Retail excl tax','Wholesale']
frequency_list = ['Daily','Weekly','Monthly']  

#test
#product_list = ['Unleaded']
#report_list = ['Retail']
#frequency_list = ['Weekly']  
#year_list = [2000]

#this should only be run once
#check if the database is empty. If so, then all data should be added.
#if not emty, then the yearlist should only the current year. and verify the data!

database = ins.return_saved_table('kent',logger,connection)
scraped_years =  database['Year'].unique()

if len(scraped_years) == 0:
    year_list = gather_years()
    verify_data=False
else:
    year_list = gather_years()
    year_list = [year_list[-1]] 
    verify_data=True

          
links = kent_links(product_list,report_list,frequency_list,year_list)
#links_test = links[:1]
gather_prices(links,logger,connection,ins,verify_data)

  
#%%#midgrade wholsale only has 2016 and up!
#2015 has xls extension
#2000 header should be 1 not 2
#maybe the frequency needs to be captalized
#TODO: This code is inneficient. Maybe dont check for duplicates each time!
#try checking if the database is empty. if so, do not check for duplicates!

