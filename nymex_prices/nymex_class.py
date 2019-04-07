# -*- coding: utf-8 -*-
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.firefox.options import Options
import pandas as pd
import datetime
from bs4 import BeautifulSoup
import logging
import logging.handlers
import time
import numpy as np
import os 
import json
import urllib
import sys
from sqlalchemy import create_engine
from sqlalchemy import exc
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

#%%
#use a class to iniate the database, driver and logger for scraping
class scrape:
    
    def __init__(self,directory):
        self.directory = directory
        os.chdir(directory)
    
    def scrape_logger(logger_name):
        name = logger_name.replace('.log','')
        logger = logging.getLogger(name)
        if not logger.handlers:
            logger.propagate = False
            logger.setLevel(logging.INFO)
            handler = logging.FileHandler(logger_name)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return(logger)
    
    def scrape_database(config_file,logger):
        __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname('__file__')))
        
        try:
            with open(os.path.join(__location__,'database.json')) as f:
                config = json.load(f)
            
            password = config[0]['password']
            user_name = config[0]['user_id']
            database = config[0]['database']
            connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE="+str(database)+";UID="+str(user_name)+";PWD="+str(password)
            params = urllib.parse.quote_plus(connection_string)
        except:
            logger.info('error with database config file ',exc_info=True)
        
        try:
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            connection=engine.connect()
            logger.info('connected to database')
            return(connection)
        except:
            logger.info('error with database connection ',exc_info=True)
            return(None)
            
    
    def scrape_driver(driver_path, browser, headless = True):
        if browser == 'Firefox':
        
            try:
                options = Options()
                options.headless = headless
                driver = webdriver.Firefox(options=options, executable_path=driver_path)
                logger.info('successfully created the web driver')
                return(driver)
            except:
                logger.info('error creating the firefox web scraper ',exc_info=True)
        else:
        
            try:
                chromeOptions = webdriver.ChromeOptions()
                if headless:
                    chromeOptions.add_argument('headless')
                driver = webdriver.Chrome(options=chromeOptions, executable_path=driver_path)
                logger.info('successfully created the web driver')
                return(driver)
            except:
                logger.info('error creating the  chrome web scraper ',exc_info=True)
                return(None)

            
        
#%%
#helper functions

#return all text between two custom characters
def brackets(text,l,r):
    text = str(text)
    if text.find(str(l)) != -1 and text.find(str(r)) != -1:
        return(text[text.find(str(l))+1:text.find(str(r))])
    else:
        return('')
        
#function to return all of the scraped data that is not in the database
        
def return_not_in_csv(logger,df1, df2):
        
    if len(df1.columns) != len(df2.columns):
        logger.info('scraped df and csv/db have different number of columns',exc_info=True)
            
        #this try block attempts to correct any differences in data types between stored and scraped dataframes
    try:
            
        for x in df1.columns:
            if x not in df2.columns:
                logger.info('scraped df and csv/db have different column names')
                    
            if df1[x].dtypes != df2[x].dtypes:
                logger.info('csv/db and scraped df have different column types')
                df1[x] = df1[x].astype(df2[x].dtypes)
    
            else:
                None #datatype is the same
    except:
        logger.info('error automatically changing datatypes for scraped df and saved df ',exc_info=True)
        raise
        
        #once the types are the same, then merge them and get anything not in csv or db
    try:
        intersected_df = pd.merge(df1, df2, how='right', indicator=True) 
        intersected_df = intersected_df[intersected_df['_merge']=='right_only']
        intersected_df = intersected_df.drop('_merge',1)
        return(intersected_df)
          
    except:
        logger.info('couldnt merge the csv/db and scraped df', exc_info=True)
        raise

#def return_not_in_csv(df1, df2):
#    # data types need to be the same between dataframes
#    intersected_df = pd.merge(df1, df2, how='right', indicator=True) 
#    intersected_df = intersected_df[intersected_df['_merge']=='right_only']
#    intersected_df = intersected_df.drop('_merge',1)
#    return(intersected_df)

#gather all of the drop down options. These options need to be formatted to use as a selection
def nymex_options(url,driver,date_id = 'cmeTradeDate'):
    
    month_dict = {'01':'Jan',
              '02':'Feb',
              '03':'Mar',
              '04':'Apr',
              '05':'May',
              '06':'Jun',
              '07':'Jul',
              '08':'Aug',
              '09':'Sep',
              '10':'Oct',
              '11':'Nov',
              '12':'Dec'}
    
    try:
        driver.get(url)
        date_selections = Select(driver.find_element_by_id(date_id)).options
    except:
        logger.info('web driver cant connect to website ',exc_info=True)
        
    date_list = []
    for x in date_selections:
        date_list.append(str(x.text))

    date_dict = {}
    try:
        for i,date in enumerate(date_list):
            x = brackets(date,',','(')
            x = x.strip()
            x = x.split(' ')
            for key,value in month_dict.items():
                if value == x[1]:
                    m = key

            full_date = str(m)+'/'+str(x[0])+'/'+str(x[2])
            date_dict.update({date:full_date})
    except:
        logger.info('cant create the date used for selection at ',exc_info=True)
    
    #date dict contains all of the data options for the current day
    return(date_dict,driver) #pass the driver to the data scraper

#%%

def nymex_scrape(date_dict,driver,date_id = 'cmeTradeDate'):
        
    try:
        data = []    
        for key,value in date_dict.items():
            date_select = Select(driver.find_element_by_id(date_id))
            date_select.select_by_visible_text(str(key))
            time.sleep(2) #give the page time to reload the new data
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            soup = str(soup)
            df = pd.read_html(soup)[2]
            df['Trade Date'] = value
            df['Settlement'] = brackets(str(key),'(',')')
            data.append(df)
            rows_added = str(df.shape[0])
            df.columns = df.columns.droplevel(level=1)
            logger.info('scraped df: '+str(key)+' '+str(rows_added)+' rows')
            
    except:
        logger.info('cant gather the source data, or cant build the dataframe at ', exc_info=True)
    
    #TODO: the np.nan's should be changed to pd.to_numeric, but this function isnt working!
    try:
        return(df)
        df = pd.concat(data, axis=0, sort=False, ignore_index=True)
        df['Trade Date'] = df['Trade Date'].astype('datetime64[ns]')
        df['Open'] = pd.to_numeric(df['Open'], errors='coerce')
        df['High'] = pd.to_numeric(df['High'], errors='coerce')
        df['Low'] = pd.to_numeric(df['Low'], errors='coerce')
        df['Last'] = pd.to_numeric(df['Last'], errors='coerce')
        df['Settle'] = pd.to_numeric(df['Settle'], errors='coerce')
        df['Change'] = pd.to_numeric(df['Change'], errors='coerce')
        df['Month'] = ['JUL'+d[3:] if d[:3]=='JLY' else d for d in df['Month']]
        #add underscore to month so that csv will not change the format
        df['Month'] = [m.replace(' ','_') for m in df['Month']]

    except:
        logger.info('cant concatentate the dataframe at ' ,exc_info=True)
     
    logger.info('successfully scraped the CME Website at'+' on '+str(sys.executable))
    return(df)

#%%
#insert new data to csv
def insert_csv(df, csv_path,logger):
    #get everything into the csv
    if os.path.isfile(csv_path):
        #if file exists, then append all the new data
        df_csv = pd.read_csv(csv_path)
        df_csv['Trade Date'] = df_csv['Trade Date'].astype('datetime64[ns]')
        not_in_csv = return_not_in_csv(logger,df1=df_csv,df2=df)
        
        if not not_in_csv.empty:
        
            with open(csv_path, 'a') as f:
                rows_added = str(not_in_csv.shape[0])
                not_in_csv.to_csv(f, header=False,index=False)
            logger.info('added '+str(rows_added)+' new rows to csv')
        else:   
            logger.info('no new csv data')
    else:
        #if the file does not exists, then save it and wait for the next day
        logger.info('fist scrape on ' +str(datetime.datetime.now()),exc_info=True)
        df.to_csv(data_file, header=True,index=False)
    return(None)
    
#%%
#insert new data to database
def insert_database(df,connection):
    sql_table = 'nymex'
    try:
        s = 'select * from [dbo].[nymex]'
        nymex_db = pd.read_sql_query(s,connection)
        #fix the datatypes
        nymex_db['Open'] = pd.to_numeric(nymex_db['Open'],errors='coerce')
        nymex_db['High'] = pd.to_numeric(nymex_db['High'],errors='coerce')
        nymex_db['Low'] = pd.to_numeric(nymex_db['Low'],errors='coerce')
        nymex_db['Last'] = pd.to_numeric(nymex_db['Last'],errors='coerce')
        not_in_db = return_not_in_csv(df1=nymex_db,df2=df)
        
        if not not_in_db.empty:
            rows_added = str(not_in_db.shape[0])
            not_in_db.to_sql(sql_table, connection, if_exists='append', index=False,chunksize=100)
            logger.info('added '+str(rows_added)+' new rows to database')
        else:
            logger.info('no new database data')

    except:
        #no rows are returned (the table is empty)
        logging.info('database query or insert error')
    
#%%    
# main   
data_file = r'/home/grant/Documents/data_files/nymex.csv'
direc = r'/home/grant/Documents/web_scraping/nymex_prices'
driver_path = r'/home/grant/geckodriver'
scrape(directory = direc)        
logger = scrape.scrape_logger('nymex_class.log') 
connection = scrape.scrape_database('database.json',logger)
driver = scrape.scrape_driver(driver_path = driver_path,browser = 'Firefox', headless = True)        

url = 'https://www.cmegroup.com/trading/energy/crude-oil/west-texas-intermediate-wti-crude-oil-calendar-swap-futures_quotes_settlements_futures.html'
options,driver = nymex_options(url,driver)
scrape_df = nymex_scrape(options,driver)
#%%
try:
    insert_csv(scrape_df,data_file,logger)
    insert_database(scrape_df,connection)
    logger.info('added all new data')
except:
    logger.info('error adding new data to csv or db',exc_info = True)
    
finally:
    driver.close()
    connection.close()

#%%