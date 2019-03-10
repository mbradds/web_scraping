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
import os 
import json
import urllib
import sys
from sqlalchemy import create_engine
from sqlalchemy import exc
os.chdir(r'/home/grant/Documents/web_scraping')
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

#set paths
data_file = r'/home/grant/Documents/web_scraping/nymex.csv'
log_file = r'/home/grant/Documents/web_scraping/nymex_log.log'
trade_date_id = 'cmeTradeDate'
#configure the logger. This needs to be changed!
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('nymex.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
#logging.basicConfig(filename=log_file,level=logging.INFO,filemode='a', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

#%%
#connect to the database

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname('__file__')))
with open(os.path.join(__location__,'database.json')) as f:
    config = json.load(f)
    
password = config[0]['password']
user_name = config[0]['user_id']
database = config[0]['database']
connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE="+str(database)+";UID="+str(user_name)+";PWD="+str(password)
params = urllib.parse.quote_plus(connection_string)
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
connection=engine.connect()

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
def return_not_in_csv(df1, df2):
 
    intersected_df = pd.merge(df1, df2, how='right', indicator=True) #change the merge to join on the key
    #intersected_df.to_csv(r'F:\bucom\INPUT DATA\Oil\NYMEX\intersected.csv', header=True,index=False)
    intersected_df = intersected_df[intersected_df['_merge']=='right_only']
    intersected_df = intersected_df.drop('_merge',1)
    return(intersected_df)

def nymex_options(url,trade_date = trade_date_id):
    
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
        #configure the driver
        options = Options()
        options.headless = True
        driver = webdriver.Firefox(options=options, executable_path=r'/home/grant/geckodriver')
        driver.get(url)
        trade_date_id = trade_date
        date_selections = Select(driver.find_element_by_id(trade_date_id)).options
    except:
        logger.info('Cant connect to website at '+str(datetime.datetime.now()),exc_info=True)
        
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
        logger.info('cant create the date used for selection at '+str(datetime.datetime.now()),exc_info=True)
    
    #date dict contains all of the data options for the current day
    return(date_dict,driver)

#%%[Id] INT NOT NULL PRIMARY KEY, -- Primary Key column

def nymex_scrape(date_dict,driver):
        
    try:
        data = []    
        for key,value in date_dict.items():
            date_select = Select(driver.find_element_by_id(trade_date_id))
            date_select.select_by_visible_text(str(key))
            time.sleep(2) #give the page time to reload the new data
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            soup = str(soup)
            df = pd.read_html(soup)[2]
            df['Trade Date'] = value
            df['Settlement'] = brackets(str(key),'(',')')
            data.append(df)
            logger.info('scraped df: '+str(key))
    except:
        logger.info('cant gather the source data, or cant build the dataframe at' +str(datetime.datetime.now()),exc_info=True)
        
    try:
        
        df = pd.concat(data, axis=0, sort=False, ignore_index=True)
        df['Trade Date'] = df['Trade Date'].astype('datetime64[ns]')
        df['Open'] = pd.to_numeric(df['Open'],errors='coerce')
        df['High'] = pd.to_numeric(df['High'],errors='coerce')
        df['Low'] = pd.to_numeric(df['Low'],errors='coerce')
        df['Last'] = pd.to_numeric(df['Last'],errors='coerce')
        df['Settle'] = pd.to_numeric(df['Settle'],errors='coerce')
        df['Change'] = pd.to_numeric(df['Change'],errors='coerce')
        df['Month'] = ['JUL'+d[3:] if d[:3]=='JLY' else d for d in df['Month']]
        #add underscore to month so that csv will not change the format
        df['Month'] = [m.replace(' ','_') for m in df['Month']]
        #df['key'] = [str(m)+'_'+str(t)+'_'+str(s) for m,t,s in zip(df['Month'],df['Trade Date'],df['Settlement'])]
    except:
        logger.info('cant concatentate the dataframe at' +str(datetime.datetime.now()),exc_info=True)
     
    logger.info('successfully scraped the CME Website at'+' '+str(datetime.datetime.now())+'on '+str(sys.executable))
    return(df)

#%%
#insert new data to csv
def insert_csv(df, csv_path):
    #get everything into the csv
    if os.path.isfile(csv_path):
        #if file exists, then append all the new data
        df_csv = pd.read_csv(csv_path)
        df_csv['Trade Date'] = df_csv['Trade Date'].astype('datetime64[ns]')
        #df_csv['key'] = [str(m)+'_'+str(t)+'_'+str(s) for m,t,s in zip(df_csv['Month'],df_csv['Trade Date'],df_csv['Settlement'])]
        not_in_csv = return_not_in_csv(df1=df_csv,df2=df)
        
        if not not_in_csv.empty:
        
            with open(csv_path, 'a') as f:
                rows_added = str(not_in_csv.shape[0])
                not_in_csv.to_csv(f, header=False,index=False)
                logging.info('added '+str(rows_added)+' new rows at ' +str(datetime.datetime.now()))
        else:   
            logger.info('no new data at '+str(datetime.datetime.now()))
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
        not_in_db.to_sql(sql_table, connection, if_exists='replace', index=False,chunksize=100)
        rows_added = str(not_in_db.shape[0])
        logging.info('added '+str(rows_added)+' new rows to database at ' +str(datetime.datetime.now()))

    except:
        #no rows are returned (the table is empty)
        logging.info('database query or insert error')
    
#%%    
# main
url = 'https://www.cmegroup.com/trading/energy/crude-oil/west-texas-intermediate-wti-crude-oil-calendar-swap-futures_quotes_settlements_futures.html'
options,driver = nymex_options(url)
scrape_df = nymex_scrape(options,driver)
try:
    insert_csv(scrape_df,data_file)
    insert_database(scrape_df,connection)
    logging.info('successfully added all data')
except:
    logging.info('error adding new data to csv or db')
    