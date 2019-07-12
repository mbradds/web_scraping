import pandas as pd
import os
import re
import datetime
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import logging
import time
import json
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
#custom module for setting up scraper
from web_scraping.scraping_modules import scraping as sc
#%%  
  
def login(driver,email,pword):
    
    username = email
    password = pword
    login_url = 'https://secure.junewarren-nickles.com/login/?pub=DOB_BROWSE&continue=https%3A%2F%2Fwww.dailyoilbulletin.com%2Faccount%2Flogin%2F%3Fcontinue%3Dhttps%253A%252F%252Fwww.dailyoilbulletin.com%252F'
    email_id = "page_content_LoginEmail"
    password_id = "page_content_password"
    driver.get(login_url)

    #enter email
    login1 = driver.find_element_by_id(email_id)
    login1.clear()
    login1.send_keys(username)
    
    #enter password
    login2 = driver.find_element_by_id(password_id)
    login2.clear()
    login2.send_keys(password)
    
    #click button to enter the website
    name="ctl00$page_content$ctl03"
    driver.find_element_by_name(name).click()
    return(driver)


def get_table(link,driver):
    url = link #this is the link that has the actual data table
    driver.get(url)
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    time_soup = BeautifulSoup(html, 'html.parser')
    soup = str(soup)
    df = pd.read_html(soup, header = [0])[0]
    try:
        mytime = str(time_soup.findAll('time'))
        m = re.search('"(.*)T', mytime)
    except:
        print('Cant get the date')
    if m:
        found = m.group(1)
    else:
        found = np.nan
        
    df = df.dropna(axis=0,how='all')
        
    df['Date'] = pd.to_datetime(found)
    for x in df:
        if df[x].dtypes == 'object':
            df[x] = df[x].str.replace('$','')
            df[x] = df[x].str.replace('*','')
        else:
            continue
            
    try:
        df['Unnamed: 1'] = pd.to_numeric(df['Unnamed: 1'],errors='coerce')
        df['Implied Values'] = pd.to_numeric(df['Implied Values'],errors='coerce')
    except:
        print('cant change types')
    
    return(df)
    
#returns a list that can be added added into the links
def date_list(date_text='2019-01-01',existing_df=None):
    min_date = datetime.strptime(date_text, '%Y-%m-%d').date()
    today = datetime.now().date()
    end_date = today - timedelta(days=1) #lag the scraper by one day
    # get list of dates to grab data
    date_list = []

    while end_date >= min_date:
        day = end_date.weekday()
        if day in [5,6]:
            end_date = end_date-timedelta(days=1)
        else:
            date_list.append(end_date)
            end_date = end_date-timedelta(days=1)
    
    if existing_df.empty:
    
        return(date_list)
    
    else:
        existing_df['set'] = [str(d.date()) for d in existing_df['Date']]
        existing_dates = list(existing_df['set'].unique())
        existing_dates = [existing_dates.remove(x) if x=='NaT' else x for x in existing_dates]
        existing_dates = [x for x in existing_dates if x is not None]
        existing_dates = [datetime.strptime(d,'%Y-%m-%d').date() for d in existing_dates]
        set1 = set(existing_dates)
        set2 = set(date_list)
        
        #remove known holidays
        remaining_dates = set2 - set1
        holidays = ['2019-04-19','2019-02-18','2019-01-01','2019-05-20']
        holidays = set([datetime.strptime(d,'%Y-%m-%d').date() for d in holidays])
        return(list(remaining_dates - holidays))
        
    
def link_list(date_list, df_saved,test = False):
    replace_list = ['YYYY','MM','DD']
    all_links = []
    for date in date_list:
        base_link = 'https://www.dailyoilbulletin.com/report/YYYY/MM/DD/selected-oil-and-gas-prices/'
        d = [str(date.year),str(date.month),str(date.day)]
        for old, new in zip(replace_list,d):
            base_link = base_link.replace(old,new)
        all_links.append(base_link)
    
    #added redundency. If the url already exists in the data, then disregard it
    
    link_set_existing = set(list(df_saved['url']))
    link_set = set(all_links)
    
    if test:
        return(all_links[:5])
    else:
        #return(all_links)
        return(list(link_set - link_set_existing))
        
def cast_types(df):
    df['Close Last Trade Day'] = df['Close Last Trade Day'].astype('object')
    df['Units'] = df['Units'].astype('object')
    df['Price'] = pd.to_numeric(df['Price'], errors = 'coerce')
    df['Implied Values'] = pd.to_numeric(df['Implied Values'], errors = 'coerce')
    df['Date'] = pd.to_datetime(df['Date'], errors = 'coerce')
    return(df)
    

#take the list of links and pass them to the get_table function
def dob_dataframe(all_links,driver,logger,mode,head,data_file):
    oil_data = []
    for link in all_links:
        try:
            df = get_table(link,driver)
            df.rename(columns={'Unnamed: 1':'Price','Unnamed: 2':'Units'},inplace = True)
            df = cast_types(df)
            # get rid of unnececary rows:
            df['Close Last Trade Day'] = [x.strip() for x in df['Close Last Trade Day']]
            df = df[df['Close Last Trade Day']!='Expressed as a basis to WTI']
            df = df[df['Close Last Trade Day']!='NYMEX Today @ 10:00 AM MST']
            df['url'] = link
            oil_data.append(df)
            time.sleep(2)
            logger.info('got '+str(link))
            print('success')
        except:
            print('failure')
            logger.info('cant get '+str(link))
            
    
    try:
        df = pd.concat(oil_data, axis=0, sort=False, ignore_index=True)
        df.to_csv(data_file,mode = mode,index=False,header=head)
        logger.info('successfully added all new days')
    except:
        logger.info('failed to add new days of data')

def main():
    #gather login and file info
    direc = r'enter_path_to_working_directory'
    dob = sc.scrape(direc)
    
    config_file = dob.config_file('database.json')[0]['work'][0]
    driver_path = config_file['driver_file'] #enter path to chromedriver
    email = config_file['dob_email'] #enter string email, or get it from json file
    password = config_file['dob_password'] #enter string password, or get it from json file
    data_file = config_file['data_file'] #enter name of csv file ('dov.csv')
    logger = dob.scrape_logger('dob.log')
    
    #get saved data
    try:
        df_saved = pd.read_csv(data_file)
        df_saved = cast_types(df_saved)
        if not df_saved.empty:
            saved_length = len(df_saved)
        else:
            saved_length = 0
    
        if saved_length == 0:
            dates = date_list(existing_df = pd.DataFrame())
            mode,head = 'w',True
    
        else:
            dates = date_list(existing_df = df_saved)
            mode,head = 'a',False
            
    except:
        raise

    #scrape new data
    try:
        driver = dob.scrape_driver(driver_path = driver_path,logger = logger, browser = 'Chrome', headless = False)
        driver = login(driver,email, password)
        links = link_list(dates,df_saved,test = False)
        dob_dataframe(links,driver,logger,mode,head,data_file)

    except:
        driver.close()
        logging.shutdown()
    finally:
        driver.close()
        logging.shutdown()
    

#%%    
if __name__ == "__main__":# and datetime.now().date().weekday() not in [5,6]:
    main()

#%%
