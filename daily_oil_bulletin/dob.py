# -*- coding: utf-8 -*-
import pandas as pd
import os
import re
import datetime
import numpy as np
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
import logging
import json
import sys
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

#%%    
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
    
#%%
#returns a list that can be added added into the links
#TODO: add mindate parameter. if csv is empty, mindate is 2018-07-06 otherwise, it is the max of the date column
def date_list(date_text='2018-07-06'):
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

    return(date_list)
#%%    
#TODO: change the data structure to a dictionary, with the key as the date. This can be used to raise a critical error if the scrape fails on a weekday
def link_list(date_list, test = False):
    replace_list = ['YYYY','MM','DD']
    all_links = []
    for date in date_list:
        base_link = 'https://www.dailyoilbulletin.com/report/YYYY/MM/DD/selected-oil-and-gas-prices/'
        d = [str(date.year),str(date.month),str(date.day)]
        for old, new in zip(replace_list,d):
            base_link = base_link.replace(old,new)
        all_links.append(base_link)

    if test:
        return(all_links[:5])
    else:
        return(all_links)
#%% 
#take the list of links and pass them to the get_table function
def dob_dataframe(all_links,driver,logger):
    oil_data = []
    for link in all_links:
        try:
            df = get_table(link,driver)
            df['Close Last Trade Day'] = df['Close Last Trade Day'].astype('object')
            df['Unnamed: 2'] = df['Unnamed: 2'].astype('object')
            df['Unnamed: 1'] = pd.to_numeric(df['Unnamed: 1'], errors = 'coerce')
            df['Implied Values'] = pd.to_numeric(df['Implied Values'], errors = 'coerce')
            df['Date'] = pd.to_datetime(df['Date'], errors = 'coerce')
            # get rid of unnececary rows:
            df['Close Last Trade Day'] = [x.strip() for x in df['Close Last Trade Day']]
            df = df[df['Close Last Trade Day']!='Expressed as a basis to WTI']
            df = df[df['Close Last Trade Day']!='NYMEX Today @ 10:00 AM MST']
            df['url'] = link
            oil_data.append(df)
            print(df.head())
            logger.info('got '+str(link))
        except:
            logger.info('cant get '+str(link))
            
    df = pd.concat(oil_data, axis=0, sort=False, ignore_index=True)
    df.rename(columns={'Unnamed: 1':'Price','Unnamed: 2':'Units'},inplace = True)
    return(df)
    
#%%    
if __name__ == "__main__" and datetime.now().date().weekday() not in [5,6]:
    
    direc = r'C:\Users\mossgrant\web_scraping\daily_oil_bulletin'
    dob = sc.scrape(direc)
    
    config_file = dob.config_file('database.json')[0]['work'][0]
    driver_path = config_file['driver_file']
    email = config_file['dob_email']
    password = config_file['dob_password']
    data_file = config_file['data_file']
    
    logger = dob.scrape_logger('dob.log')
    logger.setLevel(logging.DEBUG)
    ins = sc.insert(direc, csv_path = data_file)   
    
    try:
        driver = dob.scrape_driver(driver_path = driver_path,logger = logger, browser = 'Chrome', headless = False)
        df_saved = ins.return_saved_csv()
        if df_saved != None:
            saved_length = len(df_saved)
        else:
            saved_length = 0
        
        driver = login(driver,email = email, pword = password)
        if saved_length == 0:
            dates = date_list()
            verify = False
        else:
            dates = date_list(max(df_saved['Date']))
            verify=True 
        links = link_list(dates,test = False)
        oil = dob_dataframe(links,driver,logger)
        ins.insert_csv(oil,logger,verify_data=verify)
    except:
        driver.close()
    finally:
        driver.close()
        logging.shutdown()

#%%