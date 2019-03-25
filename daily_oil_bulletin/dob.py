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
import scraping as sc
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
    #driver = login()
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
#this function isnt working, but could be used later to get all links on a given page    
def get_links(function, base_url = 'https://www.dailyoilbulletin.com/reports/selected-oil-and-gas-prices/'):
    
    oil = pd.DataFrame()
    
    try:
        driver.get(base_url)
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        elems = driver.find_elements_by_xpath("//a[@href]")
    except:
        driver = function
        base_url = base_url
        driver.get(base_url)
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        elems = driver.find_elements_by_xpath("//a[@href]")
        
        
    links = []
    
    #elems = web_driver.find_elements_by_xpath("//a[@href]")
    
    for elem in elems:
        links.append(elem.get_attribute("href"))
    
    # add clean_links_function
    
    selected_list = []
    for y in links:
        if (y.find('selected-oil-and-gas-prices') != -1) and (y.find('junewarren') == -1) and (y.find('#') == -1) and (y.find('2019')!=-1):
            selected_list.append(y)
            
        
    page_list = []
    link_list = []
    df_list = []
    for x in selected_list:
        if x.find('page') != -1:
            if base_url == 'https://www.dailyoilbulletin.com/reports/selected-oil-and-gas-prices/':
                page_list.append(x)
            else:
                pass
        else:
            link_list.append(x)
            try:
                df = get_table(x,driver)
                print('got df'+' '+str(x))
                #continue
            except:
                print('cant get df'+' '+str(x))
                continue
            time.sleep(1)
            #print('got df'+' '+str(x))
            df_list.append(df)
            
    page_list=list(set(page_list))
            
    for x in page_list:
        l = get_links(function=login, base_url = x)
        time.sleep(2)
        print('getting next page')
        link_list.extend(l)
        
    
    driver.close()
    oil = pd.concat(df_list, axis=0, sort=False, ignore_index=True)
            
    return(oil)
#%%
#returns a list that can be added added into the links
def date_list(date_string = '2018-07-06'):
    today = datetime.now().date()
    end_date = today - timedelta(days=1) #lag the scraper by one day
    # get list of dates to grab data
    date_list = []
    date_object = datetime.strptime(date_string, '%Y-%m-%d').date() # 2018/7/9 is the first day. July 6 has 1128

    while end_date >= date_object:
        day = end_date.weekday()
        if day in [5,6]:
            end_date = end_date-timedelta(days=1)
        else:
            date_list.append(end_date)
            end_date = end_date-timedelta(days=1)

    return(date_list)
#%%    
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

def dob_dataframe(all_links,driver):
    oil_data = []
    for link in all_links:
        try:
            df = get_table(link,driver)
            #print(df.head())
            #print(df.dtypes)
            #convert the datatypes
            df['Close Last Trade Day'] = df['Close Last Trade Day'].astype('object')
            df['Unnamed: 2'] = df['Unnamed: 2'].astype('object')
            df['Unnamed: 1'] = pd.to_numeric(df['Unnamed: 1'], errors = 'coerce')
            df['Implied Values'] = pd.to_numeric(df['Implied Values'], errors = 'coerce')
            df['Date'] = pd.to_datetime(df['Date'], errors = 'coerce')
            oil_data.append(df)
            logger.info('got '+str(link))
        except:
            logger.info('cant get '+str(link))
    df = pd.concat(oil_data, axis=0, sort=False, ignore_index=True)
    return(df)
    
#%%
data_file = 'dob.csv'
direc = r'/home/grant/Documents/web_scraping/daily_oil_bulletin'
driver_path = r'/home/grant/geckodriver'
dob = sc.scrape(direc)        
logger = dob.scrape_logger('daily_oil_bulletin.log')
driver = dob.scrape_driver(driver_path = driver_path,logger = logger, browser = 'Firefox', headless = False)
connection = dob.scrape_database('database.json',logger)
config_file = dob.config_file('database.json',logger)
email = config_file[0]['dob_email']
password = config_file[0]['dob_password']

#%%
try:
    driver = login(driver,email = email, pword = password)
    dates = date_list()
    links = link_list(dates,test = False)
    oil = dob_dataframe(links,driver)
    oil.rename(columns={'Unnamed: 1':'Price','Unnamed: 2':'Units'},inplace = True)
    dob.insert_csv(oil,data_file,logger)
except:
    None #add errors to logger
finally:
    driver.close()

#%%