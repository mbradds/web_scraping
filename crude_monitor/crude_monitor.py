import pandas as pd
import requests
import lxml
import os
import time
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

#%%
def scrape_driver(driver_path,headless = False,download_path=None):
        
    try:
        chromeOptions = webdriver.ChromeOptions()
        #prefs = {'download.default_directory' : r'C:\Users\mossgrant\Documents\net_energy\download_files'}
        if download_path:
            prefs = {'download.default_directory' : download_path}
            chromeOptions.add_argument("--start-maximized")
            chromeOptions.add_experimental_option('prefs', prefs)
                
        if headless:
            chromeOptions.add_argument('headless')
                    
        driver = webdriver.Chrome(executable_path=driver_path, options=chromeOptions)
        return(driver)
    except:
        raise


def get_data(url='https://www.crudemonitor.ca/tools/quickReference.php'):
    #r = requests.get(url, allow_redirects=True, stream=True,headers=headers)
    try:
        df = pd.read_html(url)
        return(df)
    except:
        raise

def get_file(name):
        
    if os.path.isfile(name):
        df = pd.read_csv(name)
        print('read file: '+name)
    else:
        df = get_data()
        whole_crude_analysis, mini_assay_analysis = df[0],df[1]
        whole_crude_analysis.to_csv('whole_crude_analysis.csv',index=False)
        mini_assay_analysis.to_csv('mini_assay_analysis.csv',index=False)
        print('scraped and saved file')
    return(df)

def get_links(df):
    links = []
    base_link = 'https://www.crudemonitor.ca/crudes/index.php?acr=ACRONYM'
    
    for row in df.iterrows():
        acr = row[1]['Acronym']
        link = base_link.replace('ACRONYM',acr)
        links.append(link)
        
    return(links)

def wait(driver,delay,txt,txt_type = 'xpath',message='failed'):
    try:
        if txt_type == 'name':
            myElem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.NAME,txt)))
            driver.find_element_by_name(txt).click()
        elif txt_type == 'xpath':
            myElem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH,txt)))
            driver.find_element_by_xpath(txt).click()
        elif txt_type == 'id':
            myElem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.ID,txt)))
            driver.find_element_by_id(txt).click()
            
        return(driver)
    except:
        print(txt+' '+message)
        raise

def pull_data(links,driver):
    #links = links[:2] #for testing
    for link in links:
        
        try:
            driver.get(link)
            time.sleep(2)
            driver = wait(driver,5,"//*[@id='selectAll']",txt_type='xpath')
            time.sleep(2)
            driver = wait(driver,5,"//*[@id='xlsDownload']",txt_type='xpath')
            time.sleep(2)
            driver = wait(driver,5,"//*[@id='submitcustomreport'][@value='Export .CSV']",txt_type='xpath')
            time.sleep(5)
        except:
            print('cant get data for: '+str(link))
            continue
        
#%%
driver_path = r'C:\Users\mossgrant\Jupyter\Scrape\chromedriver.exe' 
download_path = r'F:\bucom\Energy Trade Team\Grant\crude_monitor'       
driver = scrape_driver(driver_path = driver_path,download_path = download_path)
#%%
df = get_file('whole_crude_analysis.csv')
links = get_links(df)
#%%
pull_data(links,driver)
driver.close()

#%%







