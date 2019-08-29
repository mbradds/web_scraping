import pandas as pd
import numpy as np
from os import path
from PIL import Image
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import matplotlib.pyplot as plt
import os
import time
from selenium import webdriver
from os import listdir
from os.path import isfile, join
from tika import parser
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
#%%
def scrape_driver(driver_path,headless = False,download_path=None):
        
    try:
        chromeOptions = webdriver.ChromeOptions()
        #prefs = {'download.default_directory' : r'C:\Users\mossgrant\Documents\net_energy\download_files'}
        if download_path:
            prefs = {'download.default_directory' : download_path,
                     'plugins.always_open_pdf_externally':True}
            chromeOptions.add_argument("--start-maximized")
            chromeOptions.add_experimental_option('prefs', prefs)
                
        if headless:
            chromeOptions.add_argument('headless')
                    
        driver = webdriver.Chrome(executable_path=driver_path, options=chromeOptions)
        return(driver)
    except:
        raise


def bcuc_links(driver,base_link):
    
    link_list = []
    driver.get(base_link)
    for a in driver.find_elements_by_xpath('.//a'):
        #print(a.get_attribute('href'))
        link_list.append(a.get_attribute('href'))
    
    return(link_list,driver)

def letters_of_comment(driver,link_list,download_path):
    
    loc = []
    for link in link_list:
        #print(link)
        if link is not None and 'LetterofComment' in link:
            loc.append(link)
    
    #for testing
    #loc = loc[:5]
    for download in loc:
        file = download.split("/")[-1]
        
        if os.path.isfile(download_path+"/"+file):
            print(file+' already exists')
        else:
            #print(file+' does not exist')
            try:
                driver.get(download)
                time.sleep(4)
            except:
                print('cant get: '+download)
                    
    return(loc)

def download_files(download_path):
    '''
    gets all the letters of comment from the BCUC website
    '''
    base_link = 'https://www.bcuc.com/ApplicationView.aspx?ApplicationId=681'
    driver_path = r'C:\Users\mossgran\Documents\chromedriver.exe' 
    #download_path = r'F:\bucom\Energy Trade Team\Grant\bcuc_word_cloud\letters'       
    driver = scrape_driver(driver_path = driver_path,download_path = download_path)
    link_list,driver=bcuc_links(driver,base_link)
    loc = letters_of_comment(driver,link_list,download_path)
    driver.close()


def read_pdfs(download_path):
    onlyfiles = [f for f in listdir(download_path) if isfile(join(download_path, f))]
    text_list = []
    
    for file in onlyfiles:
        raw = parser.from_file(download_path+"/"+file)
        pdf = raw['content']
        text_list.append(pdf)

    return(text_list)

def process_pdf(text_list):
    full = [x.strip().splitlines() for x in text_list]
    processed = []
    for section in full:
        new_section = []
        for word in section:
            new_section.append(word.strip())
            
        processed.append(new_section)
    
    cutoff1 = []
    for section in processed:
        for num,val in enumerate(section):
            if val == 'Comment:':
                cutoff1.append(section[num+1:])
            else:
                None
    
    cutoff2 = []
    for section in cutoff1:
        for num,val in enumerate(section):
            if val == '':
                #print(num,section3[num-1])
                cutoff2.append(section[:num])
                break
    
    concat = []
    for section in cutoff2:
        concat.append(' '.join(section))
        
    

    return(' '.join(concat).lower())

def cloud(text):
    wordcloud = WordCloud(max_font_size=90, max_words=100, background_color="white",width=900, height=500).generate(processed)  
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    wordcloud.to_file('bcuc1.png')
    plt.show()
    
    
    
#%%
if __name__ == "__main__":
    download_path = r'F:\bucom\Energy Trade Team\Grant\bcuc_word_cloud\letters'
    #download_files(download_path)
    #files = read_pdfs(download_path)
    processed = process_pdf(files)
    cloud(processed)
    
    
#%%
#cloud
