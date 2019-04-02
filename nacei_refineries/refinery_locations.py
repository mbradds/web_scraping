# -*- coding: utf-8 -*-
import requests
import io
import os
import pandas as pd
import sys
#TODO: at home, I need to add Documents. to relative import. Why is this?
from web_scraping.scraping_modules import scraping as sc
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

#%%
class nacei:
    
    url = 'https://www.nacei.org/content/documents/Refineries_NorthAmerica_201708.xlsx'
    
    def __init__(self):
        None

    def brackets(self,text,l,r):
        if text.find(str(l)) != -1 and text.find(str(r)) != -1:
            return(text[text.find(str(l))+1:text.find(str(r))])
        else:
            return('')
            
    #refinery data
    def pull_data(self,url):
        sheet_name = 'Refineries'
        r = requests.get(url, allow_redirects=True, stream=True, headers=headers,verify=False)
        if r.status_code == requests.codes.ok:
            df = pd.read_excel(io.BytesIO(r.content), sheet_name=sheet_name, header=[0])
                
        else:
            print('Error! File could not be downloaded from: ' + url)
            
        return(df)
        
    
    def process(self,df,canada=False):
        #flatten the data
        df = pd.melt(df,
                     value_vars=['Atmospheric Distillation (Mb/d)',
                                  'Vacuum Distillation (Mb/d)',
                                  'Catalytic Disintegration (Mb/d)',
                                  'Viscosity Reduction (Mb/d)',
                                  'Catalytic Reformation (Mb/d)',
                                  'Alkylation and Isomerization (Mb/d)',
                                  'Hydrodesulphurization (Mb/d)',
                                  'Coking (Mb/d)',
                                  'Asphalt Production (Mb/d)',
                                  'Atmospheric Distillation (km³/d)',
                                  'Vacuum Distillation (km³/d)',
                                  'Catalytic Disintegration (km³/d)',
                                  'Viscosity Reduction (km³/d)',
                                  'Catalytic Reformation (km³/d)',
                                  'Alkylation and Isomerization (km³/d)',
                                  'Hydrodesulphurization (km³/d)',
                                  'Coking (km³/d)',
                                  'Asphalt Production (km³/d)'],
        id_vars = ['Country',
                   'Latitude',
                   'Longitude',
                   'Facility Name',
                   'Owner Name (Company)',
                   'State / Province / Territory',
                   'Type'], value_name='Capacity',var_name='Refining Type')   
        
        #format the data
        df['Unit'] = [self.brackets(x,'(',')') for x in df['Refining Type']]
        df['Refining Type'] = [x.split('(')[0] for x in df['Refining Type']]
        df['Unit'] = [x.strip() for x in df['Unit']]
        df['Refining Type'] = [x.strip() for x in df['Refining Type']]
        
        if canada:
            df = df[df['Country']== 'Canada']
            df = df[df['Type']=='Refinery']
            df = df[df['Refining Type']=='Atmospheric Distillation']
        
        return(df)
    
if __name__ == "__main__":
    
    direc = r'C:\Users\mossgrant\web_scraping\nacei_refineries'
    r = sc.scrape(direc)  
    logger = r.scrape_logger('nacei.log')
    #connection = r.scrape_database('database.json',logger,work=False)
    ins = sc.insert(direc,csv_path='refineries.csv') 
    
    refineries = nacei()
    location_data = refineries.pull_data(refineries.url) 
    df = refineries.process(location_data,canada=True)
    #ins.insert_database(df,'nacei_refineries',logger,connection,insert_type = 'replace')
    ins.insert_csv(df,logger)
    