import pandas as pd
import datetime
from datetime import timedelta
from sqlalchemy import create_engine
import sqlalchemy
import urllib
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from sqlalchemy import text
import os
import json
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
#%%
def config_file(name):
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname('__file__')))
    print(os.path.join(__location__,name))
    try:
        with open(os.path.join(__location__,name)) as f:
            config = json.load(f)
            conn_str = config[0]['conn_string']
            return(conn_str)
    except:
        raise
#%%
def transport_mode(date_list,browser,data,selection):
    for p,e in enumerate(date_list):
        if p+1 != len(date_list):
            
            from_select = date_list[p]+' '+'AM' #the from and to month need to be the same!
            to_select = date_list[p]+' '+'AM'
            select_from = Select(browser.find_element_by_id('ctl00_MainContent_searchCriteria_ddlFrom'))
            select_to = Select(browser.find_element_by_id('ctl00_MainContent_searchCriteria_ddlTo'))
            select_from.select_by_value(str(from_select))
            select_to.select_by_value(str(to_select))
            browser.find_element_by_id('ctl00_MainContent_searchCriteria_btnSearch').click()
            html = browser.page_source
            soup = BeautifulSoup(html, 'html.parser')
            soup = str(soup)
            df = pd.read_html(soup, header = [0])[1]
            
            #correct the dates
            df_from = datetime.datetime.strptime(date_list[p], '%d/%m/%Y %H:%M:%S') - timedelta(hours=12)
            df_to = datetime.datetime.strptime(date_list[p], '%d/%m/%Y %H:%M:%S') - timedelta(hours=12)
            df['From'] = df_from
            df['To'] = df_to
            df = pd.melt(df,id_vars=['From','To','Units: m³ at 15ºC'],var_name='Region',value_name='Volume')
            df = df.dropna(axis=0)
            df['Product'] = str(selection)
            #format the dataframe based on transport_mode
            df['From'] = df['From'].astype('datetime64[ns]')
            df['To'] = df['To'].astype('datetime64[ns]')
            df['Units'] = 'm³ at 15ºC'
            df = df.rename(columns = {'Units: m³ at 15ºC':'Transport'})
            data.append(df)
    
    return(data)


def other_reports(date_list,browser,data,selection):

    from_select = date_list[-1]+' '+'AM'
    to_select = date_list[0]+' '+'AM'
    select_from = Select(browser.find_element_by_id('ctl00_MainContent_searchCriteria_ddlFrom'))
    select_to = Select(browser.find_element_by_id('ctl00_MainContent_searchCriteria_ddlTo'))
    select_from.select_by_value(str(from_select))
    select_to.select_by_value(str(to_select))
    browser.find_element_by_id('ctl00_MainContent_searchCriteria_btnSearch').click()
    html = browser.page_source
    soup = BeautifulSoup(html, 'html.parser')
    soup = str(soup)
    df = pd.read_html(soup, header = [0])[1]
    heading = df.columns[0]
    heading_df = heading.replace('Units:','')
    heading_df = heading_df.strip()
    df = pd.melt(df,id_vars=[heading],var_name='Region',value_name='Value')
    df = df.rename(columns = {heading:'Period'})
    try:
        df = df[df['Period']!='Total'] 
        df = df[df['Period']!='Average Price']
    except:
        pass
    df['Period'] = df['Period'].astype('datetime64[ns]')
    df['Product'] = str(selection)
    df['Units'] = str(heading_df)
    df = df.dropna(axis=0)
    data.append(df)
    return(data)
    
    
def find_cutoff(sql_table,conn,date_col='From'):
    
    try:
        sql = text('select MAX('+'['+date_col+']'+') from '+str(sql_table))
        max_date = conn.execute(sql)
        for row in max_date:
            max_date = row[0]
        max_date = max_date + timedelta(hours=12)
        max_date = datetime.datetime.strftime(max_date, '%d/%m/%Y %H:%M:%S')
        return(max_date,'append')
    except:
        max_date = '01/01/1990 12:00:00'
    
    return(max_date,'replace')
        


def neb_ngl(url,sql_table,conn):
    chromepath = r'C:\Users\mossgrant\Jupyter\Scrape\chromedriver.exe'
    browser = webdriver.Chrome(executable_path=chromepath)
    #chromeOptions = webdriver.ChromeOptions()
    #browser = webdriver.Chrome(options=chromeOptions)    
    browser.get(url) # open url in chrome browser
    if sql_table == 'NEB_CTS_NGL':
        cutoff,insert_type = find_cutoff(sql_table,conn)
    else:
        cutoff,insert_type = '01/01/1990 12:00:00','replace'
  
    data = []
    product_button = 'ctl00_MainContent_searchCriteria_ddlProduct'
    product = {'Butane':'BU', 'Propane':'PR'}

    #select_from = Select(browser.find_element_by_id('ctl00_MainContent_searchCriteria_ddlFrom'))
    #select_to = Select(browser.find_element_by_id('ctl00_MainContent_searchCriteria_ddlTo'))

    for selection in product: # select either butane or propane from the drop down 
        select_product = Select(browser.find_element_by_id(str(product_button)))
        select_product.select_by_value(str(product[selection]))
        y_selections = Select(browser.find_element_by_id('ctl00_MainContent_searchCriteria_ddlFrom')).options
        y_list = []
        for x in y_selections:
            y_list.append(str(x.text))
            #y_list is an ordered list that contains all the options for dates

        date_list = []
        for date in y_list:
            date = datetime.datetime.strptime(date, '%b-%Y' ) + timedelta(hours=12)
            if date >= datetime.datetime.strptime(cutoff, '%d/%m/%Y %H:%M:%S'):
                date_format = date.strftime('%d/%m/%Y %H:%M:%S')
                date_list.append(date_format)

        
        if sql_table == 'NEB_CTS_NGL':
            data = transport_mode(date_list,browser,data,selection)
        else:
            data = other_reports(date_list,browser,data,selection)
    
    browser.quit() 
    df = pd.concat(data, axis=0, sort=False, ignore_index=True)
    #insert to SQL
    #table = sqlalchemy.Table(sql_table, meta)
    #s = table.delete()
    #conn.execute(s)
    df.to_sql(sql_table, conn, if_exists=insert_type, index=False, chunksize=100)

def main(tables,conn):
    
    for report in tables:
        url = report['url']
        sql_table = report['table']
        
        neb_ngl(url,sql_table,conn)
        
#%% 
    
if __name__ == "__main__":
    
    
    raw_str = config_file('connection.json') 
    params = urllib.parse.quote_plus(raw_str)
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine = create_engine(conn_str)
    conn = engine.connect()
    meta = sqlalchemy.MetaData(conn)
    
    
    tables = [
            {"url":'https://apps.neb-one.gc.ca/CommodityStatistics/ExportVolumeByTransportModeSummary.aspx?commodityCode=PR',
             "table":'NEB_CTS_NGL'},
            {"url":'https://apps.neb-one.gc.ca/CommodityStatistics/ExportVolumeByPadd.aspx?commodityCode=PR',
             "table":'NEB_CTS_NGL_PADD'},
            {"url":'https://apps.neb-one.gc.ca/CommodityStatistics/ExportVolumeSummary.aspx?Language=English',
             "table":'NEB_CTS_NGL_Volume'},
            {"url":'https://apps.neb-one.gc.ca/CommodityStatistics/ExportPrice.aspx?commodityCode=PR',
             "table":'NEB_CTS_NGL_Price'}
             ]
    
    main(tables,conn)
    conn.close()
#%%