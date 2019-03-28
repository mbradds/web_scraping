
import os
import logging
import json
import urllib
from sqlalchemy import create_engine
from sqlalchemy import exc
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.firefox.options import Options
import pandas as pd


#use a class to iniate the database, driver and logger for scraping
class scrape:
    
    def __init__(self,directory):
        self.directory = directory
        os.chdir(directory)
    
    def scrape_logger(self,logger_name):
        name = logger_name.replace('.log','')
        logger = logging.getLogger(name)
        if not logger.handlers:
            logger.propagate = False
            logger.setLevel(logging.INFO)
            handler = logging.FileHandler(logger_name)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger.info('created the logger')
        return(logger)
    
    def scrape_database(self,config_file,logger):
        __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname('__file__')))
        
        try:
            with open(os.path.join(__location__,config_file)) as f:
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
            
    
    def scrape_driver(self,driver_path,logger, browser, headless = True):
        if browser == 'Firefox':
        
            try:
                options = Options()
                options.headless = headless
                driver = webdriver.Firefox(options=options, executable_path=driver_path)
                logger.info('successfully created the firefox web driver ',exc_info=True)
                return(driver)
            except:
                logger.info('error creating the firefox web scraper ',exc_info=True)
        else:
        
            try:
                chromeOptions = webdriver.ChromeOptions()
                if headless:
                    chromeOptions.add_argument('headless')
                driver = webdriver.Chrome(options=chromeOptions, executable_path=driver_path)
                logger.info('successfully created the firefox web driver')
                return(driver)
            except:
                logger.info('error creating the chrome web scraper ',exc_info=True)
                return(None)
    
    
    def config_file(self,config_file,logger):
        __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname('__file__')))
        
        try:
            with open(os.path.join(__location__,config_file)) as f:
                config = json.load(f)
                logger.info('imported the config file')
                return(config)

        except:
            logger.info('error with config file ',exc_info=True)
            return(None)

#this class handles inserting new scraped data into a csv or database table
class insert:

    #directory is used for the csv save location
    #a database table name can be specified
    #add database driver to this class...
    def __init__(self,directory,csv_path = None, table = None):
        self.directory = directory
        self.csv_path = csv_path
        os.chdir(directory)
                
    #return a dataframe that does not exist in the csv or database
    #df1 = database/csv, df2 = scraped df
    def return_not_in_csv(self,logger,df1, df2):
        
        if len(df1.columns) != len(df2.columns):
            logger.info('scraped df and csv/db have different number of columns',exc_info=True)
            
        #this try block attempts to correct any differences in data types between stored and scraped dataframes
        
        for x in df1.columns:
            if x not in df2.columns:
                logger.info('scraped df and csv/db have different column names')
                
            if df1[x].dtypes != df2[x].dtypes:
                logger.info('csv/db and scraped df have different column types')
                        #try to make the scraped df types the same as the csv or db
        #            try:
        #                df1[x] = df1[x].astype(df2[x].dtypes)
        #                logger.info('converted csv/db column '+str(x))
        #            except:
        #                logger.info('error converting csv/db column '+str(x), exc_info=True)
        #        else:
        #            None #datatype is the same
        
        #once the types are the same, then merge them and get anything not in csv or db
        try:
            intersected_df = pd.merge(df1, df2, how='right', indicator=True) 
            intersected_df = intersected_df[intersected_df['_merge']=='right_only']
            intersected_df = intersected_df.drop('_merge',1)
            return(intersected_df)
          
        except:
            logger.info('couldnt merge the csv/db and scraped df', exc_info=True)
            raise Exception('saved dataframe and scraped dataframe have different data types')


    #functions for inserting to csv/db
    def insert_csv(self,df_scrape,df_csv,logger):
        #get everything into the csv
        if os.path.isfile(self.csv_path):

            not_in_csv = self.return_not_in_csv(logger,df1=df_csv,df2=df_scrape)
            
            if not not_in_csv.empty:
            
                with open(self.csv_path, 'a') as f:
                    rows_added = str(not_in_csv.shape[0])
                    not_in_csv.to_csv(f, header=False,index=False)
                    logger.info('added '+str(rows_added)+' new rows to csv')
                
            else:   
                logger.info('no new csv data')
                
        else:
            #if the file does not exists, then save it and wait for the next day
            df_scrape.to_csv(self.csv_path, header=True,index=False)
            logger.info('fist scrape/insert. Added '+str(len(df_scrape))+' rows to csv')
        return(None)
        
    def insert_database(self,df_scrape, df_database,table,logger, connection):
        sql_table = str(table)
        try:
            #s = 'select * from [dbo].'+sql_table
            #nymex_db = pd.read_sql_query(s,connection)
            #fix the datatypes
 
            not_in_db = self.return_not_in_csv(df1=df_database,df2=df_scrape)
            
            if not not_in_db.empty:
                rows_added = str(not_in_db.shape[0])
                not_in_db.to_sql(sql_table, connection, if_exists='append', index=False,chunksize=100)
                logger.info('added '+str(rows_added)+' new rows to database')
            else:
                logger.info('no new database data')
    
        except:
            #no rows are returned (the table is empty)
            logging.info('database query or insert error')
    
    #the following 2 functions are used mainly to check the datatypes of the saved data, and to see what has already been scraped    
    def return_saved_csv(self, logger):
        
        try:
            if os.path.isfile(self.csv_path):
                 with open(self.csv_path, 'r') as f:
                     df_csv = pd.read_csv(f)
                     return(df_csv)
        except:
            logger.info('csv does not exist, or is empty')
            return(None)
    
    def return_saved_table(self,table,logger,connection):
        sql_table = str(table)
        try:
            s = 'select * from [dbo].'+sql_table
            db = pd.read_sql_query(s,connection)
            return(db)
        except:
            logger.info('database table does not exist')
            raise Exception('database table does not exist')
        