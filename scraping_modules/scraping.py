
import os
import logging
import json
import urllib
from sqlalchemy import create_engine
from sqlalchemy import exc
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.firefox.options import Options


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
        logger.info('created the logger')
        return(logger)
    
    def scrape_database(config_file,logger):
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
            logger.info('connected to database ',exc_info=True)
            return(connection)
        except:
            logger.info('error with database connection ',exc_info=True)
            return(None)
            
    
    def scrape_driver(driver_path,logger, browser, headless = True):
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
    
    
    def config_file(config_file,logger):
        __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname('__file__')))
        
        try:
            with open(os.path.join(__location__,config_file)) as f:
                config = json.load(f)
                logger.info('imported the config file')
                return(config)

        except:
            logger.info('error with config file ',exc_info=True)
            return(None)