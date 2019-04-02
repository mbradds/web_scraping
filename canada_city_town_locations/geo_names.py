import pandas as pd
import requests
import json
import io
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
from Documents.web_scraping.scraping_modules import scraping as sc


#get a list of all available provinces
class nrcan_locations:
    
    def __init__(self,url):
        self.url = url

    def geo_provinces(self):
        prov_list = []
        r = requests.get(self.url, allow_redirects=True, stream=True, headers=headers).json()
    
        for x in r['definitions']:
            prov_dict = ({'code':x['code'],'abbrev':x['term'],'description':x['description']})
            prov_list.append(prov_dict)
        
        return(prov_list)
        
    #create a list of urls for the API
    def create_urls(self,provinces,type_list = ['CITY','TOWN','UTM','LTM','MUN1']):
        url_list = []
        base_url = 'http://geogratis.gc.ca/services/geoname/en/geonames.json?province=PROV&num=1000&concise=TYPE&theme=985'
        
        for prov in provinces:
                  
            for t in type_list:
                p = prov['code']
                url = base_url.replace('PROV',p)
                if p == '12':
                    url = url.replace('&concise=TYPE','')
                    url_list.append(url)
                else:
                    url = url.replace('TYPE',t)
                    url_list.append(url)
                    
        
        url_list = list(dict.fromkeys(url_list))
        url_list.append('http://geogratis.gc.ca/services/geoname/en/geonames.json?q=halifax')
        url_list.append('http://geogratis.gc.ca/services/geoname/en/geonames.json?q=etobicoke')
        url_list.append('http://geogratis.gc.ca/services/geoname/en/geonames.json?q=north york')
        url_list.append('http://geogratis.gc.ca/services/geoname/en/geonames.json?q=yarmouth')
        url_list.append('http://geogratis.gc.ca/services/geoname/en/geonames.json?q=truro')
        return(url_list)
        
    #return json/dictionary data for the specified url   
    def return_locations(self,urls):
        location_list = []
        
        for url_ in urls:
            count = 0
    
            c = requests.get(url_, allow_redirects=True, stream=True, headers=headers).json()
    
            for x in c['items']:
                location_dict = ({'name':x['name'],'status':x['status']['code'],'concise':x['concise']['code'],
                                      'location':x['location'],'province_code':x['province']['code'],'latitude':x['latitude'],
                                     'longitude':x['longitude']})
                count = count+1
                
                if count >= 1000:
                    None
                    #raise Exception('Over 1000 locations returned. Data may be truncated due to API limits')
                location_list.append(location_dict)
            
        return(location_list)
    
    def canada_dataframe(self,locations_json,prov_list):
        provinces = pd.DataFrame(prov_list)
        locations = pd.DataFrame(locations_json)
        df = pd.merge(right = provinces, left = locations, how='left', left_on='province_code', right_on='code')
        return(df)
    
if __name__ == "__main__":
    #set up the scraper
    direc = r'/home/grant/Documents/web_scraping/canada_city_town_locations'
    nrcan_setup = sc.scrape(direc)  
    logger = nrcan_setup.scrape_logger('nrcan.log')
    connection = nrcan_setup.scrape_database('database.json',logger,work=False)
    ins = sc.insert(direc) 
    #request the location data
    nrcan = nrcan_locations(url = 'http://geogratis.gc.ca/services/geoname/en/codes/province.json')
    prov_list = nrcan.geo_provinces()
    urls = nrcan.create_urls(prov_list)
    locations_json = nrcan.return_locations(urls)
    canada = nrcan.canada_dataframe(locations_json,prov_list)
    #insert to database
    ins.insert_database(canada,'nrcan_locations',logger,connection,insert_type = 'replace')
    
  
#%% 
