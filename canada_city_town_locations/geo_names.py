import pandas as pd
import requests
import json
import io
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

#get a list of all available provinces


def geo_provinces(url):
    prov_list = []
    r = requests.get(url, allow_redirects=True, stream=True, headers=headers).json()

    for x in r['definitions']:
        prov_dict = ({'code':x['code'],'abbrev':x['term'],'description':x['description']})
        prov_list.append(prov_dict)
    
    return(prov_list)
    
#create a list of urls for the API
def create_urls(provinces,type_list = ['CITY','TOWN']):
    url_list = []
    base_url = 'http://geogratis.gc.ca/services/geoname/en/geonames.json?province=PROV&num=1000&concise=TYPE'
    
    for prov in provinces:
              
        for t in type_list:
            p = prov['code']
            url = base_url.replace('PROV',p)
            url = url.replace('TYPE',t)
            url_list.append(url)
        
    return(url_list)
    
#return json/dictionary data for the specified url   
def return_locations(urls):
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
                raise Exception('Over 1000 locations returned. Data may be truncated due to API limits')
            
            location_list.append(location_dict)
        
    return(location_list)

def canada_dataframe(locations_json,prov_list):
    provinces = pd.DataFrame(prov_list)
    locations = pd.DataFrame(locations_json)
    df = pd.merge(right = provinces, left = locations, how='left', left_on='province_code', right_on='code')
    return(df)
    
    

if __name__ == "__main__":
    
    prov_list = geo_provinces('http://geogratis.gc.ca/services/geoname/en/codes/province.json')
    urls = create_urls(prov_list)
    locations_json = return_locations(urls)
    canada = canada_dataframe(locations_json,prov_list)
    #print(canada)
    