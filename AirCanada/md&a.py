import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import requests
#%%

def Links(base_link='https://www.aircanada.com/content/dam/aircanada/portal/documents/PDF/en/quarterly-result/YYYY/YYYY_MDA_qQUARTER.pdf'):

    min_year = 2010
    now = datetime.datetime.now().date()
    current_date = now
    
    quarters = []
    
    while current_date.year >= min_year:
        
        q = (current_date.month-1)//3
        y = current_date.year
        if [q+1,y] not in quarters:
            quarters.append([q+1,y])
        
        current_date = current_date - relativedelta(months=1)
    
    links = []
    
    for mda_date in quarters:
        l = base_link 
        for r1,r2 in zip(mda_date,['QUARTER','YYYY']):
            
            l = l.replace(r2,str(r1))
        
        links.append(l)
    
    return links

def Download():
    
    links = Links()
    
    for link in links:
        
        
        file_name = link.split('/')[-1]
        print(file_name)
        
        response  = requests.get(link)
        
        try:
            
            with open('pdf/'+file_name,'wb') as f:
                f.write(response.content)
        
        except:
            print('failed: '+link)



if __name__ == '__main__':
    
    now = Download()

    