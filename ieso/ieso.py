#!/usr/bin/env python3
import pandas as pd
import requests
from bs4 import BeautifulSoup
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

def brackets(text,l,r):
    if text.find(str(l)) != -1 and text.find(str(r)) != -1:
        return(text[text.find(str(l))+1:text.find(str(r))])
    else:
        return('')

def ieso_links(url):
    r = requests.get(url, allow_redirects=True, stream=True, headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')
    links  = soup.findAll('a')
    link_list = []
    for link in links:
        link = str(link)
        if link.find('csv') != -1 and link.find('_v') == -1:
            l = brackets(link,'=','>')
            l = l.replace('"','')
            link_list.append(l)
    return(link_list)
    
url = 'http://reports.ieso.ca/public/Demand/'
l = ieso_links(url)
l