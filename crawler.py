
import requests
from lxml import html
import re
import json 
import pandas as pd
import time
import glob

import os
path = "CheckPoints"
try:
    os.mkdir(path) 
except FileExistsError: pass 

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'}
it_link = 'https://www.shoprite.co.za/search?q=%3AsearchRelevance%3AbrowseAllStoresFacetOff%3AbrowseAllStoresFacetOff&page='


class url_links():
    def main_nodes():
        y , dy = 0 , 20
        n = 11750 #total items
        links = [it_link+'0']
        for i in range(0,n-20,dy):
            y = y + dy 
            pg = int(y/dy)
            link = it_link+f'{pg}'
            links.append(link)
        return links




class item_data():
    def mkpg_tree(pgno=0):
        response = requests.get( it_link+f'{pgno}', headers=headers)
        tree = html.fromstring(response.content)
        string = response.text
        pattern = '"price":{ "value":'
        n = len([match.start() for match in re.finditer(pattern, string)]) 
        return tree, n
    
    def xpath_search(tree,path):
        elements = tree.xpath(path)
        attributes_ = []
        for element in range(len(elements)):
            attributes_ = elements[element].attrib
        return attributes_

    def get_pg_data(pgno):
        pg_data = item_data.mkpg_tree(pgno)
        n       = pg_data[1] + 2
        print("There are {} items in this page.".format(pg_data[1]))
        tree    = pg_data[0]
        name_list , item_id_list , price_list , item_links_list , img_links_list = [] , [] , [] , [] , [] 
        if n-2> 0:
             
            for i in range(2,n):
                path = f'/html/body/main/div[4]/div[1]/div[2]/div/div/div/div[2]/div[{i}]'
                attributes = json.loads(item_data.xpath_search(tree,path)['data-product-ga'])
                name, item_id, price = attributes["name"], attributes["id"], attributes["price"]
                
                path = f'/html/body/main/div[4]/div[1]/div[2]/div/div/div/div[2]/div[{i}]/div/figure/figcaption/div/h3/a'
                item_links = 'https://www.shoprite.co.za' + item_data.xpath_search(tree,path)['href']
                
                path = f'/html/body/main/div[4]/div[1]/div[2]/div/div/div/div[2]/div[{i}]/div/figure/div/a/img'
                img_links = 'https://www.shoprite.co.za' + item_data.xpath_search(tree,path)['data-original-src']
                
                
                name_list.append(name)
                item_id_list.append(item_id)
                price_list.append(float(price))
                item_links_list.append(item_links)
                img_links_list.append(img_links)
               
            
        return (name_list, item_id_list, price_list, item_links_list, img_links_list)

    def get_dataframe(pgno):
        itemz = pd.DataFrame(item_data.get_pg_data(pgno)).T
        return itemz

    def all_items(x = 0):
        itemz = item_data.get_dataframe(pgno=0)
        stop = False
        if len(item_data.get_pg_data(pgno=2)[0])== 0:
            stop == True
        
        "This limits iterations"
        dx = 1
        while stop == False:
            x = x + dx
            "This limits iterations"
            if x > 0:
                print(x)
                "This limits iterations"
                if len(item_data.get_pg_data(pgno=x)[0]) == 0 or x ==100000:
                    stop = True
                
                current_itemzpg = item_data.get_dataframe(pgno=x)
                itemz = itemz.append(current_itemzpg)
                itemz_cp = itemz.rename(columns={0:'Names', 1: 'IDs', 2: 'Prices', 3: 'Item Links', 4: 'Image Links'})
                
                files = glob.glob('CheckPoints/*')
                for f in files:
                    if f != 'CheckPoints\\Checkpoint0{}.csv'.format(x-1):
                      os.remove(f)
                      print(f)
                itemz_cp.to_csv(r'CheckPoints/Checkpoint0{}.csv'.format(x), index = False)
                
                if int(x)%100==0:
                    print('Paused...')
                    time.sleep(7)
                
         
        return print('Done Crawling')

item_data.all_items()
# test = item_data.get_dataframe(pgno=2)



