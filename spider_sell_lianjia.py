# -*- coding: utf-8 -*-
"""
Created on Wed Dec 20 10:44:38 2017

@author: huanglei
"""

# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import random  



def spider_1(url):
    
    user_agent=['Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.87 Safari/537.36',  
    'Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10',  
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',  
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36',  
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER',  
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)',  
    ]  
    headers={  
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',  
    'Accept-Encoding': 'gzip, deflate, sdch',  
    'Accept-Language': 'zh-CN,zh;q=0.8',  
    'User-Agent': user_agent[random.randint(0,5)]  
    }  
    response = requests.get(url,headers=headers)
    soup = BeautifulSoup(response.text,'lxml')
    page_array = []

    titles = soup.select('li.clear > div.info.clear > div.title > a')          # 标题
    hrefs = soup.select('ul.sellListContent > li.clear > a.img')
    details = soup.select("div.address > div.houseInfo")
    prices = soup.select("div.priceInfo > div.totalPrice > span")#解析总价
    danjias = soup.select("div.priceInfo > div.unitPrice > span")#解析单价
    loucengs = soup.select("div.info.clear > div.flood > div.positionInfo")
    addresss = soup.select("div.info.clear > div.flood > div.positionInfo > a")


    for title, href, detail, price, danjia, louceng, address in zip(titles, hrefs, details, prices, danjias, loucengs, addresss):
        data = {
            'title': title.get_text(),
            'href': href.get('href'),
            'detail': detail.get_text().strip(),
            'price': price.get_text(),
            'danjia': danjia.get_text(),
            'louceng': louceng.get_text(),
            'add': address.get_text(),
        }
        

            #print(float(data['price'])<170)
        data['court'] = data['detail'].split('|')[0].strip()
        data['area'] = data['detail'].split('|')[2][:-3]
        data['author'] = ""
        
        page_array.append(data)
    
    return page_array

        
def pandas_to_xlsx(info):
    pd_look = pd.DataFrame(info)
    xlsx_n = '链家二手房.xlsx'
    sheet_n = '武汉二手房'

    pd_look.to_excel(xlsx_n,sheet_name=sheet_n)

#返回有租房小区列表 
def add_youzufang(info):
    nlist=[]
    for index, row in info.iterrows():
        #for col_name in info.columns:
        nlist.append(index)
    return nlist

#修改买房者为租金单价
def chinese(info):
    info.rename(columns={"author":"租售比", "add":"地址","area":"平米","court":"小区名","danjia":"单价"}, inplace = True)
    info.rename(columns={"detail":"细节", "price":"总价","title":"标题"}, inplace = True)
    return info

# 读取租房信息并得到所有小区信息
df = pd.read_excel("链家武汉租房.xlsx")
df = df.drop_duplicates()
df_zufang = df.groupby('court').mean()
df_zufang.to_excel('租房均一化.xlsx','均一化')
nlist = add_youzufang(df_zufang)    


# 读取所有卖二手房信息并放在pd数据中    
page = 1
df_ershoufang =[]
while page < 300:
    url = 'https://wh.lianjia.com/ershoufang/sf1l1l2l3p1p2p3p4/pg'+str(page)
    try:
        df_ershoufang.extend(spider_1(url))    
        page = page + 1
    except:
        print("error:")
        break
    print(page)

pandas_to_xlsx(df_ershoufang)

df_ershoufang = pd.read_excel("链家二手房.xlsx")

for index, row in df_ershoufang.iterrows():
    if (row['court']) in nlist:

        df_ershoufang.at[index,'author']= df_zufang.at[row['court'],'danjia']
    else:
        df_ershoufang.drop(index,axis=0,inplace=True)

df_ershoufang['author'] = 0.12*df_ershoufang['author'].astype('int')/(df_ershoufang['price'].astype('int')/df_ershoufang['area'].astype('float'))
df_ershoufang = df_ershoufang.sort_values(by='author',ascending = False)
df_ershoufang = chinese(df_ershoufang)
pandas_to_xlsx(df_ershoufang)