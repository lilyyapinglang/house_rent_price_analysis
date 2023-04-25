
# -*- coding: UTF-8 -*-

# 导入需要的各种库
import re
import time
import requests
from random import uniform
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import sqlite3
from requests.adapters import HTTPAdapter
from fake_useragent import UserAgent
import json
import os 

# 设置一个请求头，不然无法通过链家的验证
#headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'}

ua=UserAgent()
headers={  
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',  
    'Accept-Encoding': 'gzip, deflate, sdch',  
    'Accept-Language': 'zh-CN,zh;q=0.8',  
    'User-Agent': str(ua.random),
    'Connection': 'close'
    }  

def get_parent_url(city):
    """
    获取选定城市的所有父母网址
    :param city:
    :return:
    """
    url = 'http://{city}.lianjia.com/ershoufang'.format(city=city)
    requests.adapters.DEFAULT_RETRIES = 15
    # 设置连接活跃状态为False 
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=3)) 
    s.mount('https://', HTTPAdapter(max_retries=3))
    # 在连接时关闭多余连接 
    s.keep_alive = False 

    html = requests.get(url, headers=headers, timeout=(100,100))                                           # 获取网址html
    Soup = BeautifulSoup(html.text, 'lxml')                                             # 解析html
    #get all region names
    region_lis = Soup.select('div[data-role="ershoufang"] > div > a')                  # 找出html中的区域文本
    #<li class="filter__item--level2" data-id="310118" data-type="district">
    #<a href="/zufang/qingpu/">青浦</a>
    url_parent_all = []                                                                 # 初始化最终的父母网址列表

    for i in region_lis:                                                               # 对每一个区域进行loop
        url_region = "https://sh.lianjia.com" + i['href']               # 找出区域网址
        print(i.text)
        print('url_region:  {}'.format(url_region))
        html_region = requests.get(url_region, headers=headers, timeout=(60,60),verify=False)                         # 获取区域网址html
        Soup_region = BeautifulSoup(html_region.text, 'lxml')                           # 解析html
        pages_data_element = Soup_region.select('div.page-box.house-lst-page-box')
        if pages_data_element:
            pages_data= int(json.loads(pages_data_element[0]['page-data'])['totalPage'])  
            print('total pages for region {} : {}'.format(url_region,pages_data))
            if pages_data < 100:  
                for j in range(1, pages_data+1):                                            # 对每一页网址进行循环
                    url_parent = url_region + "pg{}".format(j)
                    url_parent_all.append(url_parent)                                       # 得到该区域每一页的网址，并添加至父母网址列表中
                    print(url_parent)
                print('Got all {} page urls of region {} appened. Sleep for 2 seconds'.format(pages_data,i.text))
                html_region.close() 
            else: # when page data >=100
                print('Pages for region {} exceeds 100 pages, filter by subarea'.format(i.text))
                subareas=Soup_region.find('div', {'data-role': 'ershoufang'}).find_all('div')[1].find_all('a')
                for i in subareas:
                    url_subarea = "https://sh.lianjia.com" + i['href']
                    print('url_region_subarea:  {}'.format(url_subarea))
                    html_subarea = requests.get(url_subarea, headers=headers, timeout=(60,60),verify=False)                         # 获取区域网址html
                    Soup_subarea = BeautifulSoup(html_subarea.text, 'lxml')
                    pages_data_element = Soup_subarea.select('div.page-box.house-lst-page-box')
                    if pages_data_element:
                        pages_data= int(json.loads(pages_data_element[0]['page-data'])['totalPage'])
                        assert pages_data < 100, "subarea pages_data exceeds 100"  
                        print('total pages for subarea {} : {}'.format(url_subarea,pages_data))
                        for j in range(1, pages_data+1):                                            # 对每一页网址进行循环
                            url_parent = url_subarea + "pg{}".format(j)
                            url_parent_all.append(url_parent)                                       # 得到该区域每一页的网址，并添加至父母网址列表中
                            print(url_parent)
                        print('Got all {} page urls of subarea {}. Sleep for 2 seconds'.format(pages_data,i.text))
                    html_subarea.close()
            #sleep for 1-3 seconds until grabbing next region.
            
            t = uniform(1, 3)
            time.sleep(t)   
        else: 
            print('Region {} has no page'.format(url_region))                                                                                # 每爬一个区域，稍作等待，避免被ban

            """                   
            lis = Soup.select('ul[data-el="filterPrice"] li.filter__item--level5.check')
            count = len(lis) 
            print(count)           
            print('--------------')                                                          # 信息条数大于3000按租金分层
            
            for i in range(1, count+1):
                url_region_rp = url_region + "rp{}/".format(i)
                print(url_region_rp)
                html_region_rp = requests.get(url_region_rp, headers=headers, timeout=(60,60),verify=False)
                Soup_region_rp = BeautifulSoup(html_region_rp.text, 'lxml')
                number_data = int(Soup_region_rp.select('span.content__title--hl')[0].text)
                if number_data>0:
                    index = Soup_region_rp.select('div.content__pg')[0]                     # 操作同上
                    index = str(index)
                    re_set = re.compile(r'data-totalpage="(.*?)"')
                    index = re.findall(re_set, index)[0]
                    for j in range(1, int(index) + 1):
                        url_parent = url_region + "rp{}/".format(i) + "pg{}".format(j)
                        url_parent_all.append(url_parent)
                        print(url_parent)
                html_region_rp.close()
            t = uniform(2, 3)
            time.sleep(t)
            """
    return url_parent_all

"""
def get_detail_url(url_parent_all):

    url_detail_all = []                                                                 # 创建最终的子网址列表

    for url in url_parent_all:
        print(url)                                                        # 对每一个父母网址进行for loop
        html = requests.get(url, headers=headers, timeout=(60,60), verify=False)
        Soup = BeautifulSoup(html.text, 'lxml')
        #Selector = Soup.select('div a.content__list--item--aside')                      # 解析并找出子网址bs4对象
        Selector = Soup.select('ul.sellListContent>li>a')
        print(Selector)
        for i in Selector:
            i = i['href']
            #i = 'http://{city}.lianjia.com'.format(city=city) + i                       # 对每一个bs4子网址对象循环，构建最终的子网址
            url_detail_all.append(i)                                                    # 添加到子网址列表
            print(i)
        html.close()
        t = uniform(2, 3)
        time.sleep(t)                                                                   # 每处理一条父母网址暂停t秒
    return url_detail_all
"""

def get_detail_url_refined(url_parent_all):
    """
    对每一个父母网址进行操作，获取最终详尽的子网址列表
    :param city:
    :return:
    """
    url_detail_all = []                                                                 # 创建最终的子网址列表

    processed_urls_file = "processed_urls_2ndhand.txt"  # File to save processed urls

    if os.path.exists(processed_urls_file):    # Load processed urls if file exists
        with open(processed_urls_file, "r") as f:
            processed_urls = set(f.read().splitlines())
    else:
        processed_urls = set()

    for url in url_parent_all:
        if url in processed_urls:
            continue  # Skip urls that have already been processed

        print(url)                                                        # 对每一个父母网址进行for loop
        try:
            html = requests.get(url, headers=headers, timeout=(60,60), verify=False)
            html.raise_for_status()  # Raise an exception if                                                 the response code is not 200
            Soup = BeautifulSoup(html.text, 'lxml')
            Selector = Soup.select('ul.sellListContent>li>a')                      # 解析并找出子网址bs4对象
            for i in Selector:
                i = i['href']
                i = 'http://{city}.lianjia.com'.format(city=city) + i                       # 对每一个bs4子网址对象循环，构建最终的子网址
                url_detail_all.append(i)                                                    # 添加到子网址列表
                print(i)
            html.close()
            processed_urls.add(url)   # Add the processed url to the set
        except requests.exceptions.RequestException as e:
            print(f"Error processing url: {url}. {e}")  # Print the error message
            break  # Exit the loop if an error occurs

        t = uniform(2, 3)
        time.sleep(t)                                                                   # 每处理一条父母网址暂停t秒

        # Save processed urls to file after each iteration
        with open(processed_urls_file, "w") as f:
            f.write("\n".join(processed_urls))

    return url_detail_all

def to_mysql(df, table_name):
    """
    将爬取的数据保存到mysql中
    :param df:
    :param table_name:
    :return:
    """
    dbfile = './db/test.db'
    # Create a SQL connection to our SQLite database
    conn = sqlite3.connect(dbfile)
    # 将传入的df保存为mysql表格
    df.to_sql(name='{}'.format(table_name), con=conn, if_exists='replace', index=False)
    conn.close()

def to_csv(df, table_name):
    """
    将爬取的数据保存到csv文件中
    :param df:
    :param table_name:
    :return:
    """
    df.to_csv('{}'.format(table_name), index=False) 




if __name__ == '__main__':
    """
    test_url= 'https://sh.lianjia.com/ershoufang/chunshen/pg2/'
    test_html = requests.get(test_url, headers=headers, timeout=(60,60), verify=False)
    Soup = BeautifulSoup(test_html.text, 'lxml')   
    Selector = Soup.select('ul.sellListContent>li>a')
    print(Selector[0]['href'])
    """
    city = input("输入要爬取的城市拼音缩写(小写): ")
    # city = 'sh'
   
    url_parent_all = get_parent_url(city)
    parnet_url_csv=pd.DataFrame(url_parent_all, columns=['page_urls'])
    to_csv(parnet_url_csv, '{}_all_page_urls+2ndhand'.format(city))

    url_detail_all = get_detail_url_refined(url_parent_all)
    all_url = pd.DataFrame(url_detail_all, columns=['url'])
    to_mysql(all_url, '{}_all_url_2ndhand'.format(city))
    to_csv(all_url, '{}_all_url_2ndhand'.format(city))