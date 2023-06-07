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



# 设置一个请求头，不然无法通过链家的验证
#headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'}

"""
user_agent=['Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.87 Safari/537.36',  
    'Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10',  
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',  
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36',  
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER',  
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)',  
    ] 
    'User-Agent': user_agent[random.randint(0,5)] 
"""
ua=UserAgent()
headers={  
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',  
    'Accept-Encoding': 'gzip, deflate, sdch',  
    'Accept-Language': 'zh-CN,zh;q=0.8',  
    'User-Agent': str(ua.random)  
    }  

def get_parent_url(city):
    """
    获取选定城市的所有父母网址
    :param city:
    :return:
    """
    url = 'http://{city}.lianjia.com/zufang'.format(city=city)
    #requests.adapters.DEFAULT_RETRIES = 15
    # 设置连接活跃状态为False
    

    
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=3)) 
    s.mount('https://', HTTPAdapter(max_retries=3)) 
    s.keep_alive = False  # 在连接时关闭多余连接

    html = requests.get(url, headers=headers, timeout=(60,60))                                           # 获取网址html
    Soup = BeautifulSoup(html.text, 'lxml')                                             # 解析html
    Selector = Soup.select('ul[data-target="area"] > li.filter__item--level2')          # 找出html中的区域文本
    Selector = Selector[1:]                                                             # 排除第一个区域“不限”
    url_parent_all = []                                                                 # 初始化最终的父母网址列表

    for i in Selector:                                                                  # 对每一个区域进行loop
        url_region = "https://sh.lianjia.com" + i.select('a')[0]['href']                # 找出区域网址
        print('url_region:  {}'.format(url_region))
        html_region = requests.get(url_region, headers=headers, timeout=(60,60),verify=False)                         # 获取区域网址html
        Soup_region = BeautifulSoup(html_region.text, 'lxml')                           # 解析html
        number_data = int(Soup_region.select('span.content__title--hl')[0].text)        # 获取该区域信息条数
        
        if number_data <= 3000:  
            print(Soup_region.select('div.content__pg'))                                                       # 信息条数少于3000直接开始爬取
            index = Soup_region.select('div.content__pg')[0]                            # 找出页数文本
            index = str(index)                                                          # 将bs4对象转换为str，否则无法进行正则提取
            re_set = re.compile(r'data-totalpage="(.*?)"')
            index = re.findall(re_set, index)[0]                                        # 正则表达式提取出页数
            for j in range(1, int(index)+1):                                            # 对每一页网址进行循环
                url_parent = url_region + "pg{}".format(j)
                url_parent_all.append(url_parent)                                       # 得到该区域每一页的网址，并添加至父母网址列表中
                print(url_parent)
            html_region.close() 
            t = uniform(1, 2)
            time.sleep(t) 
                                                                                  # 每爬一个区域，稍作等待，避免被ban
        else:
            print('number of records: {}'.format(number_data))                                                                          # 信息条数大于3000按租金分层
            for i in range(1, 8):
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
            t = uniform(1, 2)
            time.sleep(t)
    return url_parent_all


def get_detail_url(url_parent_all):
    """
    对每一个父母网址进行操作，获取最终详尽的子网址列表
    :param city:
    :return:
    """
    url_detail_all = []                                                                 # 创建最终的子网址列表

    for url in url_parent_all:
        print(url)                                                        # 对每一个父母网址进行for loop
        html = requests.get(url, headers=headers, timeout=(60,60), verify=False)
        Soup = BeautifulSoup(html.text, 'lxml')
        Selector = Soup.select('div a.content__list--item--aside')                      # 解析并找出子网址bs4对象
        for i in Selector:
            i = i['href']
            i = 'http://{city}.lianjia.com'.format(city=city) + i                       # 对每一个bs4子网址对象循环，构建最终的子网址
            url_detail_all.append(i)                                                    # 添加到子网址列表
            print(i)
        t = uniform(0, 0.01)
        time.sleep(t)                                                                   # 每处理一条父母网址暂停t秒
    return url_detail_all


def get_data(url_detail_all):
    """
    从子网址列表中网址获取数据
    :param url_detail_all:
    :return:
    """
    data = []                                                                           # 初始化一个爬虫数据列表
    num_error = 0                                                                       # 记录错误数
    for i in url_detail_all:                                                            # for loop对每一个网址进行爬取操作
        try:                                                                            # 使用try...except...方法防止循环中途出错退出
            info = {}
            url = i
            print(i)
            html = requests.get(url)
            #with open("output3.html", "w") as file:
                #file.write(html.text)
            Soup = BeautifulSoup(html.text, 'lxml')
            #with open("output1.html", "w") as file:
            #    file.write(str(Soup))
            #info['房源编号'] = Soup.select('i.house_code')[0].text
            info['link'] = i
            info['house_code'] = i.split('/')[-1].split('.')[0]
            
            breadcrumb = Soup.find('div', class_='bread__nav')
            links = breadcrumb.find_all('a')
            result = [link.text[:-2] for link in links][1:]


            info['district'] = result[0]
            info['subarea']= result[1]
            info['compound_name'] = result[2]
           
            info['title'] = Soup.select('p.content__title')[0].text.strip()
            info['price'] = Soup.select("#aside > div.content__aside--title > span")[0].text
       
            #aside > div.content__aside--title > span
            
            Selector1 = Soup.find(class_='content__aside__list').find_all('li')
            #lis=Soup.find(class_='content__aside__list').find_all('li')
            #print(lis[0].find(text=True, recursive=False).strip())
            #Selector1 = list(filter(None, Selector1))
            #print(Selector1)
            
            info['lease_mode'] = Selector1[0].find(text=True, recursive=False).strip()
            info['type_area'] = Selector1[1].find(text=True, recursive=False).strip()
            info['orient_floor'] = Selector1[2].find_all(text=True)[1]

            info['last_maintain_time'] = Soup.find(class_='content__subtitle').find(text=True, recursive=False).strip().split('：')[1]
            
            import re
            
            pattern_geo = r"\{\s*longitude:\s*'(-?\d+(?:\.\d+)?)',\s*latitude:\s*'(-?\d+(?:\.\d+)?)'\s*\}"
            def script_filter(tag):
                return tag.name == 'script' and 'g_conf.coord' in tag.text

            # find the script tag that contains "g_conf.coord"
            script_tag = Soup.find(script_filter)

            if script_tag:
                script_text = script_tag.text
                match=re.search(pattern_geo,script_text)
                if match:
                    longitude = match.group(1)
                    lattitude =match.group(2)
                    info["coordinate"]= longitude+','+lattitude
            else:
                print("No script tag containing 'g_conf.coord' found.")

            subway_distance_info= Soup.find('div', {'id': 'around'}).find_all('ul')[1]
            subway_list=[]
            for li in subway_distance_info.find_all("li"):
                spans=li.find_all('span')
                line,name=spans[0].get_text().rsplit('-', 1)
                distance=spans[1].get_text()
                current_str=str([line,name,distance])
                subway_list.append(current_str)
                 
            print(','.join(subway_list))
            info['metro']=','.join(subway_list)
            if info['metro'] == '':                                                   # 配套设施为空的情况
                info['metro'] = None

            #基本信息
            info['area'] = Soup.select('li[class^="fl oneline"]')[1].text[3:]
            info['orientation'] = Soup.select('li[class^="fl oneline"]')[2].text[3:]
            info['check_in'] = Soup.select('li[class^="fl oneline"]')[5].text[3:]
            info['floor'] = Soup.select('li[class^="fl oneline"]')[7].text[3:]
            info['has_elevator'] = Soup.select('li[class^="fl oneline"]')[8].text[3:]
            info['has_parking'] = Soup.select('li[class^="fl oneline"]')[10].text[3:]
            info['water_type'] = Soup.select('li[class^="fl oneline"]')[11].text[3:]
            info['electricity_type'] = Soup.select('li[class^="fl oneline"]')[13].text[3:]
            info['gas'] = Soup.select('li[class^="fl oneline"]')[14].text[3:]
            info['heating'] = Soup.select('li[class^="fl oneline"]')[16].text[3:]
           
            info['lease_period'] = Soup.select('li[class^="fl oneline"]')[18].text[3:]
            info['house_visit'] = Soup.select('li[class^="fl oneline"]')[21].text[3:]
            
            #info['house_tags'] = Soup.select('p.content__aside--tags')[0].text[1:-1].replace('\n', ', ')
            Selector2 =Soup.find('ul', class_='content__article__info2').find_all('li')
            tags = Soup.find('p', {'class': 'content__aside--tags'}).find_all(['img', 'i'])
            if not tags:
                info['house_tags']=None
            else:
                result = [tag.get('alt') or tag.get_text() for tag in tags]
                info['house_tags']=(',').join(result)
            


            info['facilities'] = []
            for i in Selector2:  
                #print(i['class'])                                                   # 对每一个对象进行处理
                if len(i['class'])==2 and i.text.strip()!='配套设施':
                                                             # 仅保留有的配套设施
                    
                    info['facilities'].append(i.text.strip())
                        
            info['facilities'] = ",".join(info['facilities'])                                # 列表转换为str
            if info['facilities'] == '':                                                   # 配套设施为空的情况
                info['facilities'] = None
            
            if not Soup.select('.threeline'):                                           # 房源描述为空的情况
                info['house_desc'] = None
            else:
                info['house_desc'] = Soup.select('.threeline')[0].text


            data.append(info)                                                           # 将爬取的信息以字典形式添加到数据列表中
            t = uniform(0, 0.01)
            time.sleep(t)                                                               # 爬一条数据暂停t秒
        except:
            num_error += 1
            print("oops, some errors occured")
        continue
    print("出错数据行数: %d" % (num_error))
    df = pd.DataFrame(data)                                                             # 将数据转换为DataFrame
    return df 

def to_mysql(df, table_name):
    """
    将爬取的数据保存到mysql中
    :param df:
    :param table_name:
    :return:
    """
    dbfile = './db/house_rent_lianjia.db'
    # Create a SQL connection to our SQLite database
    conn = sqlite3.connect(dbfile)
    # 创建连接mysql的engine，以pymysql为driver
    #f"mysql+pymysql://{username}:{password}@{host}:{port}"
    #engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}", encoding='utf-8')
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
    df.to_csv('{}'.format(table_name), index=False)                                     # 不保留行索引


if __name__ == '__main__':
    city = input("输入要爬取的城市拼音缩写(小写): ")
    # city = 'sh'
    url_parent_all = get_parent_url(city)
    url_detail_all = get_detail_url(url_parent_all)
    all_url = pd.DataFrame(url_detail_all, columns=['url'])
    to_mysql(all_url, '{}_all_url'.format(city))
    to_csv(all_url, '{}_all_url'.format(city))
    home_df = get_data(url_detail_all)
    to_mysql(home_df, '{}_home_data'.format(city))
    to_csv(home_df, '{}_home_data'.format(city))

