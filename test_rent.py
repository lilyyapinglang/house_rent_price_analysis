import requests
from bs4 import BeautifulSoup
import time
import csv
import pandas as pd
import requests
from random import uniform
import sqlite3


def get_data(url_detail_all):
    """
    从子网址列表中网址获取数据
    :param url_detail_all:
    :return:
    """
    data = []                                                                           # 初始化一个爬虫数据列表
    num_error = 0 
    offline_count=0                                                                      # 记录错误数
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
            
            offline_div = Soup.find('div', {'class': 'offline w1150'})
            if offline_div:
                offline_count += 1  # add 1 to the count if offline_div exists
                error_msg = "This house is taken offline."
                raise AssertionError(error_msg)
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
        except Exception as e:
            num_error += 1
            print("oops, some errors occured")
            print(f"Error occurred while processing {i}. {str(e)}")
        continue
    print("出错数据行数: %d" % (num_error))
    print("房屋下架数量: %d" % (offline_count))
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

if __name__ ==  '__main__':
    with open('/Users/Yaping.Lang/Documents/GitCode/house_rent_price/db/diff_unique_urls_between1st_and_2nd.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
    
        # Create an empty list to store the 'url' values
        url_list = []
    
        # Loop through each row in the CSV file and append the 'url' value to the list
        for row in reader:
            url_list.append(row['url'])
        
        # Print the list of 'url' values
        print(url_list)
    urls = url_list
    
    df = get_data(urls)
    city='sh'
    to_mysql(df, '{}_home_data_missing_diff_1and2'.format(city))


