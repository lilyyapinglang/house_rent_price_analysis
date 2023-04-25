import requests
from bs4 import BeautifulSoup
import time

import pandas as pd
import requests
from random import uniform
import sqlite3
import os

import multiprocessing as mp
from multiprocessing import Pool
import tracemalloc
tracemalloc.start()

import asyncio
import aiohttp
import os
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
from multiprocessing import Pool, cpu_count

'''
def scrape_url(url):
    try:
        info = {}
        print('---------')
        print(url)

        html = requests.get(url)
        Soup = BeautifulSoup(html.text, 'lxml')
        offline_div = Soup.find('div', {'class': 'offline w1150'})
        if offline_div:
            error_msg = "This house is taken offline."
            raise AssertionError(error_msg)

        info['link'] = url
        info['title'] = Soup.find('h1', {'class': 'main'}).get('title')

        return info

    except Exception as e:
        print("oops, some errors occured")
        print(f"Error occurred while processing {url}. {str(e)}")
        return None

def get_data(urls, chunk_size=10):
    processed_urls_file_detail = "processed_urls_2ndhand_detail.txt"
    data_file = "scraped_data.csv"

    if os.path.exists(processed_urls_file_detail):
        with open(processed_urls_file_detail, "r") as f:
            processed_urls_detail = set(f.read().splitlines())
    else:
        processed_urls_detail = set()

    if os.path.exists(data_file):
        data = pd.read_csv(data_file)
    else:
        data = pd.DataFrame()

    pool_size = cpu_count() * 2
    with Pool(pool_size) as pool:
        for i, info in enumerate(pool.imap_unordered(scrape_url, urls)):
            if info is None:
                continue

            url = info['link']

            data = data.append(info, ignore_index=True)
            processed_urls_detail.add(url)

            if i % chunk_size == 0:
                data.to_csv(data_file, index=False)
                with open(processed_urls_file_detail, "w") as f:
                    f.write("\n".join(processed_urls_detail))
                    print(f"Processed {i+1} urls")
                    print(f"Data file size: {os.path.getsize(data_file)} bytes")
                    print(f"Processed urls file size: {os.path.getsize(processed_urls_file_detail)} bytes")

            t = uniform(0, 0.01)
            time.sleep(t)

    data.to_csv(data_file, index=False)
    with open(processed_urls_file_detail, "w") as f:
        f.write("\n".join(processed_urls_detail))

    return data


processed_urls_file_detail = "processed_urls_2ndhand_detail.txt"
data_file = "scraped_data.csv"

if os.path.exists(processed_urls_file_detail):
    with open(processed_urls_file_detail, "r") as f:
        processed_urls_detail = set(f.read().splitlines())
else:
    processed_urls_detail = set()

if os.path.exists(data_file):
    data = pd.read_csv(data_file)
else:
    data = pd.DataFrame()

async def scrape_url(session, url):
    if url in processed_urls_detail:
        return None

    try:
        async with session.get(url) as response:
            html = await response.text()
            Soup = BeautifulSoup(html, 'lxml')
            offline_div = Soup.find('div', {'class': 'offline w1150'})
            if offline_div:
                error_msg = "This house is taken offline."
                raise AssertionError(error_msg)

            info = {}
            info['link'] = url
            info['title'] = Soup.find('h1', {'class': 'main'}).get('title')
            info["价格"] = Soup.find("span", class_="total").text
            processed_urls_detail.add(url)
            return info

    except Exception as e:
        print(f"Error occurred while processing {url}. {str(e)}")
        return None

async def get_data(urls):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            task = asyncio.ensure_future(scrape_url(session, url))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        for info in results:
            if info is not None:
                data = data.append(info, ignore_index=True)
        data.to_csv(data_file, index=False)
        with open(processed_urls_file_detail, "w") as f:
            f.write("\n".join(processed_urls_detail))
    return data



def scrape_url(url):
    try:
        info = {}
        print('---------')
        print(url)

        html = requests.get(url)
        Soup = BeautifulSoup(html.text, 'lxml')
        offline_div = Soup.find('div', {'class': 'offline w1150'})
        if offline_div:
            error_msg = "This house is taken offline."
            raise AssertionError(error_msg)

        info['link'] = url
        info['title'] = Soup.find('h1', {'class': 'main'}).get('title')
        info["价格"] = Soup.find("span", class_="total").text

        return info
    except Exception as e:
        print("oops, some errors occured")
        print(f"Error occurred while processing {url}. {str(e)}")
        return None

def get_data(url_detail_all, chunk_size=10):
    processed_urls_file_detail = "processed_urls_2ndhand_detail.txt"
    data_file = "scraped_data.csv"

    if os.path.exists(processed_urls_file_detail):
        with open(processed_urls_file_detail, "r") as f:
            processed_urls_detail = set(f.read().splitlines())
    else:
        processed_urls_detail = set()

    if os.path.exists(data_file):
        data = pd.read_csv(data_file)
    else:
        data = pd.DataFrame()

    pool = Pool(processes=4) # Use 4 processes for scraping
    urls_to_scrape = [url for url in url_detail_all if url not in processed_urls_detail]

    for i, info in enumerate(pool.imap_unordered(scrape_url, urls_to_scrape)):
        if info is None:
            continue

        data = data.append(info, ignore_index=True)
        processed_urls_detail.add(info['link'])

        if i % chunk_size == 0:
            data.to_csv(data_file, index=False)
            with open(processed_urls_file_detail, "w") as f:
                f.write("\n".join(processed_urls_detail))
                print(f"Processed {i+1} urls")
                print(f"Data file size: {os.path.getsize(data_file)} bytes")
                print(f"Processed urls file size: {os.path.getsize(processed_urls_file_detail)} bytes")

    data.to_csv(data_file, index=False)
    with open(processed_urls_file_detail, "w") as f:
        f.write("\n".join(processed_urls_detail))

    return data

'''

def get_data(url_detail_all, chunk_size=10):
    processed_urls_file_detail = "processed_urls_2ndhand_detail_missing_2.txt"
    data_file = "scraped_data_missing_2.csv"

    if os.path.exists(processed_urls_file_detail):
        with open(processed_urls_file_detail, "r") as f:
            processed_urls_detail = set(f.read().splitlines())
    else:
        processed_urls_detail = set()

    if os.path.exists(data_file):
        data = pd.read_csv(data_file)
    else:
        data = pd.DataFrame()

    for i, url in enumerate(url_detail_all):
        if url in processed_urls_detail:
            continue

        try:
            info = {}
            print('---------')
            print(url)

            html = requests.get(url)
            Soup = BeautifulSoup(html.text, 'lxml')
            offline_div = Soup.find('div', {'class': 'offline w1150'})
            if offline_div:
                error_msg = "This house is taken offline."
                raise AssertionError(error_msg)

            info['link'] = url
            info['title'] = Soup.find('h1', {'class': 'main'}).get('title')
            info['subtitle'] = Soup.find('div', {'class': 'sub'}).get('title')
            info["价格"] = Soup.find("span", class_="total").text
            info["单价"] = Soup.find("span", class_="unitPriceValue").text
            info['house_build_year'] = Soup.find('div', {'class': 'area'}).find('div', {'class': 'subInfo'}).text.split('/')[0].strip()
            info["小区"] = Soup.find("div", class_="communityName").find("a", class_="info").text
            info["位置"] = Soup.find("div", class_="areaName").find("span", class_="info").text
            info["地铁"] = Soup.find("div", class_="areaName").find("a", class_="supplement").text
            info['house_code'] = Soup.find('div', {'class': 'houseRecord'}).find('span', {'class': 'info'}).text
            
            basic = Soup.find("div", class_ = "base").find_all("li") # 基本信息
            for li in basic:
                label= li.find('span',{'class':'label'}).text
                value=li.contents[-1].strip()
                info[label]=value

            transaction= Soup.find("div", class_ = "transaction").find_all("li") # 交易信息
            for li in transaction:
                label= li.find('span',{'class':'label'})
                value=label.find_next_sibling("span").text.strip()
                info[label.text]=value

            script = Soup.find('script', text=lambda t: t and 'resblockPosition' in t)
            script_text = script.text.strip()
            # extract the desired values
            isUnique = script_text.split("isUnique:'")[1].split("',")[0]
            resblockId = script_text.split("resblockId:'")[1].split("',")[0]
            resblockPosition = script_text.split("resblockPosition:'")[1].split("',")[0]
            info['isUnique']=isUnique
            info['resblockId']= resblockId
            info['resblockPosition']=resblockPosition
            print(info)

            data = data.append(info, ignore_index=True)
            processed_urls_detail.add(url)

            if i % chunk_size == 0:
                print(i)
                data.to_csv(data_file, index=False)
                with open(processed_urls_file_detail, "w") as f:
                    f.write("\n".join(processed_urls_detail))
                    print(f"Processed {i+1} urls")
                    print(f"Data file size: {os.path.getsize(data_file)} bytes")
                    print(f"Processed urls file size: {os.path.getsize(processed_urls_file_detail)} bytes")

            t = uniform(0, 1)
            time.sleep(t)
        except Exception as e:
            print("oops, some errors occured")
            print(f"Error occurred while processing {url}. {str(e)}")
            continue

    data.to_csv(data_file, index=False)
    with open(processed_urls_file_detail, "w") as f:
        f.write("\n".join(processed_urls_detail))

    return data

'''
def get_data(url_detail_all):  # sourcery skip: low-code-quality
    processed_urls_file_detail = "processed_urls_2ndhand_detail.txt"  # File to save processed urls

    if os.path.exists(processed_urls_file_detail):    # Load processed urls if file exists
        with open(processed_urls_file_detail, "r") as f:
            processed_urls_detail= set(f.read().splitlines())
    else:
        processed_urls_detail = set()

    for url in url_detail_all:
        if url in processed_urls_detail:
            continue  # Skip urls that have already been processed

        try:
            data = []                                                                           # 初始化一个爬虫数据列表
            num_error = 0
            offline_count=0                                                                      # 记录错误数
            for i in url_detail_all:                                                # for loop对每一个网址进行爬取操作
                try:                                                            # 使用try...except...方法防止循环中途出错退出
                    info = {}
                    url = i
                    print('---------')
                    print(i)

                    html = requests.get(url)

                    Soup = BeautifulSoup(html.text, 'lxml')
                    if offline_div := Soup.find(
                        'div', {'class': 'offline w1150'}
                    ):
                        offline_count += 1  # add 1 to the count if offline_div exists
                        error_msg = "This house is taken offline."
                        raise AssertionError(error_msg)
                    info['link'] = i

                    info['title'] = Soup.find('h1', {'class': 'main'}).get('title')
                    info['subtitle'] = Soup.find('div', {'class': 'sub'}).get('title')

                    info["价格"] = Soup.find("span", class_ = "total").text
                    info["单价"] = Soup.find("span", class_ = "unitPriceValue").text
                    info['house_build_year'] = Soup.find('div', {'class': 'area'}).find('div', {'class': 'subInfo'}).text.split('/')[0].strip()
                    info["小区"] = Soup.find("div", class_ = "communityName").find("a", class_ = "info").text
                    info["位置"] = Soup.find("div", class_="areaName").find("span", class_="info").text
                    info["地铁"] = Soup.find("div", class_="areaName").find("a", class_="supplement").text
                    info['house_code'] = Soup.find('div', {'class': 'houseRecord'}).find('span', {'class': 'info'}).text

                    basic = Soup.find("div", class_ = "base").find_all("li") # 基本信息
                    for li in basic:
                        label= li.find('span',{'class':'label'}).text
                        value=li.contents[-1].strip()
                        info[label]=value

                    transaction= Soup.find("div", class_ = "transaction").find_all("li") # 交易信息
                    for li in transaction:
                        label= li.find('span',{'class':'label'})
                        value=label.find_next_sibling("span").text.strip()
                        info[label.text]=value


                    script = Soup.find('script', text=lambda t: t and 'resblockPosition' in t)
                    script_text = script.text.strip()

                    # extract the desired values
                    isUnique = script_text.split("isUnique:'")[1].split("',")[0]
                    resblockId = script_text.split("resblockId:'")[1].split("',")[0]
                    resblockPosition = script_text.split("resblockPosition:'")[1].split("',")[0]

                    info['isUnique']=isUnique
                    info['resblockId']= resblockId
                    info['resblockPosition']=resblockPosition
                    print(info)

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
            df = pd.DataFrame(data)    

        except requests.exceptions.RequestException as e:
            print(f"Error processing url: {url}. {e}")  # Print the error message
            break  # Exit the loop if an error occurs                                                  # 将数据转换为DataFrame
    return df 
'''

def to_mysql(df, table_name):
    dbfile = './db/test.db'
    # Create a SQL connection to our SQLite database
    conn = sqlite3.connect(dbfile)
    df.to_sql(name=f'{table_name}', con=conn, if_exists='replace', index=False)
    conn.close()

if __name__ ==  '__main__':
    mp.set_start_method('spawn') 
    dbfile = './db/test.db'
    # Create a SQL connection to our SQLite database
    conn = sqlite3.connect(dbfile)
    c = conn.cursor()

    # Retrieve the URLs from the database
    c.execute('SELECT url FROM selldata_missing_2')
    urls = [row[0] for row in c.fetchall()]
    print(len(urls))
    # Close the database connection
    conn.close()

    df = get_data(urls)

    city='sh'
    # Pass the URLs to the get_data function

    to_mysql(df, f'{city}_home_data_2ndhand_missing2')


