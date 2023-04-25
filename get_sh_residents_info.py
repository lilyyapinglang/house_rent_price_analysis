
# https://tjj.sh.gov.cn/tjnj/nj22.htm?d1=2022tjnj/C0202.htm

# 2022 Shanghai Statistical Year Book

import sqlite3


conn = sqlite3.connect('./db/house_rent_lianjia.db')
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS district_data
             (district TEXT, area REAL, population REAL, density REAL, households INTEGER)''')
data = [('浦东', 1210.41, 576.77, 242.09, 4765),
        ('黄浦', 20.46, 58.21, 24.20, 28451),
        ('徐汇', 54.76, 111.48, 33.63, 20358),
        ('长宁', 38.30, 69.57, 22.18, 18164),
        ('静安', 36.88, 96.60, 24.50, 26193),
        ('普陀', 54.83, 124.36, 36.51, 22681),
        ('虹口', 23.48, 71.48, 19.49, 30443),
        ('杨浦', 60.73, 123.05, 30.57, 20262),
        ('闵行', 370.75, 267.32, 121.50, 7210),
        ('宝山', 270.99, 225.01, 89.15, 8303),
        ('嘉定', 464.20, 185.48, 102.81, 3996),
        ('金山', 586.05, 81.52, 29.93, 1391),
        ('松江', 605.64, 193.88, 111.61, 3201),
        ('青浦', 670.14, 129.27, 73.16, 1929),
        ('奉贤', 687.39, 114.71, 58.76, 1669),
        ('崇明', 1185.49, 60.73, 11.90, 512)]

c.executemany('INSERT INTO district_data VALUES (?, ?, ?, ?, ?)', data)
conn.commit()
conn.close()


