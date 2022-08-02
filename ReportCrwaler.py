import sqlite3
from bs4 import BeautifulSoup as bs
import re
import os
import shutil
import ssl
from urllib.request import urlopen
from urllib.parse import urlencode



# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('report_list.sqlite')
cur = conn.cursor()

# 创建储存报告的表格
cur.execute('''CREATE TABLE IF NOT EXISTS Reports
(id INTEGER PRIMARY KEY,
name TEXT UNIQUE,
link TEXT UNIQUE,
release_date TEXT,
org_id INTEGER,
count INTEGER,
status TEXT)''')

# 创建储存发布组织的表格
cur.execute('''CREATE TABLE IF NOT EXISTS Organizations
(id INTEGER PRIMARY KEY,
name TEXT UNIQUE,
end_num  INTEGER,
type_id INTEGER)''')

# 创建储存组织类型的表格
cur.execute('''CREATE TABLE IF NOT EXISTS TypeofOrg
(id INTEGER PRIMARY KEY,
type TEXT)''')

# 接下来的部分只有bain中国适用，其他网站可能得改改
cur.execute('''INSERT OR IGNORE INTO TypeofOrg
(id, type) VALUES(1, 'consulting')''')
cur.execute('''INSERT OR IGNORE INTO Organizations
(id, name, end_num, type_id) VALUES(1, 'Bain & Company', 0, 1)''')
org_id = 1

starturl = 'https://www.bain.cn/news_info.php?'
num = dict()

cur.execute('''SELECT end_num
FROM Organizations
WHERE id = 1''')
last_end = cur.fetchone()[0]
num['id'] = last_end
filepathstart = './files/'
if not os.path.exists(filepathstart[:-1]):
    os.mkdir(filepathstart[:-1])

nothing = 0
while True:
    num['id'] += 1
    if nothing >= 10:
        print('More than 10 webs no content! Automatic Exit.')
        break
    try:
        url = starturl + urlencode(num)
        urlcontent = urlopen(url, context = ctx)
        html = urlcontent.read()
        if urlcontent.getcode() != 200:
            print(url,'ERROR:',urlcontent.getcode())

        soup = bs(html,'html.parser')

    except KeyboardInterrupt:
        cur.execute('''UPDATE Organizations
        SET end_num = ? WHERE id = 1''',(num['id']-1,))
        print()
        print('Program interrupted by user...')
        print()

    except Exception as e:
        print(url,'Unable to retrieve', repr(e))
        continue

    title = soup.title.string
    if title == "贝恩公司":
        print(url, "Nothing here")
        nothing += 1
        continue

    nothing = 0
    cur.execute('''UPDATE Organizations
    SET end_num = ? WHERE id = 1''',(num['id'],))

    title = title.string
    a = title.find('-')
    title = title[a+1:]
    filelinks = soup.find_all('a', href = re.compile(".pdf"))
    if filelinks == []:
        print(url,"No pdf files")
        continue
    date = soup.find("div", class_ = "time").string
    cur.execute('''INSERT OR IGNORE INTO Reports
    (name, link, release_date,org_id,status)
    VALUES(?, ?, ?, ?, 'unread')
    ''',(title, url, date, org_id))
    cur.execute('''SELECT id
    FROM Reports
    WHERE link = ?''' , (url,))
    reportidone = cur.fetchone()
    if reportidone == None:
        continue

    reportid = reportidone[0]
    filepath = filepathstart + "%04d"%(reportid) + '.' + title
    if not os.path.exists(filepath):
        os.mkdir(filepath)

    missing = 0

    for filelink in filelinks:
        fileurl = 'https://www.bain.cn/'+filelink['href']
        if filelink.string == None:
            filename = "%04d"%(reportid) + '.' + title
        else:
            filename = "%04d"%(reportid) + '.' + title + '-' + filelink.string


        try:
            webfile = urlopen(fileurl, context = ctx)
            if len(webfile.read()) < 100:
                missing = missing + 1
            else:
                webfile = urlopen(fileurl, context = ctx)
                with open(filepath +'/' + filename + '.pdf','wb') as pdf:
                    shutil.copyfileobj(webfile,pdf)
        except:
            missing = missing + 1
    cur.execute('''UPDATE Reports
    SET count = ?
    WHERE link = ?''',(len(filelinks)-missing,url))
    print(url, reportid, title, 'file count:', len(filelinks)-missing)

    conn.commit()

cur.close()
