import os
import lxml.html
import psycopg2
import setttings.py

conn = psycopg2.connect(
    dbname=os.environ.get("DATABASE_NAME"),
    user=os.environ.get("DATABASE_USER"),
    password=os.environ.get("DATABASE_PASSWORD"),
    host=os.environ.get("DATABASE_HOST"),
    port=os.environ.get("DATABASE_PORT")
    )
# S3からファイルをダウンロードかイベントを検知して、ファイルを読み込む
file_name = 'result.html'

raw_data = open('result.html', 'r').read()
doc = lxml.html.fromstring(raw_data)
service_name = 'shuumatsu-worker'
expire_date = ''
url = os.environ.get("COOPERATIVE_SERVICE")
path = file_name

price = doc.xpath("/html/body/section/div[2]/div/div[2]/div/div[5]/dl[2]/dd").text()
title = doc.xpath("/html/body/section/div[2]/div/div[2]/div/div[2]/h2").text()

cursor = conn.cursor()
records = cursor.execute("SELECT * FROM crawler_data WHERE id=%s", FILE_NAME)

if records.fetchone():
  # 更新
  cursor.execute("UPDATE crawlers SET raw_data = %s, title = %s, price = %s WHERE id = %s", raw, title, price, id)
else:
  # 新
  cursor.execute("INSERT INTO crawlers (raw_data, title, price, path, url, service_name, expire_date) VALUES (%s, %s, %s, %s, %s, %s, %s)", (raw_data, title, price, path, url, service_name, expire_date))

# conn.commit()
conn.close()
