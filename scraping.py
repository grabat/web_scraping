import os
import lxml.html
import psycopg2
import setttings.py


class ActiveRecord:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=os.environ.get("DATABASE_NAME"),
            user=os.environ.get("DATABASE_USER"),
            password=os.environ.get("DATABASE_PASSWORD"),
            host=os.environ.get("DATABASE_HOST"),
            port=os.environ.get("DATABASE_PORT")
        )

    def save(self):
        pass

    def close(self):
        pass


class CrawlerData(ActiveRecord):
    def update(self):
        cursor.execute(
            "UPDATE crawlers "
            "SET raw_data = %s, title = %s, price = %s WHERE id = %s",
            raw_data,
            title,
            price,
            id)

    def save(self):
        cursor.execute(
            "INSERT INTO crawlers ("
            "raw_data, title, price, path, url, service_name, expire_date"
            ") VALUES ("
            "%s, %s, %s, %s, %s, %s, %s"
            ")",
            (raw_data,
             title,
             price,
             path,
             url,
             service_name,
             expire_date))

    def close(self):
        self.conn.close


class Scraping:
    cralwer_data = CrawlerData()
    # S3からファイルをダウンロードかイベントを検知して、ファイルを読み込む
    file_name = 'result.html'
    raw_data = open(file_name, 'r').read()
    doc = lxml.html.fromstring(raw_data)
    service_name = os.environ.get("COOPERATIVE_SERVICE")
    url = os.environ.get("COOPERATIVE_SERVICE_URL")
    expire_date = ''
    path = file_name

    price = doc.xpath(
        "/html/body/section/div[2]/div/div[2]/div/div[5]/dl[2]/dd").text()
    title = doc.xpath(
        "/html/body/section/div[2]/div/div[2]/div/div[2]/h2").text()

    cursor = active_record.conn.cursor()
    records = cursor.execute(
        "SELECT * FROM crawler_data WHERE id=%s",
        file_name)

    if records.fetchone():
        cralwer_data.update()
    else:
        cralwer_data.create()
