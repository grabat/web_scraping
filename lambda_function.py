import os
import lxml.html
import psycopg2
import settings
import boto3
import botocore
import logging
import re
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ActiveRecord:
    def __init__(self, args={}):
        self.conn = psycopg2.connect(
            dbname=os.environ.get("DATABASE_NAME"),
            user=os.environ.get("DATABASE_USER"),
            password=os.environ.get("DATABASE_PASSWORD"),
            host=os.environ.get("DATABASE_HOST"),
            port=os.environ.get("DATABASE_PORT")
        )
        self.after_initialize(args)

    def save(self):
        pass

    def close(self):
        pass

    def cursor(self):
        return(self.conn.cursor())

    def after_initialize(self, args):
        pass

    __cursor = cursor


class CrawlerData(ActiveRecord):
    def after_initialize(self, args):
        self.raw_data = args['raw_data']
        self.title = args['title']
        self.price = args['price']
        self.path = args['path']
        self.url = args['url']
        self.expire_date = args['expire_date']
        self.service_name = args['service_name']

    def update(self):
        cur = self.cursor()
        cur.execute(
            "UPDATE crawler_data "
            "SET raw_data = %s, title = %s, price = %s, updated_at = now() WHERE id = %s",
            self.raw_data,
            self.title,
            self.price,
            id)
        self.conn.commit()

    def save(self):
        cur = self.cursor()
        cur.execute(
            "INSERT INTO crawler_data ("
            "raw_data, title, price, path, url, service_name, expire_date, updated_at, created_at"
            ") VALUES ("
            "%s, %s, %s, %s, %s, %s, %s, %s, %s"
            ")",
            (self.raw_data,
             self.title,
             self.price,
             self.path,
             self.url,
             self.service_name,
             self.expire_date,
             datetime.now(),
             datetime.now()))
        self.conn.commit()

    def close(self):
        self.conn.close()

    def exists(self, key, value):
        records = self.cursor().execute(
            "SELECT * FROM crawler_data WHERE `%s`=%s",
            key, value)
        return records is not None


class CooperativeService:
    def __init__(self, file_name):
        self.file_name = file_name
        self.service_name = os.environ.get("COOPERATIVE_SERVICE")
        self.url = os.environ.get(
            "COOPERATIVE_SERVICE_URL") + "/" + primary_id()
        self.expire_date = None
        self.path = ''

    def call(self):
        pass

    def scraping(self, file_name):
        self.raw_data = open(file_name, 'r').read()
        doc = lxml.html.fromstring(self.raw_data)
        self.price = doc.xpath(
            "/html/body/section/div[2]/div/div[2]/div/div[5]/dl[2]/dd")[0].text
        self.title = doc.xpath(
            "/html/body/section/div[2]/div/div[2]/div/div[2]/h2")[0].text
        crawler_data = CrawlerData(self.__dict__)
        crawler_data.save()

    def primary_id(self):
        re.match(r"\d*_(\d*)\.html", file_name).groups()[0]


def lambda_handler(event, context):
    file_name = event['Records'][0]['s3']['object']['key']
    BUCKET_NAME = event['Records'][0]['s3']['bucket']['name']
    s3 = boto3.resource('s3')
    file_path = "/tmp/" + file_name
    cooperative_service = CooperativeService(file_name)

    print("run scraping lambda")

    try:
        s3.Bucket(BUCKET_NAME).download_file(file_name, file_path)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

    cooperative_service.scraping(file_path)
    print("success!!")
