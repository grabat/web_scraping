import os
import lxml.html
import psycopg2
import setttings.py
import boto3
import botocore


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
        self.conn.cursor

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
        self.service_name = args['service_name']

    def update(self):
        self.cursor.execute(
            "UPDATE crawlers "
            "SET raw_data = %s, title = %s, price = %s WHERE id = %s",
            self.raw_data,
            self.title,
            self.price,
            id)

    def save(self):
        self.cursor.execute(
            "INSERT INTO crawlers ("
            "raw_data, title, price, path, url, service_name, expire_date"
            ") VALUES ("
            "%s, %s, %s, %s, %s, %s, %s"
            ")",
            (self.raw_data,
             self.title,
             self.price,
             self.path,
             self.url,
             self.service_name,
             self.expire_date))
        return (self.cursor.row_count() == 1)

    def close(self):
        self.conn.close

    def exists(self, key, value):
        cursor = self.cursor()
        records = cursor.execute(
            "SELECT * FROM crawler_data WHERE `%s`=%s",
            key, value)
        return records is not None


class CooperativeService:
    def __init__(self, args):
        self.service_name = os.environ.get("COOPERATIVE_SERVICE")
        self.url = os.environ.get("COOPERATIVE_SERVICE_URL")
        self.expire_date = ''

    def call(self):
        pass

    def scraping(self, file_name):
        self.raw_data = open(file_name, 'r').read()
        doc = lxml.html.fromstring(self.raw_data)
        self.price = doc.xpath(
            "/html/body/section/div[2]/div/div[2]/div/div[5]/dl[2]/dd").text()
        self.title = doc.xpath(
            "/html/body/section/div[2]/div/div[2]/div/div[2]/h2").text()
        crawler_data = CrawlerData(__dict__)
        crawler_data.ave()


def s3_handler(event, context):
    cooperative_service = CooperativeService()
    cooperative_service.scraping(event['Records']['s3']['object']['key'])
    BUCKET_NAME = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']
    s3 = boto3.resource('s3')
    file_path = "/tmp/" + file_name

    print("run scraping lambda")

    try:
        s3.Bucket(BUCKET_NAME).download_file(file_name, file_path)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

    cooperative_service = CooperativeService()
    cooperative_service.scraping(file_path)
    cooperative_service.close
    print("sucess!!")
