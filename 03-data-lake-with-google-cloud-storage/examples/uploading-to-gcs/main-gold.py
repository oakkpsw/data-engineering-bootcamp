import json
import os
import sys

from google.api_core import exceptions
from google.cloud import storage
from google.oauth2 import service_account

import csv

import scrapy
from scrapy.crawler import CrawlerProcess

URL = "https://ทองคําราคา.com/"

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    keyfile = os.environ.get("KEYFILE_PATH")
    service_account_info = json.load(open(keyfile))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    project_id = "dataengineerbootcamp"

    storage_client = storage.Client(
        project=project_id,
        credentials=credentials,
    )
    bucket = storage_client.bucket(bucket_name)

    # try:
    #     bucket.delete_blob(blob_name=destination_blob_name)
    # except exceptions.NotFound as ex:
    #     print(f"File {destination_blob_name} not found")

    blob = bucket.blob(destination_blob_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    # generation_match_precondition = 0
    # blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )


class MySpider(scrapy.Spider):
    name = "gold_price_spider"
    start_urls = [URL,]

    def parse(self, response):
        header = response.css("#divDaily h3::text").get().strip()
        print(header)

        table = response.css("#divDaily .pdtable")
        # print(table)
        date_table = response.css("#wrapper .pdtable tbody tr").extract()
        # date = data_table[0]
        # print(date)
        # return true
        rows = table.css("tr")
        # rows = table.xpath("//tr")
        # print(rows)
        data = []
        for row in rows:
            data.append(row.css("td::text").extract())
            # print(row.xpath("td//text()").extract())
        file_name = "price.csv"
        #partition by data
        folder_name = "2023-05-12" 
        # bucket_name = "oak-100028"
        with open(f"{file_name}", "w") as f:
            # Write to CSV
            # YOUR CODE HERE
            for each in data:
                writer = csv.writer(f)
                writer.writerow(each)
        print (sys.argv[1])
        upload_blob(
            bucket_name=sys.argv[1],
            source_file_name=file_name,
            destination_blob_name=f"{folder_name}/{file_name}",
        )

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(MySpider)
    process.start()
