import csv

import scrapy
from scrapy.crawler import CrawlerProcess


URL = "https://ทองคําราคา.com/"


class MySpider(scrapy.Spider):
    name = "gold_price_spider"
    start_urls = [URL,]

    def parse(self, response):
        header = response.css("#divDaily h3::text").get().strip()
        print(header)

        table = response.css("#divDaily .pdtable")
        # print(table)

        rows = table.css("tr")
        # rows = table.xpath("//tr")
        # print(rows)
        data = []
        for row in rows:
            data.append(row.css("td::text").extract())
            # print(row.xpath("td//text()").extract())
    
        print (data[0])
        with open(f"price.csv", "w") as f:
            # Write to CSV
            # YOUR CODE HERE
            for each in data:
                writer = csv.writer(f)
                writer.writerow(each)
            


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(MySpider)
    process.start()
