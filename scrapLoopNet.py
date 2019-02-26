import json
from scrapy.selector import Selector
import scrapy
import csv
from w3lib.http import basic_auth_header
import random
import time
from threading import Thread

# must inherit from scrapy.Spider
from scrapy.http import Request


class KittensSpider(scrapy.Spider):

    proxyUserPass = []
    # can be any string, will be used to call from the console
    name = "scrapLoopNet"

    def initCsvFile(self):
        columns = ['pageurl', 'imageurl', 'name', 'listingid', 'city', 'state', 'Zip', 'Price','Year Built', 'No. Rooms',
                   'Building Size', 'Lot Size','APN / Parcel ID','Corridor','Property Type', 'No. Stories', 'firstName', 'lastName', 'company',
                   'phone', 'firstName', 'lastName', 'company', 'phone', 'firstName', 'lastName', 'company', 'phone']
        with open('loopnet.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(columns)

        f.close()


    def initProxy(self):
        self.proxyIP = [
            "https://107.172.80.209:4444",
            "https://107.175.235.86:4444",
            "https://149.20.244.136:4444",
            "https://152.44.107.127:4444",
            "https://199.34.83.177:4444",
            "https://104.202.30.219:4444",
            "https://107.172.225.111:4444",
            "https://107.175.229.254:4444"
         ]

    # This method must be in the spider,
    # and will be automatically called by the crawl command.
    def start_requests(self):

        self.initProxy()

        maxpage = 100
        self.d = open('goru.txt', 'wb')
        cookies = ''

        self.maxPage = 30
        self.initCsvFile()

        url = 'https://www.yelp.com/search?cflt=contractors&find_loc=San%20Francisco%2C%20CA&start=20'
        req = scrapy.Request(url,
                             callback=self.parse)
        yield req

        return

        self.fh = open("hello.txt", "wb")

        i = 1
        while i < maxpage:

            time.sleep(random.randint(1, 5))

            req = scrapy.Request(url.format(i),
                                 callback=self.parse)

            t = random.randint(0, len(self.proxyIP) - 1)
            req.meta['proxy'] = self.proxyIP[t]
            req.headers['Proxy-Authorization'] = basic_auth_header(
                '2b37ecba9f', '4ojgLl8h')
            yield req
            i += 1

    def getListingID(self, result):
        try:
            return result.xpath("@gtm-listing-id").extract()[0]
        except:
            return ''

    def getCity(self, result):
        try:
            return result.xpath("@gtm-listing-city").extract()[0]
        except:
            return ''

    def getState(self, result):
        try:
            return result.xpath("@gtm-listing-state").extract()[0]
        except:
            return ''

    def getZip(self, result):
        try:
            return result.xpath("@gtm-listing-zip").extract()[0]
        except:
            return ''

    def getProperty(self, result):
        try:
            return result.xpath("@gtm-listing-property-type-name").extract()[0]
        except:
            return ''

    def getImage(self, result):
        try:
            image = \
            result.xpath(".//div[contains(@class, 'slide') and contains(@class, 'active')]/figure[1]/@style").extract()[
                0]
            st = 'url('
            image = image[image.find(st) + len(st):image.rfind(')')]
            return image
        except:
            return ''

    def parse(self, response):
        print(response.body)
        return
        # self.d.write(response.body)
        # self.d.close()
        sel = Selector(response)
        results = sel.xpath("//article[contains(@class, 'placard') and contains(@class, 'tier')]")
        for result in results:
            try:
                href = result.xpath(".//header//h4/a/@href").extract()[0]
            except:
                continue
            listingid = self.getListingID(result)
            city = self.getCity(result)
            state = self.getState(result)
            zip = self.getZip(result)
            properyType = self.getProperty(result)
            image = self.getImage(result)

            time.sleep(random.randint(1, 5))
            req = scrapy.Request(href,
                                 callback=self.parsePage,
                                 meta={'listingid': listingid, 'city': city, 'state': state, 'zip': zip,
                                       'propertyType': properyType, 'image': image})
            t = random.randint(0, len(self.proxyIP) - 1)
            req.meta['proxy'] = self.proxyIP[t]
            req.headers['Proxy-Authorization'] = basic_auth_header(
                '2b37ecba9f', '4ojgLl8h')

            yield req

    def getName(self, result):
        try:
            name = result.xpath(".//section[contains(@class, 'basic-info')]//h1/text()").extract()[0]
            name = name[self.findFirstLetter(name):self.findLastLetter(name) + 1]
            return name
        except:
            return ''

    def findFirstLetter(self, st):
        for i in range(0, len(st)):
            if st[i:i + 1].isalpha() or st[i:i + 1].isnumeric():
                return i
        return 1000

    def findLastLetter(self, st):
        for i in range(len(st) - 1, -1, -1):
            if st[i:i + 1].isalpha() or st[i:i + 1].isnumeric():
                return i
        return 1000

    def getCompanyPhone(self, result):
        try:
            phone = result.xpath(".//span[contains(@class, 'phone-number')]/text()").extract()[0]
            return phone
        except:
            return ''

    def getCompany(self, result):
        print(result.xpath(".//li[contains(@class, 'contact-logo')]/@title"))

        try:
            company = result.xpath(
                ".//li[contains(@class, 'contact-logo') or contains(@class, 'company-name')]/@title").extract()[0]
            return company
        except:
            return ''

    def parsePage(self, response):

        if int(response.status) > 400:
            return

        dict = {}
        columns = ['pageurl', 'imageurl', 'name', 'listingid', 'city', 'state', 'Zip', 'Price','Year Built', 'No. Rooms',
                   'Building Size', 'Lot Size','APN / Parcel ID','Corridor','Property Type', 'No. Stories']
        for col in columns:
            dict[col] = ''

        sel = Selector(response)
        url = response.url
        listingid = response.meta['listingid']
        city = response.meta['city']
        state = response.meta['state']
        zip = response.meta['zip']
        propertyType = response.meta['propertyType']
        image = response.meta['image']
        try:
            result = sel.xpath("//div[contains(@class, 'profile-main-info')]")[0]
        except:
            return

        name = self.getName(result)

        # fill dict
        dict['pageurl'] = url
        dict['imageurl'] = image
        dict['name'] = name
        dict['listingid'] = listingid
        dict['city'] = city
        dict['state'] = state
        dict['Zip'] = zip

        results = sel.xpath("//table[contains(@class, 'property-data') and contains(@class, 'featured-grid')]//tr")
        print('dedisnuteli')
        for result in results:
            row = result.xpath('.//td')
            for i in range(0, len(row), 2):

                try:
                    atr = row[i].xpath('.//text()').extract()[0]
                    atr = atr[self.findFirstLetter(atr):self.findLastLetter(atr) + 1]
                except:
                    continue

                try:
                    val = row[i + 1].xpath('.//span//text()').extract()[0]
                    dict[atr] = val
                except:
                    continue


        brokerinf = []

        phone = self.getCompanyPhone(sel)
        company = self.getCompany(sel)

        row = []
        brokers = sel.xpath(".//span[(@class='contact-name')]")

        print(len(brokers))
        for broker in brokers:
            brokerinf.append(self.getBroker(broker))

        for col in columns:
            row.append(dict[col])

        for brok in brokerinf:
            row.append(brok['name'])
            row.append(brok['lname'])
            row.append(company)
            row.append(phone)

        with open('loopnet.csv', 'a') as f:
            writer = csv.writer(f)
            writer.writerow(row)

        f.close()

    def getBroker(self, broker):
        try:
            dic = {}
            name = broker.xpath(".//span[contains(@class, 'first-name')]/text()").extract()[0]
            lname = broker.xpath(".//span[contains(@class, 'last-name')]/text()").extract()[0]
            dic['name'] = name
            dic['lname'] = lname
            return dic
        except:
            return {}