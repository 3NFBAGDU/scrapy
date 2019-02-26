import json
import time

from scrapy.selector import Selector
import scrapy
import csv
from w3lib.http import basic_auth_header
import random
# must inherit from scrapy.Spider
from scrapy.http import Request


class KittensSpider(scrapy.Spider):
    # can be any string, will be used to call from the console
    name = "scrapCrexi"

    def initCsvFile(self):
        columns = ['pageurl', 'imageurl', 'name','address', 'city', 'state', 'Zip', 'Price', 'Rooms', 'Building Size', 'Lot Area', 'Property Type', 'Cap Rate', 'firstName', 'lastName', 'company', 'email', 'phone', 'firstName', 'lastName', 'company', 'email', 'phone', 'firstName', 'lastName', 'company', 'email', 'phone']
        with open('crexi.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(columns)

        f.close()

    def initProxy(self):
        self.proxyIP = [
            "https://209.251.20.59:4444",
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
        cookies = ''

        self.maxPage = 100
        self.initCsvFile()

        url = 'https://api.crexi.com/assets?types=Hospitality&count=500&offset={}&sortDirection=Descending&sortOrder=Rank'
        self.fh = open("hello.txt", "wb")

        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "method": "GET",
            "accept-encoding": " gzip, deflate, br",
            "accept-language": "en-US,en;q=0.9",
            "referer": "https://www.crexi.com/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
            "Host": "api.crexi.com",
            "Origin": "api.crexi.com",
            "Connection": 'keep-alive'
        }

        i = 0
        while i < self.maxPage:
            req = scrapy.Request(url.format(i * 500),
                          method="get",headers=headers,
                          callback=self.parse)
            time.sleep(random.randint(1, 5))
            t = random.randint(0, len(self.proxyIP) - 1)
            req.meta['proxy'] = self.proxyIP[t]
            req.headers['Proxy-Authorization'] = basic_auth_header(
                '2b37ecba9f', '4ojgLl8h')

            yield req
            i += 1


    def parse(self, response):

        jsonresponse = json.loads(response.body)

        data = jsonresponse['Data']

        for item in data:
            id = item['Id']
            url = "https://api.crexi.com/assets/{}".format(id)
            headers = {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "referer": "https://www.crexi.com/",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
                "Origin": "api.crexi.com",
            }
            time.sleep(random.randint(1, 5))

            req = scrapy.Request(url,headers = headers, callback=self.parsePage, meta={'show_link': 'https://www.crexi.com/properties/{}'.format(id)})
            t = random.randint(0, len(self.proxyIP) - 1)
            req.meta['proxy'] = self.proxyIP[t]
            req.headers['Proxy-Authorization'] = basic_auth_header(
                '2b37ecba9f', '4ojgLl8h')
            yield req

        return



    def getImageUrl(self, json):
        try:
            return json['Gallery'][0]['ImageUrl']
        except:
            return ''


    def getName(self, json):
        try:
            return json['Name']
        except:
            return ''

    def getCity(self, json):
        try:
            return json['Locations'][0]['City']
        except:
            return ''

    def getZip(self, json):
        try:
            return json['Locations'][0]['Zip']
        except:
            return ''

    def getState(self, json):
        try:
            return json['Locations'][0]['State']['Name']
        except:
            return ''

    def getAddress(self, json):
        try:
            return json['Locations'][0]['Address']
        except:
            return ''


    def getPrice(self, json):
        try:
            return json['Details']['Asking Price']
        except:
            return 'Unpriced'

    def getProperty(self, json):
        try:
            return json['Details']['Property Type']
        except:
            return ''

    def getRooms(self, json):
        try:
            return json['Details']['Keys']
        except:
            return ''

    def getStories(self, json):
        try:
            return json['Details']['Stories']
        except:
            return ''

    def getCapRate(self, json):
        try:
            return json['Details']['Cap Rate']
        except:
            return ''

    def parseBroker(self, broker):

        dict = {}
        try:
            firstName = broker['FirstName']
        except:
            firstName = ''

        try:
            lastName = broker['LastName']
        except:
            lastName = ''

        try:
            company = broker['Brokerage']['Name']
        except:
            company = ''

        try:
            email = broker['Email']
        except:
            email = ''

        dict['name'] = firstName
        dict['lastName'] = lastName
        dict['company'] = company
        dict['email'] = email

        return dict

    def parsePage(self, response):
        self.fh.write(response.body + b'\n')
        jsonresponse = json.loads(response.body)

        dict = {}
        columns = ['pageurl', 'imageurl', 'name','address', 'city', 'state', 'zip', 'Price', 'Rooms', 'Building Size', 'Lot Area', 'Property Type', 'CapRate']
        for col in columns:
            dict[col] = ''

        url = response.meta.get('show_link')
        name = self.getName(jsonresponse)
        imageUrl = self.getImageUrl(jsonresponse)
        city = self.getCity(jsonresponse)
        state = self.getState(jsonresponse)
        zip = self.getZip(jsonresponse)
        address = self.getAddress(jsonresponse)
        price = self.getPrice(jsonresponse)
        property = self.getProperty(jsonresponse)
        rooms = self.getRooms(jsonresponse)
        stories = self.getStories(jsonresponse)
        capRate = self.getCapRate(jsonresponse)

        brokersInf = []

        for broker in jsonresponse['Brokers']:

            brokersInf.append(self.parseBroker(broker))


        dict['pageurl'] = url
        dict['imageurl'] = imageUrl
        dict['name'] = name
        dict['state'] = state
        dict['zip'] = zip
        dict['city'] = city
        dict['Price'] = price
        dict['Property Type'] = property
        dict['address'] = address
        dict['Rooms'] = rooms
        dict['stories'] = stories
        dict['CapRate'] = capRate

        toAddRow = []
        for col in columns:
            toAddRow.append(dict[col])

        for i in range(0, len(brokersInf)):
            toAddRow.append('')
            toAddRow.append(brokersInf[i]['name'])
            toAddRow.append(brokersInf[i]['lastName'])
            toAddRow.append(brokersInf[i]['company'])
            toAddRow.append(brokersInf[i]['email'])
            toAddRow.append('')

        with open('crexi.csv', 'a') as f:
            writer = csv.writer(f)
            writer.writerow(toAddRow)

        f.close()

