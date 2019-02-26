import json
import time

from scrapy.selector import Selector
import scrapy
import csv
from w3lib.http import basic_auth_header
import random


class KittensSpider(scrapy.Spider):
    # can be any string, will be used to call from the console
    name = "scrapKwcommercial"

    def initCsvFile(self):
        columns = ['pageurl', 'imageurl', 'name', 'address', 'city', 'state', 'zip' , 'Price', 'Rooms', 'Building Size', 'Lot Area', 'Property Type', 'firstname', 'lastname', 'Title','company', 'address', 'email', 'CellPhone', 'OfficePhone', 'firstname', 'lastname', 'Title','company', 'address', 'email', 'CellPhone', 'OfficePhone', 'firstname', 'secondname', 'Title','company', 'address', 'email', 'CellPhone', 'OfficePhone']
        with open('kwcommercial.csv', 'w') as f:
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

        self.initCsvFile()

        self.index = 0
        # maximal amount  of pages
        self.maxPages = 100
        # url to get all products from search
        self.urlToFormat = 'https://buildout.com/plugins/2015580a3a4c71497bfe0ba575bb8f12d8c475a1/inventory?utf8=%E2%9C%93&page={}&brandingId=&searchText=&q%5Bsale_or_lease_eq%5D=sale&q%5Bs%5D%5B%5D=&viewType=list&q%5Btype_eq_any%5D%5B%5D=8&q%5Bbuilding_size_sf_gteq%5D=&q%5Bbuilding_size_sf_lteq%5D=&q%5Bsale_price_gteq%5D=&q%5Bsale_price_lteq%5D=&q%5Bproperty_use_id_eq_any%5D%5B%5D='
        # page information url, to get product details
        self.houseUrlFormat = 'https://buildout.com/plugins/2015580a3a4c71497bfe0ba575bb8f12d8c475a1/www.kwcommercial.com/inventory/{}?pluginId=0&iframe=true&embedded=true&cacheSearch=true&customParams=Prod&propertyId={}-sale&fbclid=IwAR1tlxjTQf2ivT6XIgjitbB3OD28qq9jYgxkM-1SWIn-cV1woKk5rCrFMAk'
        i = 0


        while i < self.maxPages:
            # We make a request to each url and call the parse function on the http response.
            url = self.urlToFormat.format(i)
            # this is the house information page information Url
            houseInformationUrl = ''
            # this is a request header

            headers = {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "authority": "buildout.com",
                "method": "GET",
                "path": "/plugins/2015580a3a4c71497bfe0ba575bb8f12d8c475a1/inventory?utf8=%E2%9C%93&page=0&brandingId=&searchText=&q%5Bsale_or_lease_eq%5D=sale&q%5Bs%5D%5B%5D=&viewType=list&q%5Btype_eq_any%5D%5B%5D=8&q%5Bbuilding_size_sf_gteq%5D=&q%5Bbuilding_size_sf_lteq%5D=&q%5Bsale_price_gteq%5D=&q%5Bsale_price_lteq%5D=&q%5Bproperty_use_id_eq_any%5D%5B%5D=",
                "accept-encoding": " gzip, deflate, br",
                "accept-language": "en-US,en;q=0.9",
                "referer": "https://buildout.com/plugins/2015580a3a4c71497bfe0ba575bb8f12d8c475a1/www.kwcommercial.com/inventory/?pluginId=0&iframe=true&embedded=true&cacheSearch=true&customParams=Prod&initialSearchText=&saleOrLease=sale&propertyType=8&fbclid=IwAR06aOkPG5YNWkOKdrxLEmSfp89ItUOobrfAIvYGm72PmMlmNdJIWzdsWMQ",
                "user-agent":  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
                "x-requested-with" : "XMLHttpRequest"
            }
            time.sleep(random.randint(1, 5))

            # we are sending json request, response will on json format
            req =  scrapy.Request(url,
                          method="get",
                          headers=headers,
                          callback=self.parse)

            t = random.randint(0, len(self.proxyIP) - 1)
            req.meta['proxy'] = self.proxyIP[t]
            req.headers['Proxy-Authorization'] = basic_auth_header(
                '2b37ecba9f', '4ojgLl8h')

            yield  req
            i += 1

        #yield scrapy.Request('https://buildout.com/plugins/2015580a3a4c71497bfe0ba575bb8f12d8c475a1/www.kwcommercial.com/inventory/423938-sale?pluginId=0&iframe=true&embedded=true&cacheSearch=true&customParams=Prod&propertyId=423938-sale&fbclid=IwAR1tlxjTQf2ivT6XIgjitbB3OD28qq9jYgxkM-1SWIn-cV1woKk5rCrFMAk', callback=self.parse)


    def parse(self, response):

        jsonresponse = json.loads(response.body)

        # if here is no inventory
        if len(jsonresponse['inventory']) == 0:
            return


        for item in jsonresponse['inventory']:
            id = item['show_link']
            id = id[id.find('propertyId='):][11:]
            show_link = item['show_link']
            photoUrl = item['photo_url']
            city = item['city']
            state = item['state']
            zip = item['zip']
            time.sleep(random.randint(1, 5))

            req = scrapy.Request(self.houseUrlFormat.format(id, id), callback=self.parsePage, meta={'show_link': show_link, 'photoUrl': photoUrl,'zip': zip, 'city': city, 'state': state})
            t = random.randint(0, len(self.proxyIP) - 1)
            time.sleep(random.randint(1, 5))
            req.meta['proxy'] = self.proxyIP[t]
            req.headers['Proxy-Authorization'] = basic_auth_header(
                '2b37ecba9f', '4ojgLl8h')
            yield req

    def parsePrice(self, result):
        try:
            list = Selector(text=result[0].extract()).xpath('//text()').extract()
            if len(list) > 0 :
                return list[0]
            return ''
        except:
            return ''

    def parseRooms(self, result):
        try:
            list = Selector(text=result[1].extract()).xpath('//text()').extract()
            if len(list) > 0:
                return list[0]
            return ''
        except:
            return ''

    def parseBuildingSize(self, result):
        try:
            list = Selector(text=result[2].extract()).xpath('//text()').extract()
            if len(list) > 0:
                return list[0]
            return ''
        except:
            return ''

    def propertyType(self, result):
        try:
            list = Selector(text=result[3].extract()).xpath('//text()').extract()
            if len(list) > 0:
                return list[0]
            return ''
        except:
            return ''

    def lotSize(self, result):
        try:
            list = Selector(text=result[4].extract()).xpath('//text()').extract()
            if len(list) > 0:
                return list[0]
            return ''
        except:
            return ''


    def Year(self, result):
        try:
            list = Selector(text=result[5].extract()).xpath('//text()').extract()
            if len(list) > 0:
                return list[0]
            return ''
        except:
            return ''

    def getName(self, sel):
        list = sel.xpath(".//td[contains(@class, 'header-text')]//h1//text()")
        if len(list) > 0:
            return list[0].extract()
        return ''

    def getAddress(self, sel):
        list = sel.xpath(".//td[contains(@class, 'header-text')]//div[contains(@class, 'js-header-address')]//text()")
        if len(list) > 0:
            return list[0].extract()
        return ''

    def getcityStateZipCode(self, sel):
        list = sel.xpath(".//td[contains(@class, 'header-text')]//div[2]//text()")
        if len(list) > 0:
            return list[0].extract()
        return ''



    def parseBroker(self, brokerinf):
        dict = {}

        # get name
        try:

            name = brokerinf.xpath(".//td[contains(@class, 'js-broker-details')]/strong/a/text()").extract()[0]
        except:
            name = ''

        dict['name'] = name

        # get title
        try:
            title = brokerinf.xpath(".//td[contains(@class, 'js-broker-details')]/div[1]/small/text()").extract()[0]
        except:
            title = ''

        dict['title'] = title

        # get address,city, state
        try:

            address = brokerinf.xpath(".//td[contains(@class, 'js-broker-details')]/div[2]/small/text()").extract()[0]
        except:
            address = ''
        dict['address'] = address

        # get cellphone and officePhone, email
        try:
            email = brokerinf.xpath(".//td[contains(@class, 'js-broker-details')]//div[contains(@class, 'js-broker-contact-info')]/a/text()").extract()[0]
        except:
            email = ''

        try:
            cellPhone = brokerinf.xpath(".//td[contains(@class, 'js-broker-details')]//div[contains(@class, 'js-broker-contact-info')]/div[1]//text()").extract()[0]
        except:
            cellPhone = ''

        try:
            officePhone = brokerinf.xpath(".//td[contains(@class, 'js-broker-details')]//div[contains(@class, 'js-broker-contact-info')]/div[2]//text()").extract()[0]
        except:
            officePhone = ''

        dict['email'] = email
        dict['cell'] = cellPhone
        dict['office'] = officePhone

        return dict

    def parsePage(self, response):

        print("latoti")

        sel = Selector(response)
        results = sel.xpath("//*[contains(@class, 'striped')]//td")

        # filter our result, we don't want a td which has the //strong after his path
        results = list(filter(lambda x : len(Selector(text=x.extract()).xpath("//strong").extract()) == 0, results))

        imageurl = response.meta.get('photoUrl')

        price = self.parsePrice(results)
        numberOfRooms = self.parseRooms(results)
        buildingSize = self.parseBuildingSize(results)
        propertyType = self.propertyType(results)
        lotSize = self.lotSize(results)
        year = self.Year(results)
        name = self.getName(sel)
        address = self.getAddress(sel)
        city = response.meta.get('city')
        state = response.meta.get('state')
        zip = response.meta.get('zip')
        url = response.meta.get('show_link')


        brokers = sel.xpath("//div[contains(@class, 'col-xs-12') and contains(@class, 'small')]")
        brokerInf = []

        for broker in brokers:

            brokerDict = self.parseBroker(broker)
            brokerInf.append(brokerDict)

        row = [url, imageurl, name,address, city, state, zip,  price, numberOfRooms, buildingSize, lotSize, propertyType]

        print(brokerInf)
        for i in range(0, len(brokerInf)):
            x = brokerInf[i]['name']
            x = x.split(' ')
            try:
                firstname = x[0]
            except:
                firstname = ''

            try:
                lastname = x[1]
            except:
                lastname = ''

            row.append(firstname)
            row.append(lastname)
            row.append(brokerInf[i]['title'])
            row.append('KW Commercial')
            row.append(brokerInf[i]['address'])
            row.append(brokerInf[i]['email'])
            row.append(brokerInf[i]['cell'])
            row.append(brokerInf[i]['office'])


        with open('kwcommercial.csv', 'a') as f:
            writer = csv.writer(f)
            writer.writerow(row)

        f.close()



