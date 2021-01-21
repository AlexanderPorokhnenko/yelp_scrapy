import scrapy
import json
import logging
import re
from yelp.items import YelpItem
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst


class YelpSpiderSpider(scrapy.Spider):
    name = 'yelp_spider'
    allowed_domains = ['yelp.com']

    def start_requests(self):
        try:
            yield scrapy.Request(self.link)
        except AttributeError:
            logging.error("Link is missing!")

    def getAbout(self, response):
        loader = ItemLoader(item=response.meta['item'], response=response)
        loader.default_output_processor = TakeFirst()
        response_json = json.loads(response.text)
        about = {}
        if response_json['bizDetailsPageProps']['fromTheBusinessProps']:
            about.update({'specialities': response_json['bizDetailsPageProps']['fromTheBusinessProps']['fromTheBusinessContentProps']['specialtiesText']})
            if response_json['bizDetailsPageProps']['fromTheBusinessProps']['fromTheBusinessContentProps']['historyText']:
                about.update({'History': response_json['bizDetailsPageProps']['fromTheBusinessProps']['fromTheBusinessContentProps']['historyText']})
            if response_json['bizDetailsPageProps']['fromTheBusinessProps']['fromTheBusinessContentProps']['businessOwnerBio']:
                about.update({'businessOwnerBio': response_json['bizDetailsPageProps']['fromTheBusinessProps']['fromTheBusinessContentProps']['businessOwnerBio']})
        else:
            about = None
        loader.add_value('description', about)
        return loader.load_item()

    def getAmenities(self, response):
        loader = ItemLoader(item=response.meta['item'], response=response)
        response_json = json.loads(response.text)[0]
        if response_json['data']['business']['organizedProperties']:
            amenities = [amenity['displayText'] for amenity in response_json['data']['business']['organizedProperties'][0]['properties']]
            loader.add_value('amenities', amenities)
        yield scrapy.Request('https://www.yelp.com/biz/{}/props'.format("".join(loader.get_output_value('business_id')))
                             , method='GET', headers={'Content-Type': 'application/json', 'X-Requested-With':
                'XMLHttpRequest', 'Accept':	'application/json', 'Referer':'https://www.yelp.com/biz/fog-harbor-fish-house-san-francisco-2'},
                             callback=self.getAbout,
                             meta={'item': loader.load_item()})

    def getBusinessHours(self, response):
        loader = ItemLoader(item=response.meta['item'], response=response)
        response_json = json.loads(response.text)[0]
        schedule = dict()
        if response_json['data']['business']['operationHours']:
            for day in response_json['data']['business']['operationHours']['regularHoursMergedWithSpecialHoursForCurrentWeek']:
                schedule[day['dayOfWeekShort']] = "".join(day['regularHours'])
        loader.add_value('schedule', schedule)
        post_data = [{"operationName":"GetBizPageProperties","variables":{"BizEncId":"".join(loader.get_output_value('business_id'))},"extensions":{"documentId":"f06d155f02e55e7aadb01d6469e34d4bad301f14b6e0eba92a31e635694ebc21"}}]
        yield scrapy.Request('https://www.yelp.com/gql/batch', method='POST', body=json.dumps(post_data),
                             headers={'Content-Type': 'application/json'}, callback=self.getAmenities,
                             meta={'item': loader.load_item()})

    def linkedData(self, response):
        loader = ItemLoader(item=response.meta['item'], response=response)

        address = {}
        response_json = json.loads(response.text)[0]
        address['street'] = "{}, {}".format(response_json['data']['business']['location']['address']['addressLine1'],
                                            response_json['data']['business']['location']['address']['addressLine2'],
                                            response_json['data']['business']['location']['address']['addressLine3'])
        address['city'] = response_json['data']['business']['location']['address']['city']
        address['stateprov'] = response_json['data']['business']['location']['address']['regionCode']
        address['country'] = response_json['data']['business']['location']['country']['code']
        address['postalCode'] = response_json['data']['business']['location']['address']['postalCode']
        loader.add_value('main_img_url', response_json['data']['business']['primaryPhoto']['photoUrl']['url'])
        loader.add_value('phone', response_json['data']['business']['phoneNumber']['formatted'])
        loader.add_value('average_rating', response_json['data']['business']['rating'])
        loader.add_value('address', address)
        post_data = [{"operationName":"GetBusinessHours","variables":{"BizEncId":"".join(loader.get_output_value('business_id'))},"extensions":{"documentId":"35437a3b2abdff32ea1f4d018dbfe66f58fcfb4c804b7ae1c7e341389e9de873"}}]
        yield scrapy.Request('https://www.yelp.com/gql/batch', method='POST', body=json.dumps(post_data),
            headers={'Content-Type': 'application/json'}, callback=self.getBusinessHours, meta={'item': loader.load_item()})

    def parse(self, response, **kwargs):
        loader = ItemLoader(item=YelpItem(), response=response)
        for script in response.css('script').getall():
            if '{"gaConfig' in script:
                detail_json = json.loads(re.search(r'({"gaConfig.*?)-->', script).group(1))
        loader.add_value('direct_url', detail_json['staticUrl'])
        loader.add_value('business_id', detail_json['bizDetailsPageProps']['bizContactInfoProps']['businessId'])
        loader.add_value('categories', detail_json['gaConfig']['dimensions']['www']['second_level_categories'][1])
        if detail_json['bizDetailsPageProps']['bizContactInfoProps']['businessWebsite']:
            loader.add_value('site', detail_json['bizDetailsPageProps']['bizContactInfoProps']['businessWebsite']['linkText'])
        loader.add_value('title', detail_json['bizDetailsPageProps']['businessName'])
        loader.add_value('review_count', detail_json['bizDetailsPageProps']['ratingDetailsProps']['numReviews'])
        #TODO: find way to not use hardcoded documentIds
        post_data = [{"operationName":"getLocalBusinessJsonLinkedData","variables":{"BizEncId": "".join(loader.get_output_value('business_id'))},"extensions":{"documentId":"1cf362b8e8f9b3dae26d9f55e7204acd8355c916348a038f913845670139f60a"}}]

        yield scrapy.Request('https://www.yelp.com/gql/batch', method='POST', body=json.dumps(post_data),
            headers={'Content-Type': 'application/json'}, callback=self.linkedData, meta={'item': loader.load_item()})
