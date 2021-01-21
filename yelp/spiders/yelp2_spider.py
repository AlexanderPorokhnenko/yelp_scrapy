import scrapy
import json
from yelp.items import YelpItem
from scrapy.loader import ItemLoader
import re
from scrapy.selector import Selector
from scrapy.loader.processors import TakeFirst


class YelpSpiderSpider(scrapy.Spider):
    name = 'yelp2_spider'
    allowed_domains = ['yelp.com']

    def start_requests(self):
        if self.link:
            yield scrapy.Request(self.link)
        else:
            raise ValueError('Missing link')

    def linkedData(self, response):
        loader = ItemLoader(item=response.meta['item'], response=response)
        address = {}
        address['street'] = "{}, {}".format("".join(response.css('#attr_BusinessStreetAddress1::attr(value)').get()),
                                    "".join(response.css('#attr_BusinessStreetAddress2::attr(value)').get()))
        address['city'] = response.css('#attr_BusinessCity::attr(value)').get()
        address['stateprov'] = response.css('#attr_BusinessState > option[selected="selected"]::attr(value)').get()
        address['country'] = 'USA' if address['stateprov'] else None
        address['postalCode'] = response.css('#attr_BusinessZipCode::attr(value)').get()
        loader.add_css('phone', '#attr_BusinessPhoneNumber::attr(value)')
        loader.add_value('address', address)
        schedule = {}
        for day in response.css('div[class="hours"]').extract():
            selector = Selector(text=day)
            hours = selector.css('span[class="start"]::text').get() + " - " + selector.css('span[class="end"]::text').get()
            schedule[selector.css('span[class="weekday"]::text').get()] = hours
        loader.add_value('schedule', schedule)
        return loader.load_item()


    def parse(self, response, **kwargs):
        loader = ItemLoader(item=YelpItem(), response=response)
        loader.default_output_processor = TakeFirst()
        for script in response.css('script').getall():
            if '{"gaConfig' in script:
                detail_json = json.loads(re.search(r'({"gaConfig.*?)-->', script).group(1))
        loader.add_value('direct_url', detail_json['staticUrl'])
        loader.add_value('business_id', detail_json['bizDetailsPageProps']['bizContactInfoProps']['businessId'])
        loader.add_value('categories', detail_json['gaConfig']['dimensions']['www']['second_level_categories'][1])
        loader.add_value('site', detail_json['bizDetailsPageProps']['bizContactInfoProps']['businessWebsite']['linkText'])
        loader.add_value('title', detail_json['bizDetailsPageProps']['businessName'])
        loader.add_value('review_count', detail_json['bizDetailsPageProps']['ratingDetailsProps']['numReviews'])
        yield scrapy.Request('https://www.yelp.com/biz_attribute?biz_id={}'.format("".join(loader.get_output_value('business_id'))), method='GET',
            callback=self.linkedData, meta={'item': loader.load_item()})
