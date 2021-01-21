from scrapy.item import Field, Item
from itemloaders.processors import Join


class YelpItem(Item):
    title = Field()
    direct_url = Field()
    business_id = Field()
    main_img_url = Field()
    phone = Field()
    email = Field()
    address = Field()
    average_rating = Field()
    review_count = Field()
    categories = Field()
    site = Field()
    schedule = Field()
    description = Field()
    amenities = Field(output_processor=Join(', '))





