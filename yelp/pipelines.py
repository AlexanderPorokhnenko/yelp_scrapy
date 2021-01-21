from itemadapter import ItemAdapter
from sqlalchemy import create_engine, Column, Integer, String, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy_utils import database_exists, create_database
from .settings import DB_USER, DB_PASSWORD, DB_SERVER, DB_NAME
import json


Base = declarative_base()


class Business(Base):
    __tablename__ = 'yelp_business'
    id = Column(Integer, primary_key=True)
    title = Column(String(100))
    direct_url = Column(String(100))
    business_id = Column(String(50))
    main_img_url = Column(String(100))
    phone = Column(String(15))
    email = Column(String(256))
    address = Column(String(300))
    average_rating = Column(Integer)
    review_count = Column(Integer)
    categories = Column(String(100))
    site = Column(String(100))
    schedule = Column(String(400))
    description = Column(String(10000))
    amenities = Column(String(1000))


class YelpPipeline:
    def __init__(self):
        self.engine = create_engine('mysql://{}:{}@{}/{}'.format(DB_USER, DB_PASSWORD, DB_SERVER, DB_NAME))
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
        self.inspector = inspect(self.engine)
        Base.metadata.create_all(self.engine)
        self.session = Session(bind=self.engine)

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if self.session.query(Business.id).filter_by(business_id=adapter.get('business_id')).scalar() is None:
            business = Business(title=adapter.get('title'), direct_url=adapter.get('direct_url'), business_id=adapter.get('business_id'),
                            main_img_url=adapter.get('main_img_url'),phone=adapter.get('phone'),email=adapter.get('email'),
                            address=json.dumps(adapter.get('address')),average_rating=adapter.get('average_rating'),review_count=adapter.get('review_count'),
                            categories=adapter.get('categories'), site=adapter.get('site'),schedule=json.dumps(adapter.get('schedule')),
                            description=adapter.get('description'),amenities=adapter.get('amenities'))
            self.session.add(business)
            self.session.flush()
            self.session.commit()
            self.session.close()
        return item


