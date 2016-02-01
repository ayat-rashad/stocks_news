from scrapy.exceptions import DropItem
import pymongo

class DuplicatesPipeline(object):

    collection_name = 'news'
    
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.links = self.db[self.collection_name].find({}, {'_id':0, 'url':1})
        self.links = [it['url'] for it in self.links]
        print "LINKS:", len(self.links)

    def close_spider(self, spider):
        self.client.close()
        

    def process_item(self, item, spider):
        if item['url'] in self.links:
            print 'duplicate'
            raise DropItem("Duplicate item found: %s" % item['url'])
        else:
            #self.IDs.add(item['ID'])
            return item

class MongoPipeline(object):

    collection_name = 'news'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.db[self.collection_name].insert(dict(item))
        return item
