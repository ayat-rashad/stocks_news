from scrapy.item import Item, Field

class News(Item):
    date = Field()
    url = Field()
    title = Field()
    content = Field()
