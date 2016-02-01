
BOT_NAME = 'YF_scraper'

SPIDER_MODULES = ['YF_scraper.spiders']
NEWSPIDER_MODULE = 'YF_scraper.spiders'

LOG_ENABLED = True
LOG_FILE = 'log.txt'
LOG_LEVEL = 'INFO'
#USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36'
DEFAULT_REQUEST_HEADERS = {
        'Accept': '*/*',
        'Accept-Language': 'en,en-US;q=0.8,ar;q=0.6',
        'Accept-Encoding':'gzip, deflate, sdch'
        }
        
DOWNLOAD_TIMEOUT = 120
RETRY_TIMES = 3
#RETRY_HTTP_CODES Default: [500, 502, 503, 504, 400, 408]


ITEM_PIPELINES = {
    'YF_scraper.pipelines.DuplicatesPipeline': 5,
    'YF_scraper.pipelines.MongoPipeline': 10,
}
 
'''EXTENSIONS = {
'scrapy.exporters.JsonLinesItemExporter':500,
}'''

MONGO_URI = 'mongodb://localhost:27017'
MONGO_DATABASE = 'yahoofinance_news'
