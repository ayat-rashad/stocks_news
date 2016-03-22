# -*- coding: utf-8 -*-
import logging

from celery import Celery
from celery import group

from pymongo import MongoClient

import config
from news_reader import NewsReader
from links_scraper import LinksScraper


# celery app
app = Celery('distributed_process', broker=config.BROKER, backend=config.BACKEND)

log = logging.getLogger('main')
log.setLevel(logging.DEBUG)
        
fh = logging.FileHandler('log/main.log')
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
        
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
        
log.addHandler(fh)
log.addHandler(ch)


# find news links using screen scraping (Selenium)
@app.task
def scrape_links(providers):
    scraper = LinksScraper(providers, retry=config.RETRY, proxies=config.PROXY, timeout=config.TIMEOUT)
    res = scraper.scrape_news_links()

    if res:
        return res
    else:
        raise Exception('Did not receive results.')


# scrape news and find NES
@app.task
def read_news(links):
    nreader = NewsReader(links, retry=config.RETRY, proxies=config.PROXY)
    news = nreader.read_news()

    return news

@app.task
def store_result(res):
    log = logging.getLogger('main')
    log.debug('Storing Result', len(res))
    client = MongoClient('mongodb://%s:%s' %(config.MONGO_HOST, config.MONGO_PORT))
    db = client[MONGO_DB]

    try:
        db['news'].insert_many(res)
    except Exception as e:
        log.error('could not send results to remote server..%s..%s' %(type(e), e))
    

@app.task
def chunk_read_news(result, chunk_size):
    log = logging.getLogger('main')
    n_chunks = len(result) / chunk_size
    
    try:
        return group((read_news.s(res) | store_result.s()) for res in np.split(np.array(result), n_chunks))()
    except Exception as e:
        log.error('problem with chunking..%s..%s' %(type(e),e))
        return group((read_news.s(result) | store_result.s()))()
    
