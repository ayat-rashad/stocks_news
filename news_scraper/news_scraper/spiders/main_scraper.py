# -*- coding: utf-8 -*-
from scrapy.selector import Selector
from scrapy.spiders import Spider
from scrapy.http import Request
from news_scraper.items import News

from dateutil.parser import parse
from dateutil import relativedelta
import datetime

from urlparse import urljoin
import json, re

class MainSpider(Spider):
    name = 'news_scraper'

    def __init__(self):
        self.root = 'http://finance.yahoo.com/'
        self.today = datetime.datetime.now()
        self.MaxInterval = 6     #6 months of news

        # logger setup
        self.log = logging.getLogger('newscraper')
        self.log.setLevel(logging.DEBUG)
        
        fh = logging.FileHandler('newscraper.log')
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        self.log.addHandler(fh)
        self.log.addHandler(ch)

        with open('external-links.txt') as f:
            self.start_urls = [urljoin(self.root,lnk.strip()) for lnk in f.readlines()]

        
    def getUrl(self, start, end):
        pass

    
    #def make_requests_from_url(self, url):
        #newUrl = self.getUrl(self.start, self.end)
        #return Request(newUrl)
            
    
    def parse(self, response):
        sel = Selector(response)
        title = content = date = ''

        # find news title
        tit_finders = ['.header h1', '.lede-headline', '.title', '#article-headline']
        for finder in tit_finders:
            try:
                title = sel.css('%s::text' %finder)[0].extract()
                break
            except:
                pass

        if not title:
            self.log.debug('could not find title..')

        # find news body       
        con_finders = ['.body', '.article-body__content', '#article_body', '#article-body']
        for finder in con_finders:
            try:
                content = sel.css('%s' %finder)[0].extract()
                break
            except:
                pass

        if not content:
            self.log.error('could not find content..')
            self.log.debug('news link: %s..' %response.url)

        # find news date      
        dat_finders = ['cite abbr', 'time', '.timestamp span']
        for finder in dat_finders:
            try:
                date = sel.css('%s::text' %finder)[0].extract()
                break
            except:
                pass

        if not date:
            self.log.error('could not find date..')
            self.log.debug('news link: %s..' %response.url)

        yield News(url=response.url, title=title, content=content, date=date)        
        
        
    
