# -*- coding: utf-8 -*-
from scrapy.selector import Selector
from scrapy.spiders import Spider
from scrapy.http import Request
from YF_scraper.items import News

from dateutil.parser import parse
from dateutil import relativedelta
import datetime

from urlparse import urljoin
import json, re

class MainSpider(Spider):
    name = 'YF_scraper'

    def __init__(self):
        self.root = 'http://finance.yahoo.com/'
        self.today = datetime.datetime.now()
        self.MaxInterval = 6     #6 months of news

        with open('external-links.txt') as f:
            self.start_urls = [urljoin(self.root,lnk.strip()) for lnk in f.readlines()]

        
    def getUrl(self, start, end):
        pass

    
    #def make_requests_from_url(self, url):
        #newUrl = self.getUrl(self.start, self.end)
        #return Request(newUrl)
            
    
    def parse(self, response):
        #print  response.url
        sel = Selector(response)
        title = ''
        
        tit_finders = ['.header h1', '.lede-headline', '.title', '#article-headline']
        for finder in tit_finders:
            try:
                title = sel.css('%s::text' %finder)[0].extract()
                break
            except Exception as e:
                pass
                
        con_finders = ['.body', '.article-body__content', '#article_body', '#article-body']
        for finder in con_finders:
            try:
                content = sel.css('%s' %finder)[0].extract()
                break
            except Exception as e:
                pass
                
        dat_finders = ['cite abbr', 'time', '.timestamp span']
        for finder in dat_finders:
            try:
                date = sel.css('%s::text' %finder)[0].extract()
                break
            except Exception as e:
                pass

        yield News(url=response.url, title=title, content=content, date=date)        
        
        
    
