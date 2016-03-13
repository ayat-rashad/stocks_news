# -*- coding: utf-8 -*-

import re, logging
import mechanize
import time, sys
from  collections import deque
import numpy as np
from bs4 import BeautifulSoup

class LinksScraper:
    providers = [
                 #'http://finance.yahoo.com/news/provider-accesswire',
                 #'http://finance.yahoo.com/news/provider-americancitybusinessjournals',
                 #'http://finance.yahoo.com/news/provider-ap',
                 #'http://finance.yahoo.com/news/provider-the-atlantic',
                 'http://finance.yahoo.com/news/provider-bankrate',
                 'http://finance.yahoo.com/news/provider-barrons',
                 'http://finance.yahoo.com/news/provider-benzinga',
                 'http://finance.yahoo.com/news/provider-bloomberg',
                 'http://finance.yahoo.com/news/provider-businessinsider',
                 'http://finance.yahoo.com/news/provider-businesswire',
                 'http://finance.yahoo.com/news/provider-businessweek',
                 'http://finance.yahoo.com/news/provider-capitalcube',
                 'http://finance.yahoo.com/news/provider-cbsmoneywatch',
                 'http://finance.yahoo.com/news/provider-cnbc',
                 'http://finance.yahoo.com/news/provider-cnnmoney',
                 'http://finance.yahoo.com/news/provider-cnwgroup',
                 'http://finance.yahoo.com/news/provider-consumer-reports',
                 'http://finance.yahoo.com/news/provider-credit',
                 'http://finance.yahoo.com/news/provider-credit-cards',
                 'http://finance.yahoo.com/news/provider-dailyfx',
                 'http://finance.yahoo.com/news/provider-dailyworth',
                 'http://finance.yahoo.com/news/provider-engadget',
                 'http://finance.yahoo.com/news/provider-entrepreneur',
                 'http://finance.yahoo.com/news/provider-etf-trends',
                 'http://finance.yahoo.com/news/provider-etfguide',
                 'http://finance.yahoo.com/news/provider-financial-times',
                 'http://finance.yahoo.com/news/provider-thefiscaltimes',
                 'http://finance.yahoo.com/news/provider-forbes',
                 'http://finance.yahoo.com/news/provider-fortune',
                 'http://finance.yahoo.com/news/provider-foxbusiness',
                 'http://finance.yahoo.com/news/provider-paidcontent',
                 'http://finance.yahoo.com/news/provider-globenewswire',
                 'http://finance.yahoo.com/news/provider-gurufocus',
                 'http://finance.yahoo.com/news/provider-investopedia',
                 'http://finance.yahoo.com/news/provider-investors-business-daily',
                 'http://finance.yahoo.com/news/provider-kiplinger',
                 'http://finance.yahoo.com/news/provider-los-angeles-times',
                 'http://finance.yahoo.com/news/provider-market-realist',
                 'http://finance.yahoo.com/news/provider-marketwatch',
                 'http://finance.yahoo.com/news/provider-marketwire',
                 'http://finance.yahoo.com/news/provider-money',
                 'http://finance.yahoo.com/news/provider-moneytalksnews',
                 'http://finance.yahoo.com/news/provider-moodys',
                 'http://finance.yahoo.com/news/provider-morningstar',
                 'http://finance.yahoo.com/news/provider-mrtopstep',
                 'http://finance.yahoo.com/news/provider-optionmonster',
                 'http://finance.yahoo.com/news/provider-prnewswire',
                 'http://finance.yahoo.com/news/provider-reuters',
                 'http://finance.yahoo.com/news/provider-sanjosemercurynews',
                 'http://finance.yahoo.com/news/provider-selerity',
                 'http://finance.yahoo.com/news/provider-techcrunch',
                 'http://finance.yahoo.com/news/provider-techrepublic',
                 'http://finance.yahoo.com/news/provider-thestreet',
                 'http://finance.yahoo.com/news/provider-thomsonreuters',
                 'http://finance.yahoo.com/news/provider-usnews',
                 'http://finance.yahoo.com/news/provider-usatoday',
                 'http://finance.yahoo.com/news/provider-the-wall-street-journal',
                 'http://finance.yahoo.com/news/provider-zacks',
                 'http://finance.yahoo.com/news/provider-zacks-scr',
                 'http://finance.yahoo.com/news/provider-zdnet']

    def __init__(self):
        # logger setup
        self.log = logging.getLogger('lnkscraper')
        self.log.setLevel(logging.DEBUG)
        
        fh = logging.FileHandler('lnkscraper.log')
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        self.log.addHandler(fh)
        self.log.addHandler(ch)

        logger = logging.getLogger("mechanize")
        logger.addHandler(logging.StreamHandler(sys.stdout))
        logger.setLevel(logging.DEBUG)

        self.retry = 3
        self.links_file = open('external-links.txt', 'w')
        self.recent_links = deque(maxlen=1000)
        self.maxduplicates = 300
        self.timeout = 60

        self.br = self.setup_browser()

        # proxy list
        with open('proxies.txt') as f:
            self.proxies = [l.strip() for l in f.readlines()]


    def setup_browser(self):
        headers  = [('User-Agent', 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'),
                    ('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'),
                    ('Connection', 'keep-alive')]

        br = mechanize.Browser(factory=mechanize.RobustFactory())
        #br.set_handle_gzip(True)
        br.set_handle_redirect(True)
        br.set_handle_referer(True)
        br.addheaders = headers
        
        return br


    def open_url(self, url):
        for i in range(self.retry):
            try:
                proxy = np.random.choice(self.proxies)
                self.br.set_proxies({'http':"http://%s" %proxy})
                res = self.br.open(url, timeout=self.timeout)
                return (True, res)
            except Exception as e:
                self.log.error('%s..%s' %(type(e), e))
                
        return (False, None)
    

    def scrape_news_links(self):        
        for provider_url in self.providers:
            succ, res = self.open_url(provider_url)
            if not succ:
                self.log.error("Could not start scraping Port: %s.." %provider_url)
                continue                
                
            self.log.info("Scraping Port: %s.." %provider_url)
            page = BeautifulSoup(res.read())
            retries = duplicates = 0
            self.recent_links.clear()
            
            while True:
                #if retries > self.retry:
                    #self.log.info('Retry limit exceeded...')
                    #break
                
                #links = list(br.links(url_regex="bloomberg\.com|cnbc\.com|marketwatch\.com"))
                #links = list(self.br.links(url_regex="/news/[^\.]+\.html"))
                request = self.br.request
                links = [l.a.get('href') for l in page.select('.txt')]
                self.log.info('#links found: %d' %len(links))
                
                if len(links) == 0:
                    self.log.error('No Links..')
                    #self.log.info('Retrying..')
                    #retries += 1
                    with open('lastpage.html', 'w') as f:
                        print res.info()
                        f.write(unicode(res.read()).encode('utf-8'))
                        self.log.info('failed..page contents written to lastpage.html')
                    
                    break
                
                for link in links:
                    #if link.url not in self.recent_links:
                    if link not in self.recent_links:
                        self.links_file.write(link + '\n')
                        self.recent_links.append(link)
                    else:
                        #if link.url == self.recent_links[-1]:
                        duplicates += 1
                        continue

                self.log.info('Duplicate-links so far: %d' %duplicates)
                               
                if duplicates >= self.maxduplicates:
                    self.log.error('Duplicate-links limit exceeded..')
                    self.log.info('Next Provider...')
                    break

                # next page
                res = self.br.click_link(text_regex=r"Next >>")
                self.log.info("Next Page...")                    
                time.sleep(5)           # throttle
                


if __name__ == '__main__':
    lnkScraper = LinksScraper()
    lnkScraper.scrape_news_links()
