# -*- coding: utf-8 -*-

from celery import Celery

import re, logging, time, sys
from  collections import deque
from urlparse import urljoin, urlparse

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import ActionChains
from selenium.common import exceptions

import numpy as np
from bs4 import BeautifulSoup

# celery app
app = Celery('distributed_process', broker='amqp://guest@localhost//', backend='redis://')


class LinksScraper:
    def __init__(self, providers, proxies='proxies.txt', retry=5, timeout=60):
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

        self.retry = retry
        self.recent_links = deque(maxlen=1000)
        self.timeout = timeout 
        self.YF_root = 'http://finance.yahoo.com'

        self.br = self._setup_browser()

        if hasattr(proxies, '__iter__'):    # proxy list
            self.proxies = proxies
        elif isinstance(proxies, str):      # read from file
            with open(proxies) as f:
                self.proxies = [l.strip() for l in f.readlines()]
                
        # news providers to scrape
        self.providers = providers
        

    def _setup_browser(self, prx=None):
        headers  = [('User-Agent', 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'),
                    ('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'),
                    ('Connection', 'keep-alive')]

        dcap = dict(DesiredCapabilities.PHANTOMJS)
        #dcap["phantomjs.page.settings.userAgent"] = (
        #"Mozilla/5.0 (X11; Linux i586; rv:31.0) Gecko/20100101 Firefox/31.0"
        #)
        
        if prx:
            service_args = ['--proxy=%s' %prx, '--proxy-type=http']
        else:
            service_args = None

        br = webdriver.PhantomJS(desired_capabilities=dcap, service_args=service_args)
        br.implicitly_wait(self.timeout)
        br.set_page_load_timeout(self.timeout) 
        
        return br


    def _open_url(self, url):
        for i in range(self.retry):
            try:
                if self.br:         # restart service with new proxy
                    self.br.quit()
                    
                proxy = np.random.choice(self.proxies)
                self.br = self._setup_browser(prx=proxy)
                self.ac = ActionChains(self.br)
                self.wait = WebDriverWait(self.br, self.timeout)
                res = self.br.get(url)
                return (True, res)
            
            except Exception as e:
                self.log.error('%s..%s' %(type(e), e))
                
        return (False, None)
    

    def scrape_news_links(self):
        news_links = []
        
        for provider_url in self.providers:
            succ, res = self._open_url(provider_url)
            
            if not succ:
                self.log.error("Could not start scraping provider: %s.." %provider_url)
                continue                
                
            self.log.info("Scraping provider: %s.." %provider_url)
            self.links_file = open('%s.txt' %urlparse(provider_url).path.split('/')[-1], 'w')
            retries = duplicates = 0
            last_title = ''
            self.recent_links.clear()
            
            while True:
                if retries > self.retry:
                    self.log.info('Retry limit exceeded...moving to next provider')
                    self.links_file.close()
                    break
                
                links = self.br.find_elements_by_css_selector('.txt')
                self.log.debug('#links found: %d' %len(links))
                
                if len(links) == 0:
                    self.log.error('No Links found..')

                    with open('lastpage.html', 'w') as f:
                        f.write(unicode(self.br.page_source).encode('utf-8'))
                        self.log.error('failed..page contents written to lastpage.html')

                    succ, res = self._open_url(provider_url)     # retry  with new proxy          

                    if not succ:
                        self.links_file.close()
                        break
                    
                    retries += 1
                    continue
                    
                # test for repeated pages
                if last_title and last_title == links[-1].find_element_by_tag_name('a').text:
                    self.log.debug('page repeated..')
                    #succ, res = self._open_url(provider_url)     # retry  with new proxy          
                    #if not succ:
                    #    break
                    retries += 1
                    #continue

                last_title = links[-1].find_element_by_tag_name('a').text 
                
                for link in links:
                    try:
                        date = link.find_element_by_tag_name('cite')
                        if date:
                            date = date.text.split('-')[1]
                    except Exception as e:
                        self.log.debug('problem with date..%s' %e)
                        date = ''
                        
                    link = link.find_element_by_tag_name('a').get_attribute('href')

                    if link.startswith('/news'):        # relative link
                        link = urljoin(self.YF_root, link)
                    
                    if link not in self.recent_links:
                        self.links_file.write('%s, "%s" \n' %(link, date))
                        self.recent_links.append(link)
                        news_links.append((link, date))
                    else:
                        duplicates += 1

                #self.log.debug('Duplicate-links so far: %d' %duplicates)

                # navigate to next page
                try:
                    #elem = self.br.find_element_by_partial_link_text("Next >>")
                    elem = self.wait.until(
                                EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.more-inline'))
                                )
                    self.ac.move_to_element(elem).perform()
                    elem.click()
                    
                except exceptions.NoSuchElementException as e:
                    self.log.error('Elem not found..%s' %e)
                    
                    with open('lastpage.html', 'w') as f:
                        f.write(unicode(self.br.page_source).encode('utf-8'))
                        
                    self.br.get_screenshot_as_file('sshot.jpeg')
                    
                except TimeoutException as e:
                    self.log.error('Click timeout..%s' %e)
                    #retries += 1
                    #continue

                # wait untill javascript complete loading data
                try:
                    waitResult = self.wait.until(EC.staleness_of(links[-1]))
                except Exception as e:
                    self.log.debug(e)
                    self.log.error('Script timeout..')
                    retries += 1
                    self.log.debug('Retrying..')
                    continue
                
                self.log.info("Next Page...")
                time.sleep(5)           # throttle                

        self.br.quit()
    
        return news_inks

#################################################################################

import ner
import urllib2
from dateutil.parser import parse

class NewsReader:
    def __init__(self, links, tagger_port=9191, proxies='proxies.txt', retry=3):
        # Stanford NER client
        self.tagger = ner.SocketNER(host='127.0.0.1', port=tagger_port)

        # logger setup
        self.log = logging.getLogger('nreader')
        self.log.setLevel(logging.DEBUG)
        
        fh = logging.FileHandler('nreader.log')
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        self.log.addHandler(fh)
        self.log.addHandler(ch)

        self.retry = retry
        self.links = links
        self.timeout = 30
        
        if hasattr(proxies, '__iter__'):    # proxy list
            self.proxies = proxies
        elif isinstance(proxies, str):      # read from file
            with open(proxies) as f:
                self.proxies = [l.strip() for l in f.readlines()]
                
        
    def read_news(self):
        result = []         # scraped and tagged news
        proxy = None

        if hasattr(self, 'proxies'):
            proxy = np.random.choice(self.proxies)
            
        opener = urllib2.build_opener(urllib2.ProxyHandler({'http':proxy}))

        for link, date in self.links:
            news = {'link': link, 'date': date}
            content = ''
            
            for i in range(self.retry):
                try:
                    page = opener.open(link, timeout=self.timeout).read()
                    break
                except Exception as e:
                    print type(e), e
                    # retry with different proxy
                    proxy = None
                    if hasattr(self, 'proxies'):
                        proxy = np.random.choice(self.proxies)
                    opener = urllib2.build_opener(urllib2.ProxyHandler({'http':proxy}))
            else:
                self.log.error('could not read news at: %s' %link)
                continue

            parser = BeautifulSoup(page)      
            con_finders = ['.body', '.article-body__content',   # find news body 
                           '#article_body', '#article-body']
            
            for finder in con_finders:
                try:
                    content = parser.select(finder)[0].text
                    break
                except:
                    pass

            if not content:
                self.log.error('could not find content..')
                self.log.debug('news link: %s..' %link)
                continue
                    
            news['content'] = content
            nes = self.ner_tag(content)

            if nes:
                news['nes'] = nes
            else:
                self.log.debug('news link: %s..' %link)
                continue
            
            result.append(news)

        return result


    def ner_tag(self, content):
        try:
            nes = self.tagger.get_entities(content)
            return nes
        
        except Exception as e:
            self.log.error('could not tag news content..%s' %e)
            return False

####################################################################

# find news links using screen scraping (Selenium)
@app.task
def scrape_links(providers, proxies=None):
    scraper = LinksScraper(providers, retry=3, proxies=proxies, timeout=80)
    res = scraper.scrape_news_links()

    if res:
        return res
    else:
        raise Exception('Did not receive results.')


# scrape news and find NES
@app.task
def read_news(links, proxies=None, retry=3):
    nreader = NewsReader(links, retry=3)
    news = nreader.read_news()

    return news

from celery import group

@app.task
def chunk_result(result, tsk, chunk_size):
    return group(tsk.s(res) for res in np.split(np.array(result), chunk_size))()
    
    
    

                                  

    
