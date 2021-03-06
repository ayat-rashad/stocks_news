# -*- coding: utf-8 -*-
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


class LinksScraper:
    def __init__(self, providers, proxies='proxies.txt', retry=5, timeout=60):
        # logger setup
        self.log = logging.getLogger('lnkscraper')
        self.log.setLevel(logging.DEBUG)
        
        fh = logging.FileHandler('log/lnkscraper.log')
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
        dcap = dict(DesiredCapabilities.PHANTOMJS)
        
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
                    retries += 1

                last_title = links[-1].find_element_by_tag_name('a').text 
                
                for link in links:
                    try:
                        date = link.find_element_by_tag_name('cite')
                        if date:
                            date = date.text.split('-')[1]
                    except Exception as e:
                        self.log.debug('problem with date..%s' %e)
                        date = ''

                    title = link.find_element_by_tag_name('a').text
                    link = link.find_element_by_tag_name('a').get_attribute('href')

                    if link.startswith('/news'):        # relative link
                        link = urljoin(self.YF_root, link)
                    
                    if link not in self.recent_links:
                        self.links_file.write((u'%s| %s|%s \n' %(link, title, date)).encode('utf-8'))
                        self.recent_links.append(link)
                        news_links.append({'link': link, 'date': date, 'title': title})
                    else:
                        duplicates += 1


                # navigate to next page
                try:
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
                
                self.log.debug("Next Page...")
                time.sleep(5)           # throttle                

        self.br.quit()
    
        return news_links
