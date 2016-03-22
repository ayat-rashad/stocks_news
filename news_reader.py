# -*- coding: utf-8 -*-
import urllib2, re, logging, time, sys
from  collections import deque
from urlparse import urljoin, urlparse

from dateutil.parser import parse
from fuzzywuzzy import fuzz
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import ner


class NewsReader:
    def __init__(self, links, tagger_port=9191, proxies='proxies.txt', retry=3, timeout=80):
        # Stanford NER client
        self.tagger = ner.SocketNER(host='127.0.0.1', port=tagger_port)

        # logger setup
        self.log = logging.getLogger('nreader')
        self.log.setLevel(logging.DEBUG)
        
        fh = logging.FileHandler('log/nreader.log')
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
        self.timeout = timeout

        # Exchange stocks symbols
        def normalize(s):
            s = re.sub(r'\s*-.*', '', s)
            s = re.sub(r'Incorporated', 'Inc', s)
            return s
            
        self.symbols = pd.read_csv('nasdaqlisted.txt', '|', index_col=0).dropna()
        self.symbols['Security Name'] = self.symbols['Security Name'].map(normalize)
        
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

        for news in self.links:
            content = ''
            
            for i in range(self.retry):
                try:
                    page = opener.open(news['link'], timeout=self.timeout).read()
                    break
                except Exception as e:
                    self.log.error('%s..%s' %(type(e), e))
                    # retry with different proxy
                    if hasattr(self, 'proxies'):
                        proxy = np.random.choice(self.proxies)
                    opener = urllib2.build_opener(urllib2.ProxyHandler({'http':proxy}))
            else:
                self.log.error('could not read news at: %s' %news['link'])
                continue

            parser = BeautifulSoup(page)      # find news body 
            con_finders = ['.body', '.article-body__content', '.article-text',
                           '#storyContent', '.article-body', '.post-content',
                           '.kip-content', '#story', '#mdcMainContent',
                           '#mstarContent', '#content', '.article-entry',
                           '.story', '.storyBody', '.content',
                           '.entry-content', '#storycontent',  
                           '#article_body', '#article-body']
            
            for finder in con_finders:
                try:
                    content = parser.select(finder)[0]
                    
                    for sc in content.select('script'):     # remove scripts
                        sc.decompose()
                        
                    content = content.text
                    #content = self._clean_content(content)
                    break
                except:
                    pass

            if not content:
                self.log.error('could not find content..')
                self.log.debug('news link: %s..' %news['link'])
                continue
                    
            news['content'] = content
            nes = self.ner_tag(content)

            if nes:
                news['nes'] = nes

                # find stocks symbols of the companies in this news
                if 'ORGANIZATION' in news['nes']:
                    symbols = self._find_symbols(news['nes']['ORGANIZATION'])
                    news['symbols'] = symbols
            else:
                self.log.debug('news link: %s..' %news['link'])
                continue
            
            result.append(news)

        return result


    def _clean_content(content):
        for sc in content.select('script'):     # remove scripts
            sc.decompose()

        for a in content.select('aside'):     # remove aside
            s.decompose()
            
        return content.text
    

    # find company names
    def ner_tag(self, content):
        try:
            nes = self.tagger.get_entities(content)

            for k in nes:
                nes[k] = np.unique(nes[k]).tolist()
                
            return nes
        
        except Exception as e:
            self.log.error('could not tag news content..%s' %e)
            return False
        
    # TODO: revise matching accuracy
    
    def _find_symbols(self, orgs):
        symbols = []
        
        for o in orgs:
            o = re.sub(r'Incorporated', 'Inc', o)
            matching = self.symbols['Security Name'].map(lambda n: fuzz.ratio(o, n)).sort_values()
            
            if matching[-1] < 90:       # didnot exceed matching threshold
                #self.log.debug('didnot exceed matching threshold..%s..%d' %(o, matching[-1]))
                continue

            sym = matching.index[-1]
            symbols.append(sym)

        return symbols
