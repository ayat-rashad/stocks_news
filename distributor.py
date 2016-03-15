from distributed_scraper import distributed_scraper

providers = [    'http://finance.yahoo.com/news/provider-accesswire',
                 'http://finance.yahoo.com/news/provider-americancitybusinessjournals',
                 'http://finance.yahoo.com/news/provider-ap',
                 'http://finance.yahoo.com/news/provider-the-atlantic',
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
                 'http://finance.yahoo.com/news/provider-zdnet'
            ]

if __name__ == '__main__':
    proxies = []

    with open('proxies.txt') as f:
        proxies = [l.strip() for l in f.readlines()]

    if not proxies:
        raise Exception('proxies list is empty.')

    for provider in providers[:3]:
        distributed_scraper.delay([provider], proxies) 
