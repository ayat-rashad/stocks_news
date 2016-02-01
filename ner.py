from nltk.tag import StanfordNERTagger
import os
import pymongo
from nltk import sent_tokenize, wordpunct_tokenize
from itertools import groupby, chain
import numpy as np
from pymongo.operations import UpdateOne


client = pymongo.MongoClient('mongodb://localhost:27017')
db = client['yahoofinance_news']
news = list(db['news'].find({}))

path = 'stanford-ner-2015-04-20/stanford-ner.jar'
os.environ['STANFORD_MODELS'] = 'stanford-ner-2015-04-20/classifiers'
st = StanfordNERTagger('english.all.3class.distsim.crf.ser.gz', path, java_options='-mx2g') 

def find_orgs(token_tags):
        nes = groupby(token_tags, key=lambda d: d[1])
        nes2 = []

        for k, v in nes:
            if k == 'ORGANIZATION':
                nes2.append(' '.join([t[0] for t in v]))
        return nes2
    

doc_tokens = [wordpunct_tokenize(n['content']) for n in news]
nes = map(find_orgs, st.tag_sents(doc_tokens))
nes = map(np.unique, nes)

requests = []
for n, ne in zip(news, nes):
    requests.append(UpdateOne({'_id':n['_id']}, {"$set":{'nes':ne}}))

db['news'].bulk_write(requests)
    
                            
