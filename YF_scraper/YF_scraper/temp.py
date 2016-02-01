import pymongo
from pymongo.operations import UpdateOne

client = pymongo.MongoClient('mongodb://localhost:27017')
db = client['test']
items = list(db['test'].find({}))
requests = []

for it in items:
    it['new'] = it['x']
    requests.append(UpdateOne({'_id':it['_id']}, {"$set":{'new':it['new']}}))

db['test'].bulk_write(requests)


