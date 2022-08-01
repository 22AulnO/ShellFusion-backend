import time

from elasticsearch import Elasticsearch
from elasticsearch import helpers
#import socket
from conf import conf
import nltk
import os
#from offline.file_utils import readTxt

"""
The meta-field '_type' (i.e., index_type or doc_type in some tutorials) was removed since ElasticSearch 7.0.0.
See: https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html.
https://medium.com/elasticsearch/introduction-to-elasticsearch-queries-b5ea254bf455
"""

index_name = 'shellfusion_so'

es = Elasticsearch([f"https://elastic:<yourpassword>@127.0.0.1:9200"],verify_certs=False)
def readTxt(txt): 
    data = []
    with open(txt,"r") as f:
        docs = f.readlines()
    return docs

def createIndex():
    """
    Create an index which will be used for indexing a set of docs (id, content).
    """
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print('delete index: ' + index_name)

    body = {
        "mappings": {
            "properties": {
                "id": {
                    "type": "text"
                },
                "content": {
                    "type": "text",
                    "analyzer": "whitespace"
                }
            }
        }
    }

    es.indices.create(index=index_name, body=body)
    print('create index: ' + index_name)


def indexDocs(docs_txt):  
    """
    Index the docs in a txt file
    """
    createIndex()

    batch = []
    doc = readTxt(docs_txt)
    for line in doc:
        sa = line.split('===>')
        #print(sa[0])
        if len(sa) == 2:
            batch.append({
                '_index': index_name,
                '_source': {
                    'id': sa[0].strip(),
                    'content': sa[1].strip()
                }
            })
            if len(batch) == 50000:
                print('indexing a batch ...')
                helpers.bulk(es, batch, request_timeout=100)
                batch = []

    if len(batch) > 0:
        print('indexing a batch ...')
        helpers.bulk(es, batch, request_timeout=100)


def search(s, res_txt):
    """
    Search for a query string.
    """
    query = {
        "query": {
            'match': {
                'content': s
            }
        }
    }

    # scroll = '5m' means keep the search context active for 5 minutes;
    # size = 100 sets the maximum number of hits per scroll
    res = es.search(index=index_name, body=query, scroll='1m', size=5000) 
    print('# hits:', res['hits']['total']['value'])
    with open(res_txt, 'w') as f:
        for hit in res['hits']['hits'][:1000]:
            f.write(f"{hit['_source']['id']} {hit['_score']}\n")


if __name__ == '__main__':

    # need to start the elasticsearch serive in cmd line
    _lucene_docs_txt = conf.exp_models_dir + '/lucene_docs.txt'
    #_lucene_docs_txt = './lucene_docs.txt'
    _query = 'increas partit size'
    #_res_txt = conf.exp_evaluation_dir + '/1.txt'
    _res_txt = './1.txt'

    #start = time.time()
    indexDocs(_lucene_docs_txt)
    nltk.download('stopwords')
    nltk.download('punkt')
    os.system('mkdir -p ./exp_evaluation_dir/lucene_topN_online')
    os.system('mkdir -p ./exp_evaluation_dir/embed_topn_online')
    print("Index built.")
    #search(_query, _res_txt)
    #print(time.time() - start, 's')  # 20.193s

def doQuery(query,res):
    _lucene_docs_txt = '~/shellfusion_backend/exp_models_dir/lucene_docs.txt'

    start = time.time()
    _res_txt = f"{res}/{query}.txt"
    # do indexDocs on the very first run, then comment it.
    #indexDocs(_lucene_docs_txt)
    search(query, _res_txt)
    total = time.time() - start
    print(f"ES TIME is {total}")
