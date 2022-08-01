
from query_preprocesser import preprocess
import sys
from SimQ_retriever import fullQuery_retrieve_online,w2v_trainer
import os
sys.path.append(os.getcwd())
from offline.file_utils import *
from ES import doQuery
import conf
import os
from ShellFusion_online import full_generate
import time

'''
def mytest(f):
    f = open(f,"r")
    key = '-'
    qid_info_dict = ijson.parse(f)
    for prefix, event, value in qid_info_dict:
        if prefix == '' and event == 'map_key': 
            key = value  # mark the key value
            builder = ObjectBuilder()
        elif prefix.startswith(key):
            builder.event(event, value)
            if event == 'end_map':
                yield key, builder.value
'''
if __name__ == "__main__":
    _kv = w2v_trainer.loadKV(conf.exp_models_dir + '/w2v.kv')
    _idf = load(conf.exp_models_dir + '/token_idf.dump')
    _lucene_docs_txt = conf.exp_models_dir + '/lucene_docs.txt'
    _queries_txt = conf.exp_evaluation_dir + '/origin_query.txt'
    _lucene_topN_dir = conf.exp_evaluation_dir + '/lucene_topN_new'
    _embed_topn_dir = conf.exp_evaluation_dir + '/embed_topn_new'
    _QAPairs_det_json = conf.exp_posts_dir + '/QAPairs_det.json'
    _mpcmd_info_json = conf.exp_manual_dir + '/mpcmd_info.json'
    _icse2022_dir = conf.exp_evaluation_dir + '/icse_2022'
    _genans_dir = _icse2022_dir + '/ShellFusion'

    with open(_queries_txt,"r") as f:
        query_list = []
        for query in f.readlines():
            query_list.append(query)

    pre_query = {}
    # preprocess
    i = 0
    for query in query_list:
        pre_query[i] = {'Query':query,'P-Query':preprocess(query)}
        i += 1
    # return topN
    for i in range(len(pre_query)):
        doQuery(pre_query[i]['P-Query'],_lucene_topN_dir)
    # return topn
    fullQuery_retrieve_online(_queries_txt, _lucene_topN_dir, _lucene_docs_txt, _kv, _idf, 50, _embed_topn_dir) 
    #shellfusion
    _k = 5
    full_generate(pre_query, _embed_topn_dir, _QAPairs_det_json, _k, _genans_dir + '_k' + str(_k))
    totalTime = time.time() - startTime
    print(totalTime/len(pre_query))

