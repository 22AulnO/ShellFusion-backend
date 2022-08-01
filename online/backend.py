import websockets
import asyncio
from query_preprocesser import preprocess
import sys
from SimQ_retriever import fullQuery_retrieve_online,w2v_trainer,readTransformLuceneDocs
import os
sys.path.append(os.getcwd())
from offline.file_utils import *
import json
from ES import doQuery
import conf
from ShellFusion_online import full_generate_online
from ShellFusion_online import readCmdInfo
import time
import demjson
#转化句子表示
_kv = w2v_trainer.loadKV(conf.exp_models_dir + '/w2v.kv')
#载入IDF库
_idf = load(conf.exp_models_dir + '/token_idf.dump')
_lucene_docs_txt = conf.exp_models_dir + '/lucene_docs.txt'
_queries_txt = conf.exp_evaluation_dir + '/origin_query.txt'
_lucene_topN_dir = conf.exp_evaluation_dir + '/lucene_topN_online'
_embed_topn_dir = conf.exp_evaluation_dir + '/embed_topn_online'
_QAPairs_det_json = conf.exp_posts_dir + '/QAPairs_det.json'
_mpcmd_info_json = conf.exp_manual_dir + '/mpcmd_info.json'
_icse2022_dir = conf.exp_evaluation_dir + '/icse_2022'
_genans_dir = _icse2022_dir + '/ShellFusion'
_qid_info_dict = readJson(_QAPairs_det_json)
_qid_doc_dict = readTransformLuceneDocs(_lucene_docs_txt, _kv, _idf)
cmd_info_dict, cmd_mid_desc_dict = readCmdInfo(_mpcmd_info_json)
def startup(query):
    return sf_Result(query)

def sf_Result(query):
    os.system(f"rm -rf {_embed_topn_dir}/*")
    os.system(f"rm -rf {_lucene_topN_dir}/*")   
    start = time.time() 
    pre_query =  {'Query':query,'P-Query':preprocess(query)}
    doQuery(pre_query['P-Query'],_lucene_topN_dir)
    fullQuery_retrieve_online(_lucene_topN_dir, _qid_doc_dict, _kv,_idf,50, _embed_topn_dir)
    _k = 5
    result = full_generate_online(pre_query, _embed_topn_dir, _qid_info_dict, cmd_info_dict, cmd_mid_desc_dict, _kv, _idf, False, _k, _genans_dir + '_k' + str(_k))
    full_time = time.time() - start
    print(f"total query time is {full_time}")
    print (demjson.encode(result))
    return result 
async def recv_query(websocket):
    while 1:
        recv_Q = await websocket.recv()
        result = sf_Result(recv_Q)
        await websocket.send(demjson.encode(result))

async def main_logic(websocket, path):


    await recv_query(websocket)

#opening_Query = "Move all files with a certain extension from multiple subdirectories into one directory"
opening_Query = "How to extract the first two characters of a string in shell scripting?"
print(demjson.encode(startup(opening_Query)))
start_server = websockets.serve(main_logic,'0.0.0.0',20004)
asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
