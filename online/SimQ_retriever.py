import os
import time
import sys
sys.path.append("..")

from offline import w2v_trainer
from conf import conf
from offline.file_utils import readTxt, load
from offline.similarity import transformDoc, docSySim
def readQueries_alter(queries_txt):
    """
    key是i=n，sa1是原query，sa2是处理后的
    """
    queries_dict = {}
    files = os.listdir(queries_txt)
    i=0
    for file in files:
        #lines = readTxt(queries_txt+'/'+file)
        queries_dict[i] = { 'P-Query': file[:-4] } #直接返回预处理完毕的query标题
        i+=1
    return queries_dict

def fullQuery_retrieve_online(lucene_topN_dir, qid_doc_dict, kv,idf,n, res_dir):
    """
    Rerank the candidate docs retrieved by lucene for queries using a word embedding based approach.
    """
    #每次进行top-n获取之前删掉top-n文件夹的所有.txt文件，避免干扰
    os.system(f"rm -f {res_dir}/*.txt")
    if not os.path.exists(res_dir):
        os.makedirs(res_dir)
    #TODO:在线每次执行的是单次查询，进行相应修改
    start = time.time()
    queries_dict = readQueries_alter(lucene_topN_dir) 
    #qid_doc_dict = readTransformLuceneDocs(lucene_docs_txt, kv, idf)
    
    topn_qids = set()
    for query_id in queries_dict:
        candi_qids = set()
        topN_txt = lucene_topN_dir + '/' + queries_dict[query_id]['P-Query'] + '.txt' #读取topN文档 原来是按照query的问题id进行读取，现在因为格式改了，直接按P-Query去匹配
        if os.path.exists(topN_txt):
            for line in readTxt(topN_txt):
                sa = line.split(' ') #topN文档后面是分数，此处忽略分数
                candi_qids.add(sa[0])


        start, qid_sim_dict = time.time(), {}
        matrix, idfv = transformDoc(queries_dict[query_id]['P-Query'], kv, idf)
        if matrix is not None and idfv is not None:
            for qid in candi_qids:
                qid_sim_dict[qid] = docSySim(matrix, qid_doc_dict[qid]['matrix'], idfv, qid_doc_dict[qid]['idf']) #有找不到的问题名(qid) lucene_docs里没有
        sl = sorted(qid_sim_dict.items(), key=lambda x:x[1], reverse=True)
        print(queries_dict[query_id]['P-Query'], '->', time.time() - start)


        with open(res_dir + '/' + queries_dict[query_id]['P-Query'] + '.txt', 'w', encoding='utf-8') as f:
            for item in sl[:n]:
                topn_qids.add(item[0])
                f.write(' ===> '.join([item[0], qid_doc_dict[item[0]]['doc'], str(item[1])]) + '\n')
                f.flush()
    finalTime = time.time() - start
    print(f"get topn Time {finalTime}")
    return 0


def fullQuery_retrieve(queries_txt, lucene_topN_dir, lucene_docs_txt, kv, idf, n, res_dir):
    """
    Rerank the candidate docs retrieved by lucene for queries using a word embedding based approach.
    """
    if not os.path.exists(res_dir):
        os.makedirs(res_dir)

    queries_dict = readQueries_alter(lucene_topN_dir) #TODO:这里数据格式要改掉 原来格式是sa0是ID,sa1是原名,sa2是预处理之后。改过的直接返回预处理完成的query
    qid_doc_dict = readTransformLuceneDocs(lucene_docs_txt, kv, idf)

    total_time, topn_qids = 0, set()
    for query_id in queries_dict:
        candi_qids = set()
        topN_txt = lucene_topN_dir + '/' + queries_dict[query_id]['P-Query'] + '.txt' #读取topN文档 原来是按照query的问题id进行读取，现在因为格式改了，直接按P-Query去匹配
        if os.path.exists(topN_txt):
            for line in readTxt(topN_txt):
                sa = line.split(' ') #topN文档后面是分数，此处忽略分数
                candi_qids.add(sa[0])


        start, qid_sim_dict = time.time(), {}
        matrix, idfv = transformDoc(queries_dict[query_id]['P-Query'], kv, idf)
        if matrix is not None and idfv is not None:
            for qid in candi_qids:
                qid_sim_dict[qid] = docSySim(matrix, qid_doc_dict[qid]['matrix'], idfv, qid_doc_dict[qid]['idf']) #有找不到的问题名(qid) lucene_docs里没有
        sl = sorted(qid_sim_dict.items(), key=lambda x:x[1], reverse=True)
        print(queries_dict[query_id]['P-Query'], '->', time.time() - start)
        total_time = time.time() - start

        with open(res_dir + '/' + queries_dict[query_id]['P-Query'] + '.txt', 'w', encoding='utf-8') as f:
            for item in sl[:n]:
                topn_qids.add(item[0])
                f.write(' ===> '.join([item[0], qid_doc_dict[item[0]]['doc'], str(item[1])]) + '\n')
                f.flush()

    return total_time


def retrieve(queries_txt, lucene_topN_dir, lucene_docs_txt, kv, idf, n, res_dir):
    """
    Rerank the candidate docs retrieved by lucene for queries using a word embedding based approach.
    """
    if not os.path.exists(res_dir):
        os.makedirs(res_dir)

    queries_dict = readQueries(queries_txt)
    qid_doc_dict = readTransformLuceneDocs(lucene_docs_txt, kv, idf)

    total_time, topn_qids = 0, set()
    for query_id in queries_dict:
        candi_qids = set()
        topN_txt = lucene_topN_dir + '/' + query_id + '.txt'
        if os.path.exists(topN_txt):
            for line in readTxt(topN_txt):
                sa = line.split('\t')
                if len(sa) == 2:
                    candi_qids.add(sa[0])
        else:
            candi_qids = qid_doc_dict.keys()

        start, qid_sim_dict = time.time(), {}
        matrix, idfv = transformDoc(queries_dict[query_id]['P-Query'], kv, idf)
        if matrix is not None and idfv is not None:
            for qid in candi_qids:
                qid_sim_dict[qid] = docSySim(matrix, qid_doc_dict[qid]['matrix'], idfv, qid_doc_dict[qid]['idf']) #有找不到的问题名(qid) lucene_docs里没有
        sl = sorted(qid_sim_dict.items(), key=lambda x:x[1], reverse=True)
        print(query_id, '->', time.time() - start)
        total_time += time.time() - start

        with open(res_dir + '/' + query_id + '.txt', 'w', encoding='utf-8') as f:
            for item in sl[:n]:
                topn_qids.add(item[0])
                f.write(' ===> '.join([item[0], qid_doc_dict[item[0]]['doc'], str(item[1])]) + '\n')
                f.flush()

    return total_time


def readQueries(queries_txt):
    """
    Read queries.
    """
    queries_dict = {}
    lines = readTxt(queries_txt)
    for line in lines:
        sa = line.split(' ===> ')
        if len(sa) == 3:
            queries_dict[sa[0]] = { 'Query': sa[1], 'P-Query': sa[2] }
    return queries_dict


def readTransformLuceneDocs(lucene_docs_txt, kv, idf):
    """
    Read the docs file for lucene and transform them into matrix representation based on language models.
    """
    id_doc_dict = {}
    for line in readTxt(lucene_docs_txt):
        sa = line.split(' ===> ')
        if len(sa) == 2:
            _id, doc = sa[0], sa[1]
            matrix, idfv = transformDoc(doc, kv, idf)
            if matrix is not None and idfv is not None:
                id_doc_dict[_id] = { 'doc': doc, 'matrix': matrix, 'idf': idfv }
    return id_doc_dict


if __name__ == '__main__':

    if not os.path.exists(conf.exp_evaluation_dir):
        os.makedirs(conf.exp_evaluation_dir)

    _kv = w2v_trainer.loadKV(conf.exp_models_dir + '/w2v.kv')
    _idf = load(conf.exp_models_dir + '/token_idf.dump')
    _lucene_docs_txt = conf.exp_models_dir + '/lucene_docs.txt'
    _queries_txt = conf.exp_evaluation_dir + '/queries.txt'
    _lucene_topN_dir = conf.exp_evaluation_dir + '/lucene_topN'
    _embed_topn_dir = conf.exp_evaluation_dir + '/embed_topn'

    _time = retrieve(_queries_txt, _lucene_topN_dir, _lucene_docs_txt, _kv, _idf, 50, _embed_topn_dir)
    print(_time, 's')  # 32.391s for 434 queries  ~ 0.075s per query
    #每个12秒？！到底哪里卡了？
    #加入topN之后    9s for 434 queires ~
