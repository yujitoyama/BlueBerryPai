import pandas as pd
import numpy as np
from pymongo import MongoClient
import datetime
from janome.tokenizer import Tokenizer
from gensim.models import word2vec
import re

def getcsv(filename):
    '''
    csvファイルを読み込んでpandas dataframe化
    '''
    pmdata = pd.read_csv(filename)
    return pmdata

def updatecsv(tofilename,data,score_jisyousyousai,socore_geninsyousai,repairdays,yokotenkaiclass, jisyoclass):
    ''''
    AIエンジンでの解析後に新csvファイルを作成する関数
    新しい列を追加して、そこに値をappendするイメージ
    '''
    data["jisyousayousai_score"] = score_jisyousyousai
    data["geninsyousai_score"] = socore_geninsyousai
    data["repairdays"] = repairdays
    data["yokotenkaiclass"] = yokotenkaiclass
    data["jisyoclass"] = jisyoclass
    data.to_csv(tofilename,index=True, encoding="utf-8")


def save_dict(dict_objs):
    '''
    mongoDBに接続して、ドキュメントインサートを行う関数
    '''
    client = MongoClient('localhost', 27017)
    db = client.blue_database
    coll = db.test_collection4
    #まとめてインサート
    coll.insert_many(dict_objs)
    ''' 1件ずつインサートも可能
    for dict_obj in dict_objs:
        coll.insert_one(dict_obj)
    return
    '''

def keitaiso(texts):
    '''
    事象詳細/原因詳細等のテキスト文字を形態素解析して、名詞、形容詞等の基本品詞以外を除外する関数
    '''
    results = []
    for text in texts:
        text = text.strip()
        t = Tokenizer()

        #lines = text.split("\n")
        #for line in lines:
        tokens = t.tokenize(text)
        r = []
        for tok in tokens:
            if tok.base_form == "*":
                w = tok.surface
            else:
                w = tok.base_form
            ps = tok.part_of_speech #品詞情報
            hinsi = ps.split(',')[0]
            if hinsi in['名詞','形容詞','動詞','記号']:
                r.append(w)
                rl = ("".join(r)).strip()
        results.append(rl)
    #print(results)
    return results
    
def ngram(s,num):
    '''
    ngram解析用の文字列リスト作成関数
    '''
    res = []
    slen = len(s) - num + 1
    for i in range(slen):
        ss = s[i:i+num]
        res.append(ss)
    return res

def diff_ngram(sa, sb, num):
    '''
    ngramスコア算出関数
    '''
    a = ngram(sa, num)
    b = ngram(sb, num)
    r = []
    cnt = 0
    for i in a:
        for j in b:
            if i == j:
                cnt += 1
                r.append(i)
    return cnt / len(a)

def nlpcal(data):
    '''
    自然言語解析を行なって、事象詳細、原因詳細等のスコアリングを行う関数
    '''
    pmdata = data

    #事象詳細、原因詳細を抽出
    texts_jisyousyousai = pmdata.ix[:,'jisyosyousai']
    texts_geninsyousai = pmdata.ix[:,'geninsyousai']
    
    #事象詳細、原因詳細をリスト化
    ltexts_jisyousyousai = list(texts_jisyousyousai)
    ltexts_geninsyousai = list(texts_geninsyousai)

    #事象詳細、原因詳細を形態素解析
    keitaiso_jisyousyousai = keitaiso(ltexts_jisyousyousai)
    keitaiso_geninsyousai = keitaiso(ltexts_geninsyousai)

    #事象詳細/原因詳細の最悪パターン
    model_jisyousyousai = "誤り停止レスポンスNULL"
    model_geninsyousai = "考慮漏れ設計誤り"

    score_jisyousyousai = []
    socore_geninsyousai = []

    for text in keitaiso_jisyousyousai:
        score = diff_ngram(text,model_jisyousyousai,2)
        score_jisyousyousai.append(score)

    for text in keitaiso_geninsyousai:
        score = diff_ngram(text,model_geninsyousai,2)
        socore_geninsyousai.append(score) 

    return score_jisyousyousai, socore_geninsyousai

def repairdayscal(data):
    '''
    完了日/起票日等の差を算出する関数
    '''
    pmdata = data
    
    #起票日、完了日を抽出
    days_kihyoubi = pmdata.ix[:,'kihyoubi']
    days_kanryoubi = pmdata.ix[:,'kanryoubi']
    
    #起票日、完了日をリスト化
    ldays_kihyoubi = list(days_kihyoubi)
    ldays_kanryoubi = list(days_kanryoubi)

    deltadays = []
    for kihyoubi, kanryoubi in zip(ldays_kihyoubi, ldays_kanryoubi):
        #起票日/完了日を文字列型→時刻型へ変換し、日付の計算をする
        kihyoubidate = datetime.datetime.strptime(kihyoubi, '%Y-%m-%d')
        kanryoubidate = datetime.datetime.strptime(kanryoubi, '%Y-%m-%d')
        deltaday = kanryoubidate - kihyoubidate
        #timedelta型を文字列型に変換
        deltaday = str(deltaday.days)
        deltadays.append(deltaday)
    
    return deltadays

def label_to_class(data):
    '''
    横展開有無などのラベルデータを数字データ(クラスデータ)に変換する関数
    '''
    jisyo = pmdata.ix[:,'jisyo']
    yokotenkai = pmdata.ix[:,'yokotenkai']

    ljisyo = list(jisyo)
    lyokotenkai = list(yokotenkai)

    yokotenkaiclass = []
    for yokotenkai in lyokotenkai:
        if yokotenkai == "あり":
            yokotenkaiclass.append("0")
        else:
            yokotenkaiclass.append("1")
    
    jisyoclass = []
    for jisyo in ljisyo:
        if jisyo == "ABEND":
            jisyoclass.append("0")
        elif jisyo == "システム停止":
            jisyoclass.append("1")
        elif jisyo == "データ破壊":
            jisyoclass.append("2")
        elif jisyo == "性能":
            jisyoclass.append("3") 
        else:                 
            jisyoclass.append("4")

    return yokotenkaiclass, jisyoclass

if __name__ == "__main__":
    filenameorg = "./data/pmdata.csv"
    filename = "./data/appmdata.csv"
    pmdata = getcsv(filenameorg)
    score_jisyousyousai, socore_geninsyousai = nlpcal(pmdata)
    repairdays = repairdayscal(pmdata)
    yokotenkaiclass, jisyoclass = label_to_class(pmdata)

    updatecsv("./data/appmdata.csv",pmdata,score_jisyousyousai,socore_geninsyousai,repairdays,yokotenkaiclass,jisyoclass)
    
    newpmdata = getcsv(filename)

    '''    
    texts = ["人工知能を、利用したシステムは、面白い","人工知能を、利、面白い","人工知能を、利用ステムは、い"]
    keitaiso(texts)
    a = "googleへ絶対に転職する、NTTデータはマネジメントを武器にしているが限界がくる"
    b = "NTTデータの武器はマネジメントだけど、これからの時代それだけでは無理で、googleなどには勝てない"
    a1 = ['人工知能、利用するシステム、面白い', '人工知能、利、面白い', '人工知能、利用ステム、いる']
    b1
    print("2-gram:",diff_ngram(a,b,2))
    '''


    #expmdata = pmdata.ix[:,['node','syorikinou']]
    #print(pmdata)
    pmdata_dict = [dic for index, dic in newpmdata.to_dict(orient="index").items() if index!=0]
    save_dict(pmdata_dict)

    #df = pd.DataFrame([["0001", "John"], ["0002", "Lily"]], columns=['id', 'name'])
    #df['job'] = np.random.randint(0,10,2)
    #print(np.random.randint(0,10,2))
    #print(df)

★totalスコアを算出 /必要なやつらをint化してsumをとってそれを列追加
★funcCをつくる
★funcDをつくる