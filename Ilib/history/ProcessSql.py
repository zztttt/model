# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 10:18:52 2019

@author: Irene

------ 日志
--before 190422：sql读写，date日期获取
"""

import pandas as pd
import copy
import numpy as np


def del_sql(tablename, engine='aliyun',schema='StrategyOutput'):
    from sqlalchemy import create_engine
    
    if engine == 'local':
        conname = create_engine('mssql+pymssql://sa:abc123@10.0.0.51:1433/%s?charset=utf8'%schema, encoding='utf-8')
    elif engine == 'aliyun':
        conname = create_engine('mssql+pymssql://ruihui:abc123@106.14.118.247:1433/%s?charset=utf8'%schema, encoding='utf-8')
    conname.execute("drop table [%s].[dbo].[%s]"%(schema,tablename))
    print(' ------ del %s ok ------'%tablename)
    


def to_sql(data,tablename,addtype,read='',engine='aliyun',schema='StrategyOutput', chunksize0=2000, see=True):
    '''    
    to sql
    :data: 
    :tablename:   
    :dtype0: list of column which type is str ['col1','col2']
    :addtype: writing type
    :engine:  loacl  aliyun
    :schema: 
    '''
    from sqlalchemy import create_engine
    from sqlalchemy.types import NVARCHAR, Float, Integer
    from tqdm import tqdm
    
    if engine == 'local':
        conname = create_engine('mssql+pymssql://sa:abc123@10.0.0.51:1433/%s?charset=utf8'%schema, encoding='utf-8')
    elif engine == 'aliyun':
        conname = create_engine('mssql+pymssql://ruihui:abc123@106.14.118.247:1433/%s?charset=utf8'%schema, encoding='utf-8')
    dtype0 = data.columns[data.dtypes.astype(str)=='object'].tolist()
    dtype1 = eval("{%s}" % ",".join(['"%s"'%k+': NVARCHAR(length=255)' for k in dtype0]))
    if len(data)<chunksize0 or not see:
        data.to_sql(tablename, con=conname, if_exists=addtype, index=False, chunksize=chunksize0, dtype=dtype1)
    else:
        try:
            if addtype == 'replace':
                del_sql(tablename, engine=engine,schema=schema)
                print('del, ok')
        except:
            print('del, no need')
        k = 0
        while True:
            try:
                print('------ %s total: %d------'%(tablename, len(data)))
                nn = int((len(data)-1) / chunksize0 + 1)
                for ii in tqdm(k, range(nn)):
                    data.iloc[ii*chunksize0: (ii+1)*chunksize0].to_sql(tablename, con=conname, if_exists='append', index=False, \
                                  chunksize=chunksize0, dtype=dtype1)
                    if k != nn: k = ii + 1
                break
            except:
                pass

def read_f(filename, path='', sep0='\t',tag='utf-8', header=True):        
    ''' 
    本地txt读入
    table_name--表名 
    '''
    if path!='' and path[-1]!='/': path += '/' 
    print('====================读取 '+ filename +' 数据文件')
    if filename.split('.')[1] == 'txt':
        if header == None:
            tradeData = pd.read_table(path + filename, sep=sep0, iterator=True, encoding=tag, header=None)
        else:
            tradeData = pd.read_table(path + filename, sep=sep0, iterator=True, encoding=tag)
    loop = True
    chunksCor = []
    chunkSize = 1000000 # 每次载入chunkSize行
    while loop:
        try:
            chuck_single = tradeData.get_chunk(chunkSize)
            print('==========载入 %s 行' % chunkSize)
            chunksCor.append(chuck_single)
        except StopIteration:
            loop = False
            print("Iteration is stopped.")    
    data = pd.concat(chunksCor)
    print('================一共载入 %s 行' % len(data))
    return data




#    else:
#import tqdm
#        print('\n============== %s: 一共 %d 行待入库 =============='% (tablename, len(data)) )
#        if see: see = 1000
#        nn = int((len(data)-1) / see + 1)
#        for ii in tqdm.tqdm(range(nn)):
#            data.iloc[ii*see: (ii+1)*see].to_sql(tablename, con=conname, if_exists='append', index=False, \
#                          chunksize=chunksize0, dtype=dtype1)
#from sqlalchemy.orm import sessionmaker
#from sqlalchemy.ext.declarative import declarative_base
#from sqlalchemy import Column,Integer,String

##创建对象的基类：
#Base = declarative_base()
#
##定义User对象：
#class User(Base):
#    #表的名字：
#    __tablename__ = 'user'
#
#    #表的结构:
#    userid = Column(Integer,primary_key=True)
#    username = Column(String(20))
#    age = Column(Integer)
#    department = Column(String(20))
#
#tablename = 'commo_prestats0605',
##创建session类型
##创建session对象 #先查询，后删除
#Session = sessionmaker(bind=conname)
#session = Session()
##session.query(tablename).filter(User.username == "Jack").delete()
#session.query(tablename).delete()
#session.commit()
#session.close()
#
# self.session.query(db).delete()
        
#import Ilib
#data = Ilib.read_sql('STstk')        
 

def read_sql(tablename,select=[],cond={},cond_p={},cond_pe={},cond_m={},cond_me={},\
             cond_ne={},engine='aliyun',schema='StrategyOutput'):
    '''    
    read sql
    :tablename: 
    :engine:  loacl  aliyun
    :schema: 
    '''
    from sqlalchemy import create_engine
    if engine == 'local':
        conname = create_engine('mssql+pymssql://sa:abc123@10.0.0.51:1433/%s?charset=utf8'%schema, encoding='utf-8')
    elif engine == 'aliyun':
        conname = create_engine('mssql+pymssql://ruihui:abc123@106.14.118.247:1433/%s?charset=utf8'%schema, encoding='utf-8')
        
    if len(select)==0 + len(cond) + len(cond_p) + len(cond_pe) + len(cond_ne) ==0:
        data = pd.read_sql(tablename, con=conname)
    else:
        txtselect = "*" if len(select)==0 else ",".join(select)
        
        txtconds = ""
        if len(cond_ne) != 0:
            txtL_ne = []
            for key in cond_ne.keys():
                v = cond_ne[key]
                if type(v) != str:
                    txtL_ne.append(key + " >= %s"%v )
                else:
                    txtL_ne.append(key + " >= '%s' "%v )
            txtconds += " and ".join(txtL_ne)
            
        if len(cond_pe) != 0:
            txtL_pe = []
            for key in cond_pe.keys():
                v = cond_pe[key]
                if type(v) != str:
                    txtL_pe.append(key + " >= %s"%v )
                else:
                    txtL_pe.append(key + " >= '%s' "%v )
            txtconds += (" and " if txtconds!="" else "")  + " and ".join(txtL_pe)
                    
        if len(cond_me) != 0:
            txtL_me = []
            for key in cond_me.keys():
                v = cond_me[key]
                if type(v) != str:
                    txtL_me.append(key + " <= %s"%v )
                else:
                    txtL_me.append(key + " <= '%s' "%v )
            txtconds += (" and " if txtconds!="" else "")  + " and ".join(txtL_me)

        if len(cond_p) != 0:
            txtL_p = []
            for key in cond_p.keys():
                v = cond_p[key]
                if type(v) != str:
                    txtL_p.append(key + " > %s"%v )
                else:
                    txtL_p.append(key + " > '%s' "%v )
            txtconds += (" and " if txtconds!="" else "")  + " and ".join(txtL_p)

        if len(cond_m) != 0:
            txtL_m = []
            for key in cond_m.keys():
                v = cond_m[key]
                if type(v) != str:
                    txtL_m.append(key + " < %s"%v )
                else:
                    txtL_m.append(key + " < '%s' "%v )
            txtconds += (" and " if txtconds!="" else "")  + " and ".join(txtL_m)

        if len(cond) != 0:
            txtL = []
            for key in cond.keys():
                v = cond[key]
                if type(v[0]) != str:                
                    txtL.append(key + " in (%s)" % (','.join(map(str,v))) )
                else:
                    txtL.append(key + " in ('%s')" % ("','".join(v)) )
            txtconds += (" and " if txtconds!="" else "")  + " and ".join(txtL)

        if txtconds != "": txtconds = " where %s" % txtconds
        
        sql = "select %s from %s %s" % (txtselect, tablename, txtconds )    
#        print(sql)            
        data = pd.read_sql(sql, con=conname)
    return data
    
#df_data = Ilib.read_sql('ASHAREEODPRICES', engine='local', schema='WIND', cond={'S_INFO_WINDCODE': stks}, \
#                        cond_p={'TRADE_DT': '20070116'}, select=['S_INFO_WINDCODE','TRADE_DT','S_DQ_PCTCHANGE'])
#chg_stk = df_data.drop_duplicates(['TRADE_DT', 'S_INFO_WINDCODE']).pivot(index='TRADE_DT', \
#                               columns='S_INFO_WINDCODE', values='S_DQ_PCTCHANGE').dropna(how='all')
#
#df = dfcc.iloc[:3]
#tablename = 'ASHAREEODPRICES'
#idc = 'S_DQ_PCTCHANGE'
#engine='local'
#schema='WIND'
def read_sql_df(df,tablename,idcs='S_DQ_PCTCHANGE',idxcol=['S_INFO_WINDCODE', 'TRADE_DT'],engine='aliyun',schema='StrategyOutput'):
    '''
    read sql
    :tablename: 
    :engine:  loacl  aliyun
    :schema: 
    '''
    from tqdm import tqdm
    df1 = copy.deepcopy(df).sort_index()
    df1.index = dts = [k.strftime('%Y%m%d') for k in pd.to_datetime(df1.index)]
#    idcs = 'S_DQ_HIGH / S_DQ_PRECLOSE - 1'
    idcs_process = idcs.split(' ')
    import re    
    idc = [k for k in idcs_process if k not in ['+','-','*','**','/'] and re.findall(re.compile("([\d]+)"), k)!=[k]]
#    type(eval(idcs_process[0]))==str
    
    ll = []
    print('-------- total:', len(dts),'--------')
    for dt, nextdt in tqdm(zip(dts, dts[1:]+['20190611'])):
#        print(dt, nextdt)
#        x = df1.loc[dt].dropna().index.tolist()
        ll.append(read_sql(tablename, select=idc+idxcol, cond_pe={idxcol[1]: dt}, cond_m={idxcol[1]:nextdt},\
             cond={idxcol[0]: df1.loc[dt].dropna().index.tolist()}, engine=engine, schema=schema))
#                .pivot(index='TRADE_DT', columns='S_INFO_WINDCODE', values='S_DQ_PCTCHANGE')
        
    df2 = pd.concat(ll).drop_duplicates(idxcol)
    
    expr = " ".join(['%s' if k not in ['+','-','*','**','/'] and re.findall(re.compile("([\d]+)"), k)!=[k] else k for k in idcs_process])
    df2['result'] = eval(expr % tuple(["df2['%s']" %k for k in idc]))
    df3 = df2.pivot(index=idxcol[1], columns=idxcol[0], values='result').dropna(how='all')
    df3.index = pd.to_datetime(df3.index.astype(str))
    return df3
#                 set_index(idxcol[0])[idc])
#        print(dt, nextdt)
#    df1.index = pd.to_datetime(df1.index)
#    Dts = read_sql('TD0', select=['Date'], cond={'A': [1]}, cond_pe={'Date': '2008-01-01'}, cond_me={'Date': '2019-06-10'})['Date'].tolist()
#    DtsD = pd.Series(Dts[:-1], index=Dts[1:])
#    
#    
#    df2 = pd.DataFrame(df1.fillna(0), index=Dts).fillna(method='pad').replace(0, np.nan).dropna(how='all', axis=0).dropna(how='all', axis=1)
    
#    for d in df1.index:
#        x = 
#        
#    df1.index[0]
#    df1.index[0]
#    
    
#    df1.index = pd.to_datetime(pd.Series(df1.index)).apply(lambda x: x.strftime('%Y%m%d'))
#    if len(df1) >= len(df1.T):
#        return df1.apply(lambda x: read_sql(tablename, select=[idc,idxcol[1]], \
#                                cond={idxcol[0]: [x.name], idxcol[1]: x.dropna().index.tolist(),}, \
#                                 engine=engine, schema=schema).set_index(idxcol[1])[idc] )
#    else:
#        return df1.apply(lambda x: read_sql(tablename, select=[idc,idxcol[0]], \
#                                cond={idxcol[0]: x.dropna().index.tolist(), idxcol[1]: [x.name],}, \
#                                 engine=engine, schema=schema).set_index(idxcol[0])[idc] , axis=1)
#
#       
