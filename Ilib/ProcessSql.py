# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 10:18:52 2019

@author: Irene

------ 日志
--before 190422: to_sql, read_sql
--before 190624: del_sql, read_f, read_sql_df
"""

import pandas as pd
import copy
import numpy as np



def del_sql(tablename, engine='newsql',schema='finance_db'):
    from sqlalchemy import create_engine
    
    if engine == 'local':
        conname = create_engine('mssql+pymssql://sa:abc123@10.0.0.51:1433/%s?charset=utf8'%schema, encoding='utf-8')
    elif engine == 'aliyun':
        conname = create_engine('mssql+pymssql://ruihui:abc123@106.14.118.247:1433/%s?charset=utf8'%schema, encoding='utf-8')
    elif engine == 'c++':
        conname = create_engine("mysql+pymysql://ruihui:Ruihui_123@10.0.0.111:3306/%s"%schema)
    elif engine == 'newsql':
        conname = create_engine("mysql+pymysql://finance_temp:ruihui+123@rm-uf68frea8d8ny3sn8ao.mysql.rds.aliyuncs.com:3306/%s"%schema)
    conname.execute("drop table %s"%(tablename))
    print(' ------ del %s ok ------'%tablename)
    

def update_sql(sqltxt, engine='newsql',schema='finance_db'):
    from sqlalchemy import create_engine
    
    if engine == 'local':
        conname = create_engine('mssql+pymssql://sa:abc123@10.0.0.51:1433/%s?charset=utf8'%schema, encoding='utf-8')
    elif engine == 'aliyun':
        conname = create_engine('mssql+pymssql://ruihui:abc123@106.14.118.247:1433/%s?charset=utf8'%schema, encoding='utf-8')
    elif engine == 'c++':
        conname = create_engine("mysql+pymysql://ruihui:Ruihui_123@10.0.0.111:3306/%s"%schema)
    elif engine == 'newsql':
        conname = create_engine("mysql+pymysql://finance_temp:ruihui+123@rm-uf68frea8d8ny3sn8ao.mysql.rds.aliyuncs.com:3306/%s"%schema)
    elif engine == 'hedge':
        conname = create_engine("mysql+pymysql://hedge_temp:Fields+123@rm-uf68frea8d8ny3sn8ao.mysql.rds.aliyuncs.com:3306/%s"%'hedge_db')
    elif engine == 'graph':
        schema = 'graph_db'
        conname = create_engine("mysql+pymysql://fields_code:Fields+123@rm-uf68frea8d8ny3sn8ao.mysql.rds.aliyuncs.com:3306/%s"%schema)
    elif engine == 'customer':
        conname = create_engine("mysql+pymysql://customer_temp:Fields+123@rm-uf68frea8d8ny3sn8ao.mysql.rds.aliyuncs.com:3306/%s"%'customer_db')
    conname.execute(sqltxt)
#    print(' ------ ok ------')

#data = df1
#data,tablename,addtype = df1, 'cpy_业绩预警', 'replace'
    
#data,tablename,addtype = df1, 'FUTURE_MIN_DATA', 'append'
#engine='c++'
#schema='maintain'
# tablename='test'
# addtype='replace'
# engine='HedgeOut'

    
    
def to_sql(data,tablename,addtype,engine='newsql',schema='finance_db', chunksize0=5000, see=False, diaodu=False):
    '''    
    to sql
    :data: 
    :tablename:   
    :dtype0: list of column which type is str ['col1','col2']
    :addtype: writing type
    :engine:  local / aliyun
    :schema: 
    '''
    from sqlalchemy import create_engine
    from sqlalchemy.types import NVARCHAR, Float, Integer, Date
    import time

    if see:
        print('\n------ %s total: %d, start... ------'%(tablename, len(data)))
    if engine == 'local':
        conname = create_engine('mssql+pymssql://sa:abc123@10.0.0.51:1433/%s?charset=utf8'%schema, encoding='utf-8')
    elif engine == 'aliyun':
        conname = create_engine('mssql+pymssql://ruihui:abc123@106.14.118.247:1433/%s?charset=utf8'%schema, encoding='utf-8')
    elif engine == 'c++':
        conname = create_engine("mysql+pymysql://ruihui:Ruihui_123@10.0.0.111:3306/%s"%schema)
    elif engine == 'newsql':
        conname = create_engine("mysql+pymysql://finance_temp:ruihui+123@rm-uf68frea8d8ny3sn8ao.mysql.rds.aliyuncs.com:3306/%s"%schema)
    elif engine == 'graph':
        schema = 'graph_db'
        conname = create_engine("mysql+pymysql://fields_code:Fields+123@rm-uf68frea8d8ny3sn8ao.mysql.rds.aliyuncs.com:3306/%s"%schema)
    elif engine == 'hedge':
        conname = create_engine("mysql+pymysql://hedge_temp:uhji2563@rm-uf68frea8d8ny3sn8ao.mysql.rds.aliyuncs.com:3306/%s"%'hedge_db')
        schema = 'hedge_db'
    elif engine == 'strategy':
        conname = create_engine('mysql+pymysql://zuowenting:ZOQG6B^#at@192.168.50.225:3306/Strategy-Out')
        schema = 'Strategy-Out'
    elif engine == 'HedgeIn':
        conname = create_engine('mysql+pymysql://zuowenting:ZOQG6B^#at@192.168.50.225:3306/Hedge-In')
        schema = 'Hedge-In'
    elif engine == 'HedgeOut':
        conname = create_engine('mysql+pymysql://zuowenting:ZOQG6B^#at@192.168.50.225:3306/Hedge-Out')
        schema = 'Hedge-Out'
    elif engine == 'customer':
        conname = create_engine("mysql+pymysql://customer_temp:Fields+123@rm-uf68frea8d8ny3sn8ao.mysql.rds.aliyuncs.com:3306/%s"%'customer_db')
    elif engine == 'spyder':
        schema = 'Spider_Database'
        conname = create_engine("mysql+pymysql://xijingwei:e7PeY$2syhNK3Y8O@58.33.183.138:9045/%s"%schema)

#        conname = pymysql.connect(
#                        host='10.0.0.111',
#                        user='ruihui',
#                        passwd='Ruihui_123',
#                        db=schema,
#                        charset='utf8',
#                        autocommit=1
#                    )
    
    data.columns = [k.encode('utf-8').decode('utf-8') for k in data.columns]
    if 'BL' not in tablename.upper():        
        data.columns = [k.replace('(', '_').replace(')','').replace('%','pct').replace('/','_') for k in data.columns]
#    for col in data.columns:
#        data[col] = pd.to_datetime(data[col])
    if addtype == 'replace':
        dtype0 = data.columns[data.dtypes.astype(str)=='object'].tolist()    
        if True:
            dtype0 = [[k, max(255, nk+1)] for k,nk in [[k, data[k].astype(str).apply(lambda x: len(x)).max()] for k in dtype0]]
        dtype1 = eval("{%s}" % ",".join(['"%s"'%k+': NVARCHAR(length=%d)'%nk for k,nk in dtype0]))
        dtype0 = data.columns[data.dtypes.astype(str)=='datetime64[ns]'].tolist()    
        dtype1.update({k: Date for k in dtype0})
    else:
        dtype1 = {}
    
    if see:
        try:
            if addtype == 'replace':
                del_sql(tablename, engine=engine,schema=schema)
                print('del, ok')
        except:
            print('del, no need')

    if len(data)<chunksize0 or not see:
        data.to_sql(tablename, con=conname, if_exists=addtype, index=False, chunksize=chunksize0, dtype=dtype1)
    else:
        nn = int((len(data)-1) / chunksize0 + 1)
        iis = list(range(nn))
        n = 0
        while len(iis)>0 and n<=200:
            if see:
                print('\n------ round %d: %d------'%(n+1, len(iis)))
            if n > 0: 
#                time.sleep(3)
                if see:
                    print(iis)
            n += 1
            
            errorl = []
            if not diaodu:
                from tqdm import tqdm
                for ii in tqdm(iis[::-1]):
                    time.sleep(1)
                    try:
                        data.iloc[ii*chunksize0: (ii+1)*chunksize0].to_sql(tablename, con=conname, if_exists='append', index=False, \
                                      chunksize=chunksize0, dtype=dtype1)
#                        data.iloc[ii*chunksize0: (ii+1)*chunksize0].to_sql(tablename, con=conname, index=False)
                        iis.remove(ii)
                    except Exception as e: 
                        errorl.append(e)
            else:
                for ii in iis[::-1]:
                    time.sleep(1)
                    try:
                        data.iloc[ii*chunksize0: (ii+1)*chunksize0].to_sql(tablename, con=conname, if_exists='append', index=False, \
                                      chunksize=chunksize0, dtype=dtype1)
                        iis.remove(ii)
                    except Exception as e: 
                        errorl.append(e)

            if len(errorl) > 0:
                if see:
                    print(errorl)

    if addtype == 'replace':
        idname = 'tid'
        try:
            try:
                key_name = read_sql("SELECT column_name FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` WHERE table_name='%s' AND constraint_name='PRIMARY'"%tablename, engine=engine, schema=schema).iloc[0, 0]
                update_sql('ALTER TABLE %s DROP COLUMN %s'%(tablename, key_name), engine=engine, schema=schema)
            except:
                pass
            update_sql('alter table %s add %s int not null primary key Auto_increment'%(tablename, idname), engine=engine, schema=schema)
        except:
            pass


#    update_sql('alter table %s add id int NOT NULL IDENTITY(1,1)'%tablename)

##            except:
#            except Exception as e: 
#                print(e)# e变量是Exception类型的实例，支持__str__()方法，可以直接打印。 invalid literal for int() with base 10: 'x'try:  
#
#                pass
                

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



            
#cc500 = pd.read_excel('file:///D:/Q/190101股票多头对冲/成分进出记录500.xlsx')
#cc500['指数'] = '000905.SH'
#cc300 = pd.read_excel('file:///D:/Q/190101股票多头对冲/成分进出记录300.xlsx')
#cc300['指数'] = '000300.SH'
#cc300 = pd.read_excel('file:///D:/Q/190101股票多头对冲/成分进出记录A.xlsx')
#cc300['指数'] = '_A'
#df = pd.concat([ cc300])

#df['日期'] = pd.to_datetime(df['日期'])
#df[['权重（%）', '涨跌幅（%）', '调整时市值(亿元)']] = df[['权重（%）', '涨跌幅（%）', '调整时市值(亿元)']].apply(lambda x: x.apply(lambda y: y.replace(',', ''))).replace('--', np.nan).astype(float)
#df.columns = ['日期', '代码', '简称', '状态', 'wind行业', '权重%', '涨跌幅%', '调整时市值亿元', '指数']
#Ilib.to_sql(df, 'IndexMember', 'append')

    

#else:
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

#def to_sql(data,tablename,addtype,engine='aliyun',schema='StrategyOutput', chunksize0=2000, see=False):
#    '''    
#    to sql
#    :data: 
#    :tablename:   
#    :dtype0: list of column which type is str ['col1','col2']
#    :addtype: writing type
#    :engine:  loacl  aliyun
#    :schema: 
#    '''
#    from sqlalchemy import create_engine
#    from sqlalchemy.types import NVARCHAR, Float, Integer
#    if engine == 'local':
#        conname = create_engine('mssql+pymssql://sa:abc123@10.0.0.51:1433/%s?charset=utf8'%schema, encoding='utf-8')
#    elif engine == 'aliyun':
#        conname = create_engine('mssql+pymssql://ruihui:abc123@106.14.118.247:1433/%s?charset=utf8'%schema, encoding='utf-8')
#    dtype0 = data.columns[data.dtypes.astype(str)=='object'].tolist()
#    dtype1 = eval("{%s}" % ",".join(['"%s"'%k+': NVARCHAR(length=255)' for k in dtype0]))
##    if see==0 or len(data)<=3000:
#    data.to_sql(tablename, con=conname, if_exists=addtype, index=False, chunksize=chunksize0, dtype=dtype1)
#
'''
# 月度未来收益率
dts = Ilib.read_sql('TD1', cond_pe={'Date': '2004-01-01'}, cond_me={'Date': '2019-06-30'}).set_index('Date')['A'].asfreq('M')
df_close = Ilib.read_sql('ASHAREEODPRICES', engine='local', schema='WIND', select=['S_INFO_WINDCODE','TRADE_DT','S_DQ_CLOSE'], \
          cond={'TRADE_DT': dts.apply(lambda x: x.strftime('%Y%m%d')).tolist()}).drop_duplicates(['TRADE_DT','S_INFO_WINDCODE'])\
                 .dropna().pivot(index='TRADE_DT', columns='S_INFO_WINDCODE', values='S_DQ_CLOSE')

import importlib
importlib.reload(Ilib)
df_stks = Ilib.get_idx_cont('_A', dts)

df_close.index = pd.to_datetime(df_close.index)

df_toload = df_stks.apply(lambda x: list(set(x.dropna().index) - set(df_close.loc[x.name].dropna().index)), axis=1)

dt = dts[0]
ll = []
sum = 0
for dt in tqdm(dts):
    stk_tmp = df_toload[dt]
    sum += len(stk_tmp)
#    tmp = Ilib.wsd(stk_tmp, ['close'], dt, dt).iloc[0]
#    ll.append(tmp)
    



df_close.index = dts.index
df_chg = df_close / df_close.shift(1) - 1
df_chg_sql = df_chg.stack().reset_index()
df_chg_sql.columns = ['日期', '代码', '月涨跌幅']
#Ilib.to_sql(df_chg_sql, 'ChgM', 'replace')


'''

#tablename = '50ETF_1min'
#cond_pe={'ddate': self.dt_start}, cond_me={'ddate': self.dt_end}, schema='Options', engine='local'
def read_sql(tablename,select=[],cond={},cond_p={},cond_pe={},cond_p_n={},cond_m={},cond_me={},\
             cond_ne={},cond_like={},engine='newsql',schema='finance_db', see=False):
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
    elif engine == 'c++':
        conname = create_engine("mysql+pymysql://ruihui:Ruihui_123@10.0.0.111:3306/%s"%schema)
    elif engine == 'newsql':
        conname = create_engine("mysql+pymysql://finance_temp:ruihui+123@rm-uf68frea8d8ny3sn8ao.mysql.rds.aliyuncs.com:3306/%s"%schema)
    elif engine == 'customer':
        conname = create_engine("mysql+pymysql://customer_temp:Fields+123@rm-uf68frea8d8ny3sn8ao.mysql.rds.aliyuncs.com:3306/%s"%'customer_db')
    elif engine == 'hedge':
        conname = create_engine("mysql+pymysql://hedge_temp:uhji2563@rm-uf68frea8d8ny3sn8ao.mysql.rds.aliyuncs.com:3306/%s"%'hedge_db')
    elif engine == 'Finbase':
        conname = create_engine('mysql+pymysql://lixiaolong:#P2KM4*rFF@192.168.50.225:3306/Finbase')
    elif engine == 'HedgeIn':
        conname = create_engine('mysql+pymysql://zuowenting:ZOQG6B^#at@192.168.50.225:3306/Hedge-In')
        schema = 'Hedge-In'
    elif engine == 'HedgeOut':
        conname = create_engine('mysql+pymysql://zuowenting:ZOQG6B^#at@192.168.50.225:3306/Hedge-Out')
        schema = 'Hedge-Out'
    elif engine == 'graph':
        schema = 'graph_db'
        conname = create_engine("mysql+pymysql://fields_code:Fields+123@rm-uf68frea8d8ny3sn8ao.mysql.rds.aliyuncs.com:3306/%s"%schema)
    elif engine == 'spyder':
        schema = 'Spider_Database'
        conname = create_engine("mysql+pymysql://xijingwei:e7PeY$2syhNK3Y8O@58.33.183.138:9045/%s"%schema)
    

        # Ilib.read_sql('td0', engine='Finbase')
#    elif engine == 'c++':
#        import pymysql
#        conname = pymysql.connect(
#                        host='10.0.0.111',
#                        user='root',
#                        passwd='RH123@fi',
#                        db=schema,
#                        charset='utf8',
#                        autocommit=1
#                    )
#        conname = create_engine('mssql+mysqldb://ruihui:Ruihui_123@10.0.0.111:3306/%s?charset=utf8'%schema, encoding='utf-8')
#    elif engine == 'c++':
#        from urllib import parse
#        conname = create_engine('mssql+pymssql://root:%s@10.0.0.111:3306/%s?charset=utf8'%(parse.quote_plus('RH123@fi'), schema), encoding='utf-8')
    
    if len(select)==0 + len(cond) + len(cond_p) + len(cond_pe) + len(cond_ne) + len(cond_me) + len(cond_m) + len(cond_like) ==0:
        data = pd.read_sql(tablename, con=conname)
    else:
        txtselect = "*" if len(select)==0 else ",".join(select)
        
        txtconds = ""
        if len(cond_ne) != 0:
            txtL_ne = []
            for key in cond_ne.keys():
                v = cond_ne[key]
                if type(v) != str:
                    txtL_ne.append(key + " <> %s"%v )
                else:
                    txtL_ne.append(key + " <> '%s' "%v )
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

        if len(cond_like) != 0:
            txtL_p = []
            for key in cond_like.keys():
                v = cond_like[key]
                if type(v) != str:
                    txtL_p.append(key + " like %%%%%s%%%%"%v )
                else:
                    txtL_p.append(key + " like '%%%%%s%%%%' "%v )
            txtconds += (" and " if txtconds!="" else "")  + " and ".join(txtL_p)

#        if len(cond_like) != 0:
#            txtL_like = []
#            for key in cond_like.keys():
#                if type(cond_like[key]) == list:
#                    for v in cond_like[key]:
#                        if len(cond_like) != 0:
#                            txtL_like = []
#                            for key in cond_like.keys():
#                                if type(v) != str:
#                                    txtL_like.append(key + " like %%%s%%"%v )
#                                else:
#                                    txtL_like.append(key + " like '%%%s%%' "%v )
#                else:
#                    v = cond_like[key]
#                    if type(v) != str:
#                        txtL_like.append(key + " like %s"%v )
#                    else:
#                        txtL_like.append(key + " like '%s' "%v )
#            txtconds += (" and " if txtconds!="" else "")  + " and ".join(txtL_like)            
            
        if txtconds != "": txtconds = " where %s" % txtconds
        
        sql = "select %s from %s %s" % (txtselect, tablename, txtconds )    
        if see: print(sql[:200])            
        data = pd.read_sql(sql, con=conname)
    data.columns = [k.encode('utf-8').decode('utf-8') for k in data.columns]
    if 'tid' in data.columns.tolist():
        data = data.drop('tid', axis=1)
    return data



def delete_sql(tablename,cond={},cond_p={},cond_pe={},cond_p_n={},cond_m={},cond_me={},\
             cond_ne={},cond_like={},engine='newsql',schema='finance_db', see=True):
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
    elif engine == 'c++':
        conname = create_engine("mysql+pymysql://ruihui:Ruihui_123@10.0.0.111:3306/%s"%schema)
    elif engine == 'newsql':
        conname = create_engine("mysql+pymysql://finance_temp:ruihui+123@rm-uf68frea8d8ny3sn8ao.mysql.rds.aliyuncs.com:3306/%s"%schema)
    elif engine == 'customer':
        conname = create_engine("mysql+pymysql://customer_temp:Fields+123@rm-uf68frea8d8ny3sn8ao.mysql.rds.aliyuncs.com:3306/%s"%'customer_db')
    elif engine == 'hedge':
        conname = create_engine("mysql+pymysql://hedge_temp:uhji2563@rm-uf68frea8d8ny3sn8ao.mysql.rds.aliyuncs.com:3306/%s"%'hedge_db')
#    elif engine == 'c++':
#        conname = create_engine('mssql+mysqldb://ruihui:Ruihui_123@10.0.0.111:3306/%s?charset=utf8'%schema, encoding='utf-8')

    txtconds = ""
    if len(cond_ne) != 0:
        txtL_ne = []
        for key in cond_ne.keys():
            v = cond_ne[key]
            if type(v) != str:
                txtL_ne.append(key + " <> %s"%v )
            else:
                txtL_ne.append(key + " <> '%s' "%v )
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

    if len(cond_like) != 0:
        txtL_like = []
        for key in cond_like.keys():
            if type(cond_like[key]) == list:
                for v in cond_like[key]:
                    if len(cond_like) != 0:
                        txtL_like = []
                        for key in cond_like.keys():
                            if type(v) != str:
                                txtL_like.append(key + " like %%%s%%"%v )
                            else:
                                txtL_like.append(key + " like '%%%s%%' "%v )
            else:
                v = cond_like[key]
                if type(v) != str:
                    txtL_like.append(key + " like %s"%v )
                else:
                    txtL_like.append(key + " like '%s' "%v )
        txtconds += (" and " if txtconds!="" else "")  + " and ".join(txtL_like)

    

    if txtconds != "": txtconds = " where %s" % txtconds
    
    sql = "delete from %s %s" % (tablename, txtconds)    
    if see: print(sql[:100])
    conname.execute(sql)
#        pd.read_sql(sql, con=conname)
#    return data



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
def read_sql_df(df,tablename,idcs='S_DQ_PCTCHANGE+1',idxcol=['S_INFO_WINDCODE', 'TRADE_DT'],engine='aliyun',schema='StrategyOutput'):
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

def sql_txt(tablename,select=[],cond={},cond_p={},cond_pe={},cond_p_n={},cond_m={},cond_me={},\
             cond_ne={},cond_like={}):
    txtselect = "*" if len(select)==0 else ",".join(select)
    
    txtconds = ""
    if len(cond_ne) != 0:
        txtL_ne = []
        for key in cond_ne.keys():
            v = cond_ne[key]
            if type(v) != str:
                txtL_ne.append(key + " <> %s"%v )
            else:
                txtL_ne.append(key + " <> '%s' "%v )
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

    if len(cond_like) != 0:
        txtL_p = []
        for key in cond_like.keys():
            v = cond_like[key]
            if type(v) != str:
                txtL_p.append(key + " like %%%%%s%%%%"%v )
            else:
                txtL_p.append(key + " like '%%%%%s%%%%' "%v )
        txtconds += (" and " if txtconds!="" else "")  + " and ".join(txtL_p)

    if txtconds != "": txtconds = " where %s" % txtconds
    
    sql = "select %s from %s %s" % (txtselect, tablename, txtconds ) 
    return sql

