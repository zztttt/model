# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 10:18:52 2019

@author: Irene

------ 日志
--before 190624：stks, stksD, wsd
"""

import pandas as pd
import Ilib
import numpy as np
import datetime

#tablename, idc, func, enddaydiff = ['hedge_data_TPFP', '', 'wset_tradesuspend', 0]
#tablename, idc, func, enddaydiff = ['hedge_data_IpoExit', 'TradeCode', 'wset_idpd', 0]
#w.wsd("000001.SZ", "maxupordown", "2019-09-30", "2019-10-29", "")


#['hedge_data_ST', 'TradeCode', 'wset_dict', 0]
#tmp = w.wset("indexhistory","startdate=2019-06-13;enddate=2019-06-20;windcode=000300.SH;field=tradedate,")
#w.wset("carryoutspecialtreatment","startdate=2018-10-29;enddate=2019-10-29;field=wind_code,implementation_date")


#tmp = w.wset("resumelistsecurity","startdate=2019-09-29;enddate=2019-10-29;field=wind_code,resume_list_date;sectorid=a001010100000000")
#tmp1 = pd.DataFrame(tmp.Data).T.set_index(0)
#tmp1

#col_dt='Date', col_code='Code', idc=idc, func=func, endday=endday
def sql_update(tablename, suffixD, col_dt='Date', col_code='Code', idc='close', func='wsd', enddaydiff=0):
    
    from WindPy import w
    w.start()
    dt = Ilib.dt(tp='s')
    endday = (Ilib.dt() + datetime.timedelta(days=enddaydiff)).strftime('%Y-%m-%d') if enddaydiff!=0 else Ilib.dt(tp='s')
    w_func = func.split('_')[0]
    w_suffix = func.split('_')[-1]
    if idc == '' or w_suffix == 'A':
        code_lastdate = pd.Series(Ilib.read_sql(tablename, select=['max(Date)']).iloc[0, 0], index=['A'])
        code_wait2update = (pd.to_datetime(code_lastdate[code_lastdate<endday]) + datetime.timedelta(days=1)).astype(str)
    else:
        code_lastdate = Ilib.read_sql('SELECT %s,max(%s) as a FROM %s group by %s order by a'%(col_code, col_dt, tablename, col_code))\
                    .set_index(col_code)['a'].apply(lambda x: x.date()).astype(str)
        code_wait2update = (pd.to_datetime(code_lastdate[code_lastdate<endday]) + datetime.timedelta(days=1)).astype(str)
#    code_wait2update = pd.Series('2019-06-17', index=['000905.SH'])
    
#    code_wait2update['上市'] = '2019-10-01'
#    code_wait2update['退市'] = '2019-10-01'
    
    if len(code_wait2update) == 0:
        print(tablename, ': update, no need')
    else:
        ll = []
        idcl = idc.split(',') if idc != '' else []
#        idc = [idc] if type(idc)!=list else idc
        for code in code_wait2update.index:
            if w_suffix == 'idpd':
#                stksnow = 
                df = Ilib.read_sql('hedge_data_IpoExit')
                df1 = df[df['TradeCode'].apply(lambda x: x.split('.')[-1])!='OC']
                df1.dtypes
#                Ilib.to_sql(df1, 'hedge_data_IpoExit', 'replace')
                w.wset("sectorconstituent","date=%s;sectorid=0201320000000000;field=wind_code"%dt)
                
                
                
            elif w_func == 'wsd':
                tmp = w.wsd(code, idc, code_wait2update[code], endday, "")
                tmp1 = pd.DataFrame(tmp.Data, columns=tmp.Times, index=idcl).T
            elif w_func == 'wsi':
                tmp = w.wsi(code, idc, code_wait2update[code]+" 09:00:00", endday+" 16:00:00", "BarSize=%s"%w_suffix)
                tmp1 = pd.DataFrame(tmp.Data, columns=tmp.Times, index=idcl).T
            elif w_func == 'wset':
                w_suffix0 = w_suffix
                suffix_text = ['startdate=%s'%code_wait2update[code], 'enddate=%s'%endday]
                if w_suffix == 'indexhistory':
                    suffix_text.append('windcode=%s'%code)
                    suffix_text.append('field=tradedate,%s'%idc)
                elif w_suffix == 'tradesuspend':
                    suffix_text.append('field=date,wind_code')
                elif w_suffix == 'dict':
                    if code in ['暂停上市', '恢复上市', '上市']:
                        suffix_text.append('sectorid=a001010100000000')
                    w_suffix0, suffix_text_one = suffixD[code]
                    suffix_text.append('field=%s,wind_code'%suffix_text_one)

                tmp = w.wset(w_suffix0, ';'.join(suffix_text))
                if len(tmp.Data) > 0:
                    if 'Parameter Error' in str(tmp.Data[0][0]):
                        print('\nNONSENSE!!\n')
#                        break
                    tmp1 = pd.DataFrame(tmp.Data).T.set_index(0)
                    tmp1 = tmp1[tmp1.index>code_lastdate[code]]
                    if w_suffix == 'indexhistory' and len(tmp1[2].value_counts()) == 1:
                        tmp1 = []
                else:
                    tmp1 = []
            else:
                print('\nNONSENSE!!\n')
#                break
            if len(tmp1) > 0:
                if idc != '':
                    tmp1[col_code] = code
                ll.append(tmp1)
        if len(ll) > 0:
            df_update = pd.concat(ll).reset_index().dropna()
            df_update.columns = [col_dt] + idcl + [col_code]
            Ilib.to_sql(df_update, tablename, 'append')
            print(tablename, ': update, done')
        else:
            print(tablename, ': update, no data')





#mkt = '_commo'
def stks(mkt, dt='', wgt='', n=np.inf, tp=''):
    '''    
    common contents of idx
    :mkt: 
    :dt:  date
    :tp: '' dataframe, 'l' index list
    '''
    from WindPy import w
    w.start()   
    if dt == '':
        dt = Ilib.dt()
    mktD = {
            '_zx2': 'a39901012f000000',
            '_zx1': 'a39901012e000000',
            '_out': 'a001010m00000000',
            '_commo': '1000010084000000',
            '_A': 'a001010100000000',
            '_STIB': '1000033374000000',
            '_stkidx': 'a39901010d000000',
            '_xsb': 'a001010n00000000',
            '_HK': 'a002010100000000',
            }
    if mkt[0] == '_':
        mkt0 = mktD[mkt]
        way = 'sectorid'
    else:
        mkt0 = mkt
        way = 'windcode'
        
    if mkt == '_STIB':
        tmp = w.wset("sectorconstituent","date=2019-07-23;%s=%s;field=wind_code,sec_name"%(way, mkt0))
    else:
        tmp = w.wset("sectorconstituent","date=%s;%s=%s;field=wind_code,sec_name"%(pd.to_datetime(dt).strftime('%Y-%m-%d'), way, mkt0))

    df_ind = pd.DataFrame(tmp.Data).T.rename(columns={0: '代码',1: '名称'})\
                    .set_index('代码')[['名称']] 
    if mkt == '_zx1':
        df_ind['名称'] = df_ind['名称'].apply(lambda x: x.split('(')[0])
    elif mkt == '_commo':
        df_ind['名称'] = df_ind['名称'].apply(lambda x: x.replace('DCE', '').replace('SHFE', '').replace('CZCE', '').replace('DCE', '').replace('INE', ''))
        
#    df_ind.index = pd.to_datetime(df_ind.index)
    cstk = df_ind.index.tolist()
    if wgt == 'mkt':
        mkt = pd.Series(w.wsd(cstk, "mkt_cap_ard", dt, dt, "unit=1").Data[0], index=cstk)
        if n != np.inf:
            mkt = mkt.sort_values(ascending=False).head(n)
        df_ind['wgt'] = mkt / mkt.sum()
    if tp == '':
        return df_ind, cstk
    elif tp == 'l':
        return cstk
    elif tp == 'df':
        return df_ind

#l, dt = inds, pd.to_datetime(dt_5)
def stksD(l, dt=''):
    ''' 
    common contents of idxs in a list, return a dict
    :l: 
    :dt:  date
    '''
    if dt == '':
        dt = Ilib.dt()
    dfD = {}
    for k in l:
        dfD[k] = Ilib.stks(k, dt=dt, tp='l')
    dfstks = pd.DataFrame.from_dict(dfD, orient='index').T
    return dfstks


def __suffix(period, PriceAdj, Days):
    suffix = ''
    if period != '':
        suffix += "Period=%s"%period
    if PriceAdj != '':
        suffix += "PriceAdj=%s"%PriceAdj
    return suffix


#l = '000001.SZ,000008.SZ,000017.SZ,000018.SZ,000019.SZ,000020.SZ,000021.SZ'.split(',')
#fds = ['close','open','volume']
#dt1 = '2019-5-1'
#dt2 = '2019-5-10'

def __dataT(tmp):
    if len(tmp.Times) == 1:
        return np.array(tmp.Data).T
    else:
        return tmp.Data


#l, fds = df_pro_stk['证券代码'].tolist(), ['sec_name']
def wsd(l, fds, dt1='', dt2='', Period='', PriceAdj='', Days='', suf=''):
    '''    
    common contents of idxs in a list, return a dict
    :l: 
    :dt:  date
    '''
    from WindPy import w
    w.start()
    if dt1 == '':
        dt1 = Ilib.dt()
    if dt2 == '':
        dt2 = Ilib.dt()
    suffix = __suffix(Period, PriceAdj, Days)
    if suf != '':
        suffix += (';' + suf)
#    if len([k for k in fds if 'industry' in k]) > 0 and 'industryType' not in suffix: 
#        suffix += ((';' if suf[-1]!=';' else '') + suf)
    
    if len(l) == 1: 
        tmp = w.wsd(l[0], fds, dt1, dt2, suffix)
        return pd.DataFrame(tmp.Data, columns=tmp.Times, index=fds).T
    elif len(fds) == 1: 
        tmp = w.wsd(l, fds[0], dt1, dt2, suffix)
        return pd.DataFrame(__dataT(tmp), columns=tmp.Times, index=l).T
    else:
        cols =  ['日期','代码','指标','数值']
        if len(l) <= len(fds):
            ll = []
            for k in l:
                tmp = w.wsd(k, fds, dt1, dt2, suffix)
                tmp = pd.DataFrame(tmp.Data, columns=tmp.Times, index=fds).stack()
                tmp.name = k
                ll.append(tmp)
            df = pd.concat(ll, axis=1).stack().reset_index()
            df.columns = ['指标','日期','代码','数值']
        else:
            ll = []
            for k in fds:
                tmp = w.wsd(l, k, dt1, dt2, suffix)
                tmp = pd.DataFrame(__dataT(tmp), columns=tmp.Times, index=l).stack()
                tmp.name = k
                ll.append(tmp)
            df = pd.concat(ll, axis=1).stack().reset_index()
            df.columns = ['代码','日期','指标','数值']
#    df.index = pd.to_datetime(df.index)
    return df[cols]





           
#k = fds[0]
