# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 10:18:52 2019

@author: Irene

------ 日志
--before 190422：sql读写，date日期获取
"""

import pandas as pd
import Ilib
import numpy as np
import datetime

#data = 
#data = copy.deepcopy(dfstkok)
#dts = 
def del_stk(data, mkt='A', tp='ts', ifnot=False):
#    from WindPy import w
#    w.start()   
    if type(data) == pd.core.frame.DataFrame:
        dts = data.index.tolist()
        stks = data.columns.tolist()
    else:
        dts, stks = data
    dts1 = Ilib.read_sql('TD1', select=['Date', mkt]).set_index('Date')[mkt].loc[dts].apply(lambda x: x.strftime('%Y%m%d')).tolist()
    if tp == 'ts':
        dftrade = Ilib.read_sql('ASHAREEODPRICES', engine='local', schema='WIND', select=['TRADE_DT','S_INFO_WINDCODE','S_DQ_TRADESTATUS'], \
                      cond={'TRADE_DT': dts1, 'S_INFO_WINDCODE': stks,}).set_index(['TRADE_DT','S_INFO_WINDCODE'])['S_DQ_TRADESTATUS']\
                          .apply(lambda x: 0 if x=='停牌' else 1).reset_index().pivot(index='TRADE_DT', columns='S_INFO_WINDCODE', \
                                values='S_DQ_TRADESTATUS').replace('')
        tmp = Ilib.wsd(stks, ['trade_status'], '2019-4-30', '2019-4-30').iloc[0]
        dftrade.loc['20190430'] = tmp.loc[stks].apply(lambda x: 1 if x=='交易' else 0)            
                          
    elif tp == 'st':
        dfst = Ilib.read_sql('STstk').replace('实施ST', 'input').replace('暂停上市', 'input').replace('撤消ST', 'output')\
                                .replace('恢复上市', 'output').sort_values('日期')
        dfst = dfst[dfst['代码'].isin(stks)]
        dftrade = pd.Series(dts, index=dts).apply(lambda x: dfst[dfst['日期']<x].drop_duplicates(['代码'], keep='last')\
                  .set_index('代码')['类型'].replace('output', np.nan).replace('input', 0)).fillna(1)#.replace(0,np.nan)
    elif tp == 'up':
        dftrade = Ilib.read_sql('ASHAREEODDERIVATIVEINDICATOR', engine='local', schema='WIND', select=['TRADE_DT','S_INFO_WINDCODE',\
                   'UP_DOWN_LIMIT_STATUS'], cond={'TRADE_DT': dts1, 'S_INFO_WINDCODE': stks,}).pivot(index='TRADE_DT', 
                        columns='S_INFO_WINDCODE', values='UP_DOWN_LIMIT_STATUS').replace(0, np.nan).replace(-1, np.nan).replace(1, -1)\
                           .fillna(1).replace(-1, 0)
        tmp = Ilib.wsd(stks, ['maxupordown'], '2019-4-30', '2019-4-30').iloc[0]
        dftrade.loc['20190430'] = tmp.loc[stks].replace(1,np.nan).replace(0,1).replace(-1,1).fillna(0)
    elif tp == 'down':
        dftrade = Ilib.read_sql('ASHAREEODDERIVATIVEINDICATOR', engine='local', schema='WIND', select=['TRADE_DT','S_INFO_WINDCODE',\
                   'UP_DOWN_LIMIT_STATUS'], cond={'TRADE_DT': dts1, 'S_INFO_WINDCODE': stks,}).pivot(index='TRADE_DT', 
                        columns='S_INFO_WINDCODE', values='UP_DOWN_LIMIT_STATUS').replace(0, np.nan).replace(1, np.nan)\
                           .fillna(1).replace(-1, 0)
    elif tp == 'ipo':
        dfipo = Ilib.read_sql('ipoDT').set_index('日期')['ipo_date'].loc[stks]
        dftrade = pd.Series(dts, index=dts).apply(lambda x: pd.Series(1, \
                           index=dfipo[dfipo < x- datetime.timedelta(days=63) ].index)).fillna(0)
    dftrade.index = dts
    if ifnot:
        dftrade.fillna(0).repalce(1, np.nan) + 1
    return dftrade.loc[dts, stks].fillna(1)


#data = df_nav[df_nav['tag']=='多空'].set_index('日期').iloc[:,:-1]

#data = data.sort_index()
#data.
#ns = ''
#    nav = data

def nav_stats(nav, chg='', ns=''): 
    nsD = {
            5: '最近一周', 
            21: '最近一月', 
            252: '最近一年', 
            len(nav)-1: '累计至今', 
            }
    nav.index = pd.to_datetime(nav.index)
    chg = nav/nav.shift(1)-1 if len(chg)==0 else chg
    if ns == '':
        ns = [5, 21, 252, len(nav)-1]
    else:
        ns = [ns] if type(ns)==int else ns
    ll = []
    for n in ns:
        if len(nav) >= n:
            nav0 = nav.tail(n)
            chg0 = chg.tail(n)
            df = pd.DataFrame({
                    '累计收益': nav0.apply(lambda x: x.iloc[-1] / x.iloc[0] - 1),
                    '最大回撤': nav0.apply(lambda x: (1 - x / x.cummax()).max()),
                    '夏普': chg0.apply(lambda x: x.mean() / x.std() * np.sqrt(12)),
                    '最近': nsD[n],
                    })
            ll.append(df)
    df0 = pd.concat(ll).reset_index()
    df0.columns = ['策略'] + df0.columns[1:].tolist()
    return df0
    

def nav_stats_y(nav, chg='', ns=''):
    nav.index = pd.to_datetime(nav.index)
    chg = nav/nav.shift(1)-1 if len(chg)==0 else chg
    if ns == '':
        ns = [5, 21, 252, len(nav)-1]
    else:
        ns = [ns] if type(ns)==int else ns
    ys = pd.Series(nav.index, index=nav.index).apply(lambda x: x.year)
    ll = []
    for y in list(set(ys)) + ['全年度']:
        if y == '全年度':            
            tmp = nav_stats(nav, chg=chg, ns='')
        else:
            tmp = nav_stats(nav.loc[ys[ys==y].index], chg=chg, ns='')
        tmp['年'] = y
        ll.append(tmp)
    dfall = pd.concat(ll)
    return dfall


'''
def ipo整理():
    dfipo = Ilib.wsd(Ilib.stks('_A', tp='l') + Ilib.stks('_out', tp='l'), ['ipo_date']).iloc[0].reset_index()
    dfipo.columns = ['日期', 'ipo_date']
    Ilib.to_sql(dfipo, 'ipoDT', 'replace')


def ST整理(): 
#    w.wset("sectorconstituent","date=2019-06-03;sectorid=1000011892000000;field=wind_code")
         
    os.chdir('D:/Q/风险警示/')
    ll = []
    
    df = pd.read_excel('实施ST.xlsx')[['代码', '实施日期']].rename(columns={'实施日期': '日期'})
    df['类型'] = '实施ST'
    ll.append(df)
    
    df = pd.read_excel('撤消ST.xlsx')[['代码', '撤销日期']].rename(columns={'撤销日期': '日期'})
    df['类型'] = '撤消ST'
    ll.append(df)
    
    df = pd.read_excel('暂停上市.xlsx')[['代码', '暂停上市日期']].rename(columns={'暂停上市日期': '日期'})
    df['类型'] = '暂停上市'
    ll.append(df)
    
    df = pd.read_excel('恢复上市.xlsx')[['代码', '恢复上市日期']].rename(columns={'恢复上市日期': '日期'})
    df['类型'] = '恢复上市'
    ll.append(df)
    
    dfall = pd.concat(ll)
    Ilib.to_sql(dfall, 'STstk', 'replace')
    
'''    
    
    
    



