# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 10:18:52 2019

@author: Irene

------ 日志
--before 190624: get_idx_cont, nav_stats, nav_stats_y
"""

import pandas as pd
import Ilib
import numpy as np
import datetime


codeD = {
        '沪深300': '000300.SH',
        '中证500': '000905.SH',
        '港股通': 'H50069.CSI',
#        '': '',
#        '': '',        
        }

##df = pd.read_excel('file:///D:/Q/190723常用数据维护/定期报告披露日期.xlsx').set_index('证券代码').iloc[:-2, 1:]
#df = pd.read_excel('file:///D:/Q/190723常用数据维护/定期报告披露日期-08.xlsx').set_index('证券代码').iloc[:-2, 1:]
#df.columns = Ilib.date_rpt('2000-01-01', '2007-12-31')
#df_rptDT = df.stack().reset_index()
#df_rptDT.columns = ['代码', '报告期', '披露日']
#df_rptDT.dtypes
#Ilib.to_sql(df_rptDT, '披露日', 'append')

def rpt_now():
    dt = Ilib.dt(tp='s')
    dtmd = dt[-5:]
    if dtmd <= '04-30':
        rpt = '0331'
    elif dtmd <= '0831':
        rpt = '0630'
    elif dtmd <= '1030':
        rpt = '0930'
    rpt = dt[:4] + rpt
    return rpt
    
    

def filterExtreme_median(series, n=3):
    median = series.quantile(0.5)
    dev_median = (series - median).abs().quantile(0.5)
    MADe = 1.483 * dev_median
#    return np.clip(series, median - n*MADe, median + n*MADe)
    return series[(series>= median - n*MADe) & (series<= median + n*MADe)]



def Zscore(series):
    return (series - series.mean())/series.std()

#df_fac_m = df_fac_m.apply(lambda x: Ilib.Zscore(x.dropna()), axis=1)
def CAP_INDneutralization(df_fac_m, df_ev, df_ind0):
    import statsmodels.api as sm
    stks = df_fac_m.columns.tolist()
    dts = df_fac_m.index.astype(str).tolist()
    if len(df_ev) == 0:
        df_ev = Ilib.read_sql('evM', cond={'代码': stks, '日期': dts}).drop_duplicates(['代码', '日期']).pivot(index='日期', columns='代码', values='总市值')
    if len(df_ind0) == 0:
        df_ind = Ilib.read_sql('全股票索引').set_index('代码')['中信一级']#.reset_index()
    df_ind = df_ind0.reset_index()
    df_ind['tag'] = 1
    df_ind_stk = df_ind.pivot(index='代码', columns='中信一级', values='tag').fillna(0)
    
    dt = dts[0]
    ll = []
    for dt in dts:
        yx = pd.concat([df_fac_m.loc[dt].dropna() ,sm.add_constant(pd.concat([df_ev.loc[dt], df_ind_stk], axis=1).dropna())], axis=1).dropna()
        if len(yx) > 0:
            result = sm.OLS(yx.iloc[:,0],yx.iloc[:,1:]).fit().resid
            result.name = dt
            ll.append(result)
    df_fac_new = pd.concat(ll, axis=1).T
    df_fac_new.index = pd.to_datetime(df_fac_new.index)
    return df_fac_new


def get_idx_cont(idx_code, dts, tp='', see=True):
    if len(dts) == 1:
        dt = pd.to_datetime(dts[0]).strftime('%Y-%m-%d')
        idx_cont = Ilib.read_sql('IndexMember', cond={'指数': [idx_code]}, cond_me={'日期': dt}, see=see)
        dfstk = idx_cont.sort_values('日期').drop_duplicates(['代码'], keep='last')[['代码', '状态', '简称']]\
                    .replace('剔除', np.nan).dropna()
        if tp == '':
            return dfstk.set_index('代码')[['简称']], dfstk['代码'].tolist()
        elif tp == 'l':
            return dfstk['代码'].tolist()
        elif tp == 'df':
            return dfstk.set_index('代码')[['简称']]
    else:
        dts = [pd.to_datetime(k).strftime('%Y-%m-%d') for k in dts]
        dt = '2019-07-13'
        idx_cont = Ilib.read_sql('IndexMember', cond={'指数': [idx_code]}, see=see)
        df_cont = pd.Series(dts, index=dts).apply(lambda dt: pd.Series(1, index=idx_cont[idx_cont['日期']<=dt].sort_values('日期')\
                                      .drop_duplicates(['代码'], keep='last')['代码'].replace('剔除', np.nan).dropna()))
        df_cont.index = pd.to_datetime(df_cont.index)
        return df_cont


#data = df_chg
def del_stk(data, mkt='A', tp='ts', ipoN=63, ifnot=False):
#    from WindPy import w
#    w.start()   
    if type(data) == pd.core.frame.DataFrame:
        dts = data.index.tolist()
        stks = data.columns.tolist()
    else:
        dts, stks = data
    dts1 = Ilib.read_sql('td1', select=['Date', mkt], engine='newsql',schema='finance_db').set_index('Date')[mkt].loc[dts].apply(lambda x: x.strftime('%Y%m%d')).tolist()
    if tp == 'ts':
        dftrade = Ilib.read_sql('ASHAREEODPRICES', engine='local', schema='WIND', select=['TRADE_DT','S_INFO_WINDCODE','S_DQ_TRADESTATUS'], \
                      cond={'TRADE_DT': dts1, 'S_INFO_WINDCODE': stks,}).set_index(['TRADE_DT','S_INFO_WINDCODE'])['S_DQ_TRADESTATUS']\
                          .apply(lambda x: 0 if x=='停牌' else 1).reset_index().drop_duplicates(['TRADE_DT','S_INFO_WINDCODE'])\
                          .pivot(index='TRADE_DT', columns='S_INFO_WINDCODE', \
                                values='S_DQ_TRADESTATUS').replace('').loc[dts1]
        dftrade.index = dts
#        if '20190430' in dts1:
#            tmp = Ilib.wsd(stks, ['trade_status'], '2019-4-30', '2019-4-30').iloc[0]
#            dftrade.loc['20190430'] = tmp.loc[stks].apply(lambda x: 1 if x=='交易' else 0)            
                          
    elif tp == 'st':
        dfst = Ilib.read_sql('ststk', engine='newsql',schema='finance_db').replace('实施ST', 'input').replace('暂停上市', 'input').replace('撤消ST', 'output')\
                                .replace('恢复上市', 'output').sort_values('日期')
        dfst = dfst[dfst['代码'].isin(stks)]
        dftrade = pd.Series(dts, index=dts).apply(lambda x: dfst[dfst['日期']<x].drop_duplicates(['代码'], keep='last')\
                  .set_index('代码')['类型'].replace('output', np.nan).replace('input', 0)).fillna(1)#.replace(0,np.nan)
    elif tp == 'up':
        dftrade = Ilib.read_sql('ASHAREEODDERIVATIVEINDICATOR', engine='local', schema='WIND', select=['TRADE_DT','S_INFO_WINDCODE',\
                   'UP_DOWN_LIMIT_STATUS'], cond={'TRADE_DT': dts1, 'S_INFO_WINDCODE': stks,}).drop_duplicates(['TRADE_DT','S_INFO_WINDCODE'])\
                .pivot(index='TRADE_DT', 
                        columns='S_INFO_WINDCODE', values='UP_DOWN_LIMIT_STATUS').replace(0, np.nan).replace(-1, np.nan).replace(1, -1)\
                           .fillna(1).replace(-1, 0).loc[dts1]
        dftrade.index = dts
#        if '20190430' in dts1:
#            tmp = Ilib.wsd(stks, ['maxupordown'], '2019-4-30', '2019-4-30').iloc[0]
#            dftrade.loc['20190430'] = tmp.loc[stks].replace(1,np.nan).replace(0,1).replace(-1,1).fillna(0)
    elif tp == 'down':
        dftrade = Ilib.read_sql('ASHAREEODDERIVATIVEINDICATOR', engine='local', schema='WIND', select=['TRADE_DT','S_INFO_WINDCODE',\
                   'UP_DOWN_LIMIT_STATUS'], cond={'TRADE_DT': dts1, 'S_INFO_WINDCODE': stks,}).drop_duplicates(['TRADE_DT','S_INFO_WINDCODE'])\
                .pivot(index='TRADE_DT', 
                        columns='S_INFO_WINDCODE', values='UP_DOWN_LIMIT_STATUS').replace(0, np.nan).replace(1, np.nan)\
                           .fillna(1).replace(-1, 0)
    elif tp == 'ipo':
        dfipo = Ilib.read_sql('ipodt', engine='newsql',schema='finance_db').set_index('日期')['ipo_date'].loc[stks]
        dftrade = pd.Series(dts, index=dts).apply(lambda x: pd.Series(1, \
                           index=dfipo[dfipo < x- datetime.timedelta(days=ipoN) ].index)).fillna(0)
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
    
#nav = a
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
    
    
    



