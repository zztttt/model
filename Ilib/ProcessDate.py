# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 10:18:52 2019

@author: Irene

------ 日志
--190522: dt
--190508: date_lastp
--190422：date_offset
--190422：date_freq，date_rpt
"""

import pandas as pd
import datetime

def __dtTrans(dt, tp):
    dt = pd.to_datetime(dt)
    if tp == '':
        return dt
    elif tp == 's':
        return dt.strftime('%Y-%m-%d')
    elif tp == 'i':
        return dt.strftime('%Y%m%d')
        

def dt(t=15, Days='T', tp=''):
    '''    
    return end day
    :t: hour / time
    :Days: type of dates
    '''
    try:
        import datetime
        now = datetime.datetime.today()
        if now.strftime('%H:%m') < '%02d:00'%t:
            nn = 0 if date_offset(now,sft=0,Days=Days).strftime('%Y-%m-%d') != now.strftime('%Y-%m-%d') else -1
            dt0 = date_offset(now,sft=nn,Days=Days).strftime('%Y-%m-%d')
        else:
            dt0 = date_offset(now,sft=0,Days=Days).strftime('%Y-%m-%d')#now.strftime('%Y-%m-%d')
    except:
        dt0 = now
    return __dtTrans(dt0, tp)
    

def date_lastp(day, wd=1, tp=''):
    '''    
    get last weekday
    :day:
    :wd: week 1-7
    '''
    import calendar
    import datetime
    dt1 = pd.to_datetime(day)
    wdD = {1: calendar.MONDAY, 2: calendar.TUESDAY, 3: calendar.WEDNESDAY, 
           4: calendar.THURSDAY, 5: calendar.FRIDAY, 6: calendar.SATURDAY, 
           7: calendar.SUNDAY,}
    oneday = datetime.timedelta(days = 1)
    dt1 -= oneday
    while dt1.weekday() != wdD[wd]:
        dt1 -= oneday
    return __dtTrans(dt1, tp)
    

#day = dt0
#sft = -1
#Days='C'
def date_offset(day,sft=1,Days='T',Period='',MKT='A', tp=''):
    '''    
    wind get date
    :day:
    :sft: shift
    :Days: W weekday C calendar T tradeday
    :Period: W M Q S Y
    :MKT: HK  A
    '''
    from WindPy import w
    w.start()
    DaysD = {'W': 'Weekdays', 'C':'Alldays'}
    MarketD = {'HK': 'HKEX', 'A':''}
    suffix = ''
    if Days!='T': suffix += 'Days=%s;'%DaysD[Days]
    if Period!='': suffix += 'Period=%s;'%Period
    if MKT!='A': suffix += 'MKT=%s;'%MarketD[MKT]
    day0 = w.tdaysoffset(sft, day, suffix).Data[0][0]
    return __dtTrans(day0, tp)
    


def date_freq(start,end,freq=1,Days='T',Period='',MKT='A'):
    '''    
    wind get date
    :start: start date
    :end: end date
    :freq: freqency
    :Days: W weekday C calendar T tradeday
    :Period: W M Q S Y
    :MKT: HK  A
    '''
    from WindPy import w
    w.start()
    DaysD = {'W': 'Weekdays', 'C':'Alldays', 'T': ''}
    MarketD = {'HK': 'HKEX', 'A':''}
    suffix = ''
    if Days!='': suffix += 'Days=%s;'%DaysD[Days]
    if Period!='': suffix += 'Period=%s;'%Period
    if MKT!='': suffix += 'MKT=%s;'%MarketD[MKT]
    dts = w.tdays(start, end, suffix).Data[0]
    dtsBYfreq = [dts[k*freq] for k in range(int((len(dts)-1)/freq)+1)]
    return dtsBYfreq



def time_freq(freq=15*60,start=datetime.time(9,30,00), end=datetime.time(15,00,00), dt=datetime.date.today(), tp='end'):
    startT = datetime.datetime.combine(dt, start)
    endT = datetime.datetime.combine(dt, end)
    nn = int((endT - startT).total_seconds() / freq + 1)
    ts = [startT + datetime.timedelta(seconds=freq*i) for i in range(nn)]
    midcut1 = datetime.datetime.combine(dt, datetime.time(11,30,00))
    midcut2 = datetime.datetime.combine(dt, datetime.time(13,00,00))
    if tp == '':
        tsBYfreq = [t for t in ts if t<midcut1 or (t>=midcut2 and t<endT)]
    else:
        tsBYfreq = [t for t in ts if t<midcut1 or (t>=midcut2 and t<=endT)]
    return tsBYfreq
    

def date_rpt(start,end):
    '''    
    get date as rpt
    :start: start day
    :end: end day
    '''
    st = pd.to_datetime(start)
    ed = pd.to_datetime(end)
    dts = pd.to_datetime(pd.Series([str(y)+md for y in range(st.year-1, 
                     ed.year+1) for md in ['0331','0630','0930','1231']]))
    dtsBYrpt = dts[(dts>=st) & (dts<=ed)].tolist()
    return dtsBYrpt


## 日期处理
#import Ilib
#import numpy as np
#df0 = pd.DataFrame(columns=['A','HK'], index=Ilib.date_freq('2000-01-01', '2020-12-31', Days='C'))
#df0.loc[Ilib.date_freq('2000-01-01', '2020-12-31', Days='T', MKT='A'), 'A'] = 1
#df0.loc[Ilib.date_freq('2000-01-01', '2020-12-31', Days='T', MKT='HK'), 'HK'] = 1
#df0.index.name = 'Date'
#df0.reset_index()
#
#df1 = df0.apply(lambda x: pd.Series(x.dropna().index, \
#                                      index=x.dropna().index)).loc[df0.index].fillna(method='pad').reset_index()
#
#Ilib.to_sql(df0.reset_index(), 'TD0', 'replace')
#Ilib.to_sql(df1, 'TD1', 'replace')




    