# -*- coding: utf-8 -*-
"""
Created on Wed May  8 13:50:56 2019

@author: uer
#------ 日志
#--before 190422: ImportC, ImportCrawl
"""

import pandas as pd
import datetime
import time


def box(content, title0='提醒', button='ok'):
    '''
    :content: 提醒内容
    :title0: 方框标题
    :button: 按钮
    '''
    import easygui
    easygui.msgbox("止损！！！！", title=title0, ok_button=button)


def alarm(time_with_text):
    '''
    :time_with_text: Series
    index —— 时间 datetime
    value —— 提醒内容 text
    '''
    time_with_text.index
    testL = ['11:30','14:50','15:00','16:00','16:10']
    wait_test_time = pd.Series([pd.to_datetime(dt + ' '+k+':00') for k in testL])#.tolist()
    wait_test_time = wait_test_time[wait_test_time>=datetime.datetime.now()].tolist()
    
    print('\n------- 日内PNL统计：', wait_test_time, '\n')
    
    for ti in range(len(wait_test_time)):
        nT = wait_test_time[ti]
        print('休眠等待......', nT.strftime('%Y-%m-%d %H:%M'))
        time.sleep((nT - datetime.datetime.now()).total_seconds()+5)
        filename = PNL_日内统计.main()
        print('PNL更新至：%s\n'%nT.strftime('%Y-%m-%d %H:%M'))
        if nT == wait_test_time[-1]: shutil.copy(inroot+filename,inroot+'股票实盘今日PNL_%s.xlsx' % dt)

#
#class mapall():
#    def __init__():
#        pass
#        
#df0 = pd.read_excel('./somedata/中国各个市县名称汇总.xls')#.iloc[5:,:]
#df0.columns = ['省','num','市','县市区']
#df0[['省', '市']] = df0[['省', '市']].fillna(method='pad')
#df0['省'] = df0['省'].apply(lambda x: x.replace('省', '').replace('市', '').replace(' ', ''))
##df0['市'].apply(lambda x: x.replace('省', '').replace('市', '').replace('自治州', ''))
##tmp = df0['市'].value_counts()
#
#df0['市'] = df0['市'].dropna().apply(lambda x: x.replace('盟','').replace('自治州','').replace('藏族','').replace('蒙古族','').replace('蒙古','').replace('族','').replace('地区','').replace('市','').replace(' ','')).replace('省直辖行政单位',np.nan)
#
#import numpy as np
#
#df0 = pd.read_excel('D:/Q/190418图谱/中国各个市县名称汇总.xls',header=None).iloc[5:,:]
#df1 = df0[[0,2]].dropna(how='all').fillna(method='pad')
#df1[0] = df1[0].dropna().apply(lambda x: x.replace('省','').replace('市',''))
#df1[2] = df1[2].dropna().apply(lambda x: x.replace('盟','').replace('自治州','').replace('藏族','').replace('蒙古族','').replace('蒙古','').replace('族','').replace('地区','').replace('市','').replace(' ','')).replace('省直辖行政单位',np.nan)
#df2 = df1.dropna()
#df3 = pd.DataFrame({0:df1[0].drop_duplicates(), 2:df1[0].drop_duplicates(),})
#df1 = df0[[0,3]].dropna(how='all').fillna(method='pad')
#df11 = df1[df1[3].apply(lambda x: True if '市' in x else False)]
#df11[0] = df11[0].dropna().apply(lambda x: x.replace('省','').replace('市','').replace(' ',''))
#df11[3] = df11[3].dropna().apply(lambda x: x.replace('市','').replace(' ',''))
#
##
#dfall = pd.concat([df2, df3,df11]).fillna(method='pad', axis=1).drop_duplicates([0,3]).set_index(0)[3]
#
#df00 = pd.read_excel('D:/Q/190418图谱/KGv4.xlsx')
#df00['名字中省'] = df00['公司'].apply(lambda x: dfall[dfall.apply(lambda y : True if y in x else False)].index).apply(lambda x: x[0] if len(x)>0 else np.nan)
#df00['名字注册地省份对比'] = df00.fillna(method='pad', axis=1).iloc[:,-1]
#df00['对比不同的为1'] = pd.Series(1,index=df00[df00.iloc[:,-3]!=df00.iloc[:,-1]].index)
#df00.drop_duplicates('公司').to_excel('D:/Q/190418图谱/KGv405.xlsx', index=None)
#df00.to_excel('D:/Q/190418图谱/KGv404.xlsx', index=None)
#
#dfall[dfall=='宜兴']
#
#df0 = pd.read_excel('量化私募.xlsx', header=None, index=None).iloc[5:]
#df0.iloc[:,4]
#tmp = df0[2].value_counts()
#tmp0 = tmp[(tmp>7) & (tmp<13)]
#df1 = df0.set_index(2).loc[tmp0.index].reset_index()
#df1[3] = df1[3].astype(float)
#df2 = df1.groupby(2)[3].mean().sort_values(ascending=False).head(20)
#aa = df2.reset_index()
#aa['dsf'] = tmp.loc[df2.index].tolist()
#dfout = df0.set_index(2).loc[df2.index].reset_index()
#
#df0 = pd.read_excel('D:/Q/190418图谱/KGv4.xlsx')
#
#provs = ['北京', '天津', '上海', '重庆', '河北', '山西', '辽宁', 
#         '吉林', '黑龙江', '江苏',
#         '浙江', '安徽', '福建', '江西', '山东', '河南', '湖北', '湖南',
#         '广东', '海南', '四川', '贵州', '云南', '陕西', '甘肃', '青海',
#         '台湾', '内蒙古', '广西', '西藏', '宁夏','新疆', '香港', '澳门']
#ps = pd.Series(provs, index=provs)
#df0['名字比对-省'] = df0['公司'].apply(lambda x: ps[ps.apply(lambda y : True if y in x else False)]).fillna(method='pad', axis=1).iloc[:,-1]
#df0['对比确认'] = df0.fillna(method='pad', axis=1).iloc[:,-1]
#df0['名字和省不对的'] = pd.Series(1,index=df0[df0.iloc[:,-3]!=df0.iloc[:,-1]].index)
#
#df0.to_excel('D:/Q/190418图谱/KGv402.xlsx', index=None)
#
#df0.head()
##
##
##
##
##a = df1[0].value_counts()
##
##    
