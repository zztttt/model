# -*- coding: utf-8 -*-
"""
Created on Thu May  9 15:21:12 2019

@author: uer
"""

pd.read_excel('./somedata/ref/少数民族分布的主要地区及人口_.xls').iloc[5:,0].dropna()\
    .apply(lambda x: x.replace(' ', '')).to_excel('./somedata民族.xlsx', index=None, header=None)


