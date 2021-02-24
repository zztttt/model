# -*- coding: utf-8 -*-
"""
Created on Wed May  8 13:50:56 2019

@author: uer

------ 日志
--190508: replace_l, freq
"""


def replace_l(string, Dict):
    for k in Dict:
        string = string.replace(k, Dict[k])
    return string

def isin(string1, string2, tp='lr'):    
    if tp == 'lr':
        strs1 = list(string1)
        strs2 = list(string2)        
        return len([k for k in strs1 if k in string2]) == len(strs1) or len([k for k in strs2 if k in string1]) == len(strs2)

    
def freq(dts,freq):
    dts1 = list(dts)
    return [dts1[k*freq] for k in range(int((len(dts1)-1)/freq)+1)]


def paper(a):
#    al = [k for k in a.split('\n') if k != '']
    return a.replace('\n', ' ').strip(' ')

#alpha = 'a'
def abc2num(alpha):    
    num = ord(alpha.upper()) - 64
    return num

def num2abc(num):   
    abc = chr(64+num)
    return abc

#data, string = df_ctg, fomula_key
def ari4(data, string):
    import re
    import numpy as np
    punc = ['(',')','+','-','*','**','/']
    string_one = string.split(' ')
    idc = [k for k in string_one if k not in punc and re.findall(re.compile("([\d]+)"), k)!=[k]]
    expr = " ".join(['%s' if k not in punc and re.findall(re.compile("([\d]+)"), k)!=[k] else k for k in string_one])
    return eval(expr % tuple(["data['%s'].fillna(0)"%k for k in idc])).replace(0, np.nan)
    
def aristr(string):
    import re
    punc = ['(',')','+','-','*','**','/']
    string_one = string.split(' ')
    idc = [k for k in string_one if k not in punc and re.findall(re.compile("([\d]+)"), k)!=[k]]
    return idc
    

   

'''

'''
