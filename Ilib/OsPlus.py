# -*- coding: utf-8 -*-
"""
Created on Wed May  8 13:50:56 2019

@author: uer
------ 日志
--190508: rename
"""

import os
import Ilib


def rename(path, Dict):
    from tqdm import tqdm
    if path[-1] != '/': path += '/'
    for f in tqdm(os.listdir(path)):
        newf = Ilib.replace_l(f, Dict)
        os.rename(path + f, path + newf)
    
    
