# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 10:18:52 2019

@author: Irene

------ 日志
--before 190422: ImportC, ImportCrawl
"""


def ImportC():
    a = '''
    \n
    import pandas as pd
    import numpy as np
    from sqlalchemy import create_engine
    from sqlalchemy.types import NVARCHAR, Float, Integer
    from WindPy import w
    w.start()
    import os
    os.chdir(os.path.abspath('.'))
    import shutil
    import datetime
    import copy
    import easygui
    if __name__ =='__main__':
        easygui.msgbox("止损！！！！", title="提醒", ok_button="ok")
    '''
    print(a)


def ImportCrawl():
    a = '''
    \n
    import time
    from selenium import webdriver 
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.wait import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    '''
    print(a)



