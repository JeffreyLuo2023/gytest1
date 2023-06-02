# -*- coding: utf-8 -*-
"""
Created on Fri Apr  7 11:09:21 2023

@author: GYGJ
"""

import pymysql
import pandas as pd
from tools import * 
import time 



db_conn = pymysql.connect(
    host = '172.16.30.164',
    port = 13306,
    
    user = 'source',
    password = 'WfW2mZE1QUiaPkt',
    database = 'factor_sourcedata',
    charset = 'utf8',
    )


# 获取股票日行情
def Get_EOD_Data(start_date='20190101'):
    start_time = time.time()
    sql = 'select * from EOD where trade_date>=\'{}\''.format(start_date)
    df_temp = pd.read_sql(sql,con=db_conn)
    
    Open = df_temp[['ts_code','trade_date','open']].set_index(['trade_date','ts_code']).unstack()
    Open.columns=[x[1] for x in Open.columns]
    Open.index = [pd.to_datetime(x).date() for x in Open.index]

    high = df_temp[['ts_code','trade_date','high']].set_index(['trade_date','ts_code']).unstack()
    high.columns=[x[1] for x in high.columns]
    high.index = [pd.to_datetime(x).date() for x in high.index]

    low = df_temp[['ts_code','trade_date','low']].set_index(['trade_date','ts_code']).unstack()
    low.columns=[x[1] for x in low.columns]
    low.index = [pd.to_datetime(x).date() for x in low.index]

    close = df_temp[['ts_code','trade_date','close']].set_index(['trade_date','ts_code']).unstack()
    close.columns=[x[1] for x in close.columns]
    close.index = [pd.to_datetime(x).date() for x in close.index]

    volume = df_temp[['ts_code','trade_date','vol']].set_index(['trade_date','ts_code']).unstack()
    volume.columns=[x[1] for x in volume.columns]
    volume.index = [pd.to_datetime(x).date() for x in volume.index]

    amount = df_temp[['ts_code','trade_date','amount']].set_index(['trade_date','ts_code']).unstack()
    amount.columns=[x[1] for x in amount.columns]
    amount.index = [pd.to_datetime(x).date() for x in amount.index]

    adjfactor = df_temp[['ts_code','trade_date','adj_factor']].set_index(['trade_date','ts_code']).unstack()
    adjfactor.columns=[x[1] for x in adjfactor.columns]
    adjfactor.index = [pd.to_datetime(x).date() for x in adjfactor.index]

    trading = df_temp[['ts_code','trade_date','trading']].set_index(['trade_date','ts_code']).unstack()
    trading.columns=[x[1] for x in trading.columns]
    trading.index = [pd.to_datetime(x).date() for x in trading.index]

    up_limit = df_temp[['ts_code','trade_date','up_limit']].set_index(['trade_date','ts_code']).unstack()
    up_limit.columns=[x[1] for x in up_limit.columns]
    up_limit.index = [pd.to_datetime(x).date() for x in up_limit.index]

    down_limit = df_temp[['ts_code','trade_date','down_limit']].set_index(['trade_date','ts_code']).unstack()
    down_limit.columns=[x[1] for x in down_limit.columns]
    down_limit.index = [pd.to_datetime(x).date() for x in down_limit.index]
    end_time = time.time()
    time_cost = round(end_time-start_time,4)
    print('EOD data received. time cost: {}s'.format(time_cost))
    
    return Open,high,low,close,volume,amount,adjfactor,trading,up_limit,down_limit
    


def OpenData(start_date='20200101'):
    sql = 'SELECT ts_code,trade_date,Open FROM `eod` where trade_date >=\'{}\''.format(start_date)
    df_temp = pd.read_sql(sql,con=db_conn)
    Open = df_temp.set_index(['trade_date','ts_code']).unstack()
    Open.columns=[x[1] for x in Open.columns]
    Open.index = [pd.to_datetime(x).date() for x in Open.index]
    return Open


# 获取大盘指数日行情
def IndexHQ1d(index_code='399001.SZ',start_date='20200101'):
    sql = 'select * from indexHQ1d where ts_code=\'{}\''.format(index_code)
    df_temp = pd.read_sql(sql,con=db_conn).sort_values('trade_date')
    return df_temp




# 获取大盘指数成分股及权重
def Get_IndexMW1D_Data(index_code='000300.SH',start_date='20200101'):
    start_time = time.time()
    temp = index_code.split('.')
    sql = 'select * from indexmw1d_{}{} where trade_date>=\'{}\''.format(temp[0],temp[1].lower(),start_date)
    df_temp = pd.read_sql(sql,con=db_conn).drop_duplicates()
    df_temp = df_temp[['con_code','trade_date','weight']].set_index(['trade_date','con_code']).unstack()
    df_temp.columns = [x[1] for x in df_temp.columns]
    df_temp.index = [pd.to_datetime(x).date() for x in df_temp.index]
    end_time = time.time()
    time_cost = round(end_time-start_time,4)
    print('IndexMW1D data received. time cost: {}s'.format(time_cost))
    return df_temp




# 获取申万一级行业进出表
def Get_membswi_Data(level='L1'):
    sql = 'select * from sw_index_2021 where level = \'{}\''.format(level)
    sw_index = pd.read_sql(sql,con=db_conn)
    sql = 'select * from secmembswi'
    swi = pd.read_sql(sql,con=db_conn)
    df_temp = swi[pd.Series([(x in set(sw_index['index_code'])) for x in swi['index_code']])]
    return df_temp


# 获取申万行业名称
def Get_sw_index(level='L1'):
   sql = 'select * from sw_index_2021 where level = \'{}\''.format(level)
   sw_index = pd.read_sql(sql,con=db_conn)
   return sw_index 






# 获取流通市值
def Get_EODDE_Data(col='circ_mv',start_date='20190101'):
    start_time = time.time()
    sql = 'select ts_code,trade_date,{} from EODDE where trade_date>=\'{}\''.format(col,start_date)
    df_temp = pd.read_sql(sql,con=db_conn)
    df_temp = df_temp.set_index(['trade_date','ts_code']).unstack()
    df_temp.columns = [x[1] for x in df_temp.columns]
    df_temp.index = [pd.to_datetime(x).date() for x in df_temp.index]
    end_time = time.time()
    time_cost = round(end_time-start_time,4)
    print('EODDE data received. time cost: {}s'.format(time_cost))
    return df_temp




# 获取股票名称
def Get_stkdes_Data(trade_date):
    sql = 'select * from stkdes where trade_date=\'{}\''.format(trade_date)
    df_temp = pd.read_sql(sql,con=db_conn)
    return df_temp




# 获取交易日期
def GetTradeDate():
    sql = 'select * from dttrd'
    df_temp = pd.read_sql(sql,con=db_conn)
    return df_temp 


















