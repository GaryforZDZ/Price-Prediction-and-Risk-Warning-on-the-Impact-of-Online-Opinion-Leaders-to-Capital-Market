# coding=utf-8
from __future__ import print_function, absolute_import
from gm.api import *
import datetime
import pandas as pd
import numpy as np
from sklearn.ensemble import AdaBoostClassifier
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
# import calendar


# 中位数去极值法
def filter_MAD(df, factors, n=3):
    """
    df: 去极值的因子序列
    factor: 待去极值的因子
    n: 中位数偏差值的上下界倍数
    return: 经过处理的因子dataframe
    """
    for factor in factors:
        median = df[factor].quantile(0.5)
        new_median = ((df[factor] - median).abs()).quantile(0.5)
        max_range = median + n * new_median
        min_range = median - n * new_median

        for i in range(df.shape[0]):
            if df.loc[i, factor] > max_range:
                df.loc[i, factor] = max_range
            elif df.loc[i, factor] < min_range:
                df.loc[i, factor] = min_range
    return df


# 策略中必须有init方法
def init(context):
    #从掘金中导入因子数据
    context.factor_names=['PB','PCLFY','PCTTM','PETTM','PSTTM','DY']
    #获取沪深300成分股股票
    context.hs300=get_constituents(index='SHSE.000300')
    #每月的第一个交易日的09:40:00执行策略algo_1
    schedule(schedule_func=algo_1, date_rule='1m', time_rule='9:40:00')
    #设置预测模型
    context.clf = AdaBoostClassifier(n_estimators=100, random_state=0)
    #回测开始后前五个月左右训练模型，之后的时间每个月执行下单委托，所以这里要加一个判断的日期
    #设置训练模型的时间，这里设置回测开始时间的20周
    context.if_date=datetime.datetime.strptime(context.backtest_start_time,'%Y-%m-%d %H:%M:%S')+datetime.timedelta(weeks=20)

def algo_1(context):    
    #获取因子数据
    data=get_fundamentals(table='trading_derivative_indicator',
                    symbols=context.hs300, 
                    start_date=context.now,
                    end_date=context.now,             
                    fields='PB,PCLFY,PCTTM,PETTM,PSTTM,DY', 
                    df=True)
    #对数据进行去极值
    data=filter_MAD(data,context.factor_names)
    #标准化
    data[context.factor_names]=data[context.factor_names].apply(lambda x: (x - np.min(x)) / (np.max(x) - np.min(x)))
    data.index=data['symbol']


    #########为了计算收益率#########################
    #获取上一个月交易日,这里取前四个星期
    # last_date=get_previous_trading_date(exchange='SHSE', date=context.now)
    last_mouth_day=context.now-datetime.timedelta(weeks=4)
    last_mouth_day=last_mouth_day.strftime('%Y-%m-%d')
    ## 返回前一天的交易日
    last_mouth_day=get_previous_trading_date(exchange='SHSE', date=last_mouth_day)

    #获取上个月的这一天的收盘价
    data1=history(symbol=context.hs300, frequency='1d', start_time=last_mouth_day,  end_time=last_mouth_day, fields='symbol,close', adjust=ADJUST_PREV, df= True)
    #合并到表格data中
    data1.index=data1['symbol']
    data['pre_close']=data1['close']

    #获取当前时间的收盘价
    data2 = history(symbol=context.hs300, frequency='1d', start_time=context.now.strftime('%Y-%m-%d'),  end_time=context.now.strftime('%Y-%m-%d'), fields='symbol,close', adjust=ADJUST_PREV, df= True)
    #合并到表格data中
    data2.index=data2['symbol']
    data['close']=data2['close']
    
    #构建bool标签，当收益率为正的话标签为1
    data['y']=data.close>data.pre_close
    data=data.replace({True:1, False:0})
    X=data[context.factor_names]
    y=data['y']
    
    # 训练模型
    if context.now.strftime('%Y-%m-%d')<context.if_date.strftime('%Y-%m-%d'):        
        x_train,X_test,y_train,y_test=train_test_split(X,y,test_size=0.3,random_state=666)
        #将数据喂给模型
        context.clf.fit(x_train, y_train)
        #计算模型的准确率
        print('当前时间为：',context.now,'模型准确率为',context.clf.score(X_test, y_test))##相当于sum(np.array(y_test)==clf.predict(X_test))/len(X_test)
        #预测完将测试数据喂给模型
        context.clf.fit(X_test, y_test)
    #委托下单
    else:
        
        #使用模型进行选股
        symbols=list(X[context.clf.predict(X)==1].index)
        #选出来的股票池为
        print('选出来的股票池为',symbols)
        #股票池数量
        num=len(symbols)

        #平仓
        print('平仓，然后买入标的股票')
        order_close_all()

        #将筛选出来的股票买入
        for symbol in symbols:
            order_percent(symbol=symbol, percent=0.8/num, side=OrderSide_Buy,order_type=OrderType_Market, position_effect=PositionEffect_Open)
        
        #最后将数据继续喂给模型
        context.clf.fit(X, y)

if __name__ == '__main__':
    '''
        strategy_id策略ID, 由系统生成
        filename文件名, 请与本文件名保持一致
        mode运行模式, 实时模式:MODE_LIVE回测模式:MODE_BACKTEST
        token绑定计算机的ID, 可在系统设置-密钥管理中生成
        backtest_start_time回测开始时间
        backtest_end_time回测结束时间
        backtest_adjust股票复权方式, 不复权:ADJUST_NONE前复权:ADJUST_PREV后复权:ADJUST_POST
        backtest_initial_cash回测初始资金
        backtest_commission_ratio回测佣金比例
        backtest_slippage_ratio回测滑点比例
        '''
    run(strategy_id='e9cfb643-1087-11ec-90b8-00ff60fdf6b5',
        filename='main.py',
        mode=MODE_BACKTEST,
        token='{{token}}',
        backtest_start_time='2020-01-01 08:00:00',
        backtest_end_time='2020-11-10 16:00:00',
        backtest_adjust=ADJUST_PREV,
        backtest_initial_cash=10000000,
        backtest_commission_ratio=0.0001,
        backtest_slippage_ratio=0.0001)

