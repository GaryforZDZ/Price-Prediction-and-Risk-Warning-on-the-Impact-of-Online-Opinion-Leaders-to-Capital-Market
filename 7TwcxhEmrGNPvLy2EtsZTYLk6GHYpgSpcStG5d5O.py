# coding=utf-8
from __future__ import print_function, absolute_import, unicode_literals
from gm.api import *

import datetime
import numpy as np
import pandas as pd
import statsmodels.api as sm

'''
本策略基于Fama-French三因子模型。
策略逻辑：每月初计算Fama三因子（beta、账面市值比、市值），进行时序回归计算因子暴露，再进行截面回归计算因子收益率，取因子收益率最大的前N只股票构建投资组合。
'''


def init(context):
    # 成分股指数
    context.index_symbol = 'SHSE.000300'
    # 数据滑窗
    context.periods = 252
    # 最大持仓
    context.max_holding = 30
    # 每月第一个交易日定时执行algo任务（仿真和实盘时不支持该频率）
    schedule(schedule_func=algo, date_rule='1m', time_rule='09:15:00')
    # 每个交易日的09:30 定时执行algo任务（仿真和实盘时不支持该频率）
    schedule(schedule_func=algo_bug, date_rule='1m', time_rule='09:30:00')

def algo(context):
    # 当前时间
    now = context.now
    # 获取上一个交易日的日期
    last_day = get_previous_trading_date(exchange='SHSE', date=context.now)
    # 获取前2*periods个交易日的日期
    pre_n_day = get_previous_N_trading_date(last_day,counts=2*context.periods)
    # 获取上一个交易日的日期
    pre_n_1_day = get_previous_trading_date(exchange='SHSE', date=pre_n_day)
    # # 获取沪深300成份股
    stock300 = list(get_history_constituents(index=context.index_symbol, start_date=last_day,end_date=last_day)[0]['constituents'].keys())
    # 计算日频收益率
    close = history_new(context, security=','.join(stock300),frequency='1d',start_time=pre_n_1_day,end_time=last_day,fields='eob,symbol,close',skip_suspended=False,fill_missing=None,adjust=ADJUST_PREV, df=True)
    ret = (close/close.shift(1)-1).iloc[1:,:].replace([np.inf,-np.inf],np.nan).fillna(0)
    # 计算指数日频收益
    close_index = history(symbol=context.index_symbol, frequency='1d', start_time=pre_n_1_day, end_time=last_day, fields='eob,close', skip_suspended=False, fill_missing=None, adjust=ADJUST_PREV, adjust_end_time=context.backtest_end_time, df=True).set_index('eob')
    ret_index = (close_index/close_index.shift(1)-1).iloc[1:,:]
    # 假设市场无风险利率为3%
    RiskFreeReturn = 0.03
    RiskFreeReturnDaily = np.power(1+RiskFreeReturn, 1 / 365) - 1
    ret_alpha = ret-RiskFreeReturnDaily
    ret_index_alpha = ret_index-RiskFreeReturnDaily
    # 计算beta因子
    X1 = sm.add_constant(ret_index_alpha)
    Y1 = ret_alpha
    Beta = pd.DataFrame()
    for i in range(len(X1)-context.periods):
        date = X1.index[i+context.periods]
        x = X1.iloc[i:i+context.periods,:]
        y = Y1.iloc[i:i+context.periods]
        model = sm.OLS(y, x).fit()
        params = model.params
        params.columns = ret_alpha.columns
        params = pd.DataFrame(params.iloc[-1,:])
        params.columns = [date]
        Beta = pd.concat([Beta,params],axis=1)
    Beta = Beta.T
    # 获取前N个交易日的日期
    pre_periods_day = get_previous_N_trading_date(last_day,counts=context.periods)
    # 计算账面市值比,为P/B的倒数
    Bp1 = get_fundamentals(table='trading_derivative_indicator', symbols=stock300[:150],
                           start_date=pre_periods_day, end_date=last_day,fields='PB', limit=40000, df=True).set_index(['pub_date','symbol'])
    Bp2 = get_fundamentals(table='trading_derivative_indicator', symbols=stock300[150:],
                           start_date=pre_periods_day, end_date=last_day,fields='PB', limit=40000, df=True).set_index(['pub_date','symbol'])
    Bp = pd.concat([Bp1,Bp2])
    Bp['BP'] = (Bp['PB'] ** -1)
    Bp = Bp[['BP']].unstack().fillna(method='ffill').fillna(method='bfill')
    Bp.columns = Bp.columns.droplevel(level=0)
    # 计算市值
    TotMatCap1 = get_fundamentals(table='trading_derivative_indicator', symbols=stock300[:150],
                           start_date=pre_periods_day, end_date=last_day,fields='TOTMKTCAP', limit=40000, df=True).set_index(['pub_date','symbol'])
    TotMatCap2 = get_fundamentals(table='trading_derivative_indicator', symbols=stock300[150:],
                           start_date=pre_periods_day, end_date=last_day,fields='TOTMKTCAP', limit=40000, df=True).set_index(['pub_date','symbol'])
    TotMatCap = pd.concat([TotMatCap1,TotMatCap2])
    TotMatCap = TotMatCap[['TOTMKTCAP']].unstack().fillna(method='ffill').fillna(method='bfill')
    TotMatCap.columns = TotMatCap.columns.droplevel(level=0)
    # 截面回归第一步：时序回归计算因子暴露
    exposure = pd.DataFrame()
    Y = ret.iloc[-context.periods:,:].fillna(0)
    for stock in stock300:
        try:
            X_ = pd.concat([Beta.loc[:,stock],Bp.loc[:,stock],TotMatCap.loc[:,stock]],axis=1)
            X_.columns = ['Beta','BP','TotMatCap']
            X = sm.add_constant(X_)
            model = sm.OLS(Y.loc[:,stock], X).fit()
            exposure_ = pd.DataFrame(model.params).T
            exposure_.index = [stock]
            exposure = pd.concat([exposure,exposure_])
        except:
            continue
    exposure.dropna(axis=0,how='all',inplace=True)
    exposure = exposure[['Beta','BP','TotMatCap']]
    # 标准化
    exposure = (exposure-exposure.mean())/exposure.std()
    # 截面回归第二步：计算因子预期收益率
    mean_return = Y.mean().loc[exposure.index].replace([np.inf,-np.inf],np.nan).fillna(0)
    model_exposure = sm.OLS(mean_return, exposure).fit()
    expected_return_factor = model_exposure.params
    # 计算股票预期收益
    expected_return_stocks = pd.DataFrame(np.dot(exposure,expected_return_factor),index=exposure.index,columns=['expected_return_stocks'])
    # 筛选买入组合
    context.to_buy = list(expected_return_stocks.sort_values('expected_return_stocks',ascending=False).iloc[:context.max_holding].index)
    print(context.to_buy)
    

def algo_bug(context):
    positions = context.account().positions()
    # 平不在标的池的股票
    for position in positions:
        symbol = position['symbol']
        if symbol not in context.to_buy:
            order_info = order_target_percent(symbol=symbol, percent=0, order_type=OrderType_Market,position_side=PositionSide_Long)

    # 获取股票的权重
    percent = 1 / len(context.to_buy)
    # 买在标的池中的股票
    for symbol in context.to_buy:
        order_info = order_target_percent(symbol=symbol, percent=percent, order_type=OrderType_Market,position_side=PositionSide_Long)


def get_previous_N_trading_date(date,counts=1,exchanges='SHSE'):
    """
    获取end_date前N个交易日,end_date为datetime格式，包括date日期
    :param date：目标日期
    :param counts：历史回溯天数，默认为1，即前一天
    """
    if isinstance(date,str) and len(date)>10:
        date = datetime.datetime.strptime(date,'%Y-%m-%d %H:%M:%S')
    if isinstance(date,str) and len(date)==10:
        date = datetime.datetime.strptime(date,'%Y-%m-%d')
    previous_N_trading_date = get_trading_dates(exchange=exchanges, start_date=date-datetime.timedelta(days=max(counts+30,counts*2)), end_date=date)[-counts]
    return previous_N_trading_date


def history_new(context, security,frequency,start_time,end_time,fields,skip_suspended=True,fill_missing=None,adjust=ADJUST_PREV, df=True):
    # 分区间获取数据（以避免超出数据限制）(start_time和end_date为字符串,fields需包含eob和symbol,单字段)
    Data = pd.DataFrame()
    if frequency=='1d':
        trading_date = get_trading_dates(exchange='SZSE', start_date=start_time, end_date=end_time)
    else:
        trading_date = history('SHSE.000300', frequency=frequency, start_time=start_time, end_time=end_time, fields='eob', skip_suspended=skip_suspended, fill_missing=fill_missing, adjust=adjust, adjust_end_time=context.backtest_end_time, df=df)
        trading_date = trading_date['eob']
    space = 5
    if len(trading_date)<=space:
        Data = history(security, frequency=frequency, start_time=start_time, end_time=end_time, fields=fields, skip_suspended=skip_suspended, fill_missing=fill_missing, adjust=adjust, adjust_end_time=context.backtest_end_time, df=df)
    else:
        for n in range(int(np.ceil(len(trading_date)/space))):
            start = n*space
            end = start+space-1
            if end>=len(trading_date):
                data = history(security, frequency=frequency, start_time=trading_date[start], end_time=trading_date[-1], fields=fields, skip_suspended=skip_suspended, fill_missing=fill_missing, adjust=adjust, adjust_end_time=context.backtest_end_time, df=df)
            else:
                data = history(security, frequency=frequency, start_time=trading_date[start], end_time=trading_date[end], fields=fields, skip_suspended=skip_suspended, fill_missing=fill_missing, adjust=adjust, adjust_end_time=context.backtest_end_time, df=df)
            if len(data)>=33000:
                print('请检查返回数据量，可能超过系统限制，缺少数据！！！！！！！！！！')
            Data = pd.concat([Data,data])    
    Data.drop_duplicates(keep='first',inplace=True)
    if len(Data)>0:
        Data = Data.set_index(['eob','symbol'])
        Data = Data.unstack()
        Data.columns = Data.columns.droplevel(level=0)
    return Data


def get_normal_stocks(date,new_days=365):
    """
    获取目标日期date的A股代码（剔除停牌股、ST股、次新股（365天））
    :param date：目标日期
    :param new_days:新股上市天数，默认为365天
    """
    if isinstance(date,str) and len(date)==10:
        date = datetime.datetime.strptime(date,"%Y-%m-%d")
    elif isinstance(date,str) and len(date)>10:
        date = datetime.datetime.strptime(date,"%Y-%m-%d %H:%M:%S")
    date = date.date()
    # 先剔除退市股、次新股和B股
    df_code = get_instrumentinfos(sec_types=SEC_TYPE_STOCK, fields='symbol, listed_date, delisted_date', df=True)
    df_code['listed_date'] = df_code['listed_date'].apply(lambda x:x.date())
    df_code['delisted_date'] = df_code['delisted_date'].apply(lambda x:x.date())
    all_stocks = [code for code in df_code[(df_code['listed_date']<=date-datetime.timedelta(days=new_days))&(df_code['delisted_date']>date)].symbol.to_list() if code[:6]!='SHSE.9' and code[:6]!='SZSE.2']
    # 再剔除当前的停牌股和ST股
    history_ins = get_history_instruments(symbols=all_stocks, start_date=date, end_date=date, fields='symbol,sec_level, is_suspended', df=True)
    all_stocks = list(history_ins[(history_ins['sec_level']==1) & (history_ins['is_suspended']==0)]['symbol'])
    all_stocks_str = ','.join(all_stocks)
    return all_stocks,all_stocks_str


def on_order_status(context, order):
    # 标的代码
    symbol = order['symbol']
    # 委托价格
    price = order['price']
    # 委托数量
    volume = order['volume']
    # 目标仓位
    target_percent = order['target_percent']
    # 查看下单后的委托状态，等于3代表委托全部成交
    status = order['status']
    # 买卖方向，1为买入，2为卖出
    side = order['side']
    # 开平仓类型，1为开仓，2为平仓
    effect = order['position_effect']
    # 委托类型，1为限价委托，2为市价委托
    order_type = order['order_type']
    if status == 3:
        if effect == 1:
            if side == 1:
                side_effect = '开多仓'
            elif side == 2:
                side_effect = '开空仓'
        else:
            if side == 1:
                side_effect = '平空仓'
            elif side == 2:
                side_effect = '平多仓'
        order_type_word = '限价' if order_type==1 else '市价'
        print('{}:标的：{}，操作：以{}{}，委托价格：{}，目标仓位：{:.2%}'.format(context.now,symbol,order_type_word,side_effect,price,target_percent))


def on_backtest_finished(context, indicator):
    print('*'*50)
    print('回测已完成，请通过右上角“回测历史”功能查询详情。')


if __name__ == '__main__':
    '''
    strategy_id策略ID,由系统生成
    filename文件名,请与本文件名保持一致
    mode实时模式:MODE_LIVE回测模式:MODE_BACKTEST
    token绑定计算机的ID,可在系统设置-密钥管理中生成
    backtest_start_time回测开始时间
    backtest_end_time回测结束时间
    backtest_adjust股票复权方式不复权:ADJUST_NONE前复权:ADJUST_PREV后复权:ADJUST_POST
    backtest_initial_cash回测初始资金
    backtest_commission_ratio回测佣金比例
    backtest_slippage_ratio回测滑点比例
    '''
    run(strategy_id='47911e51-0657-11ed-87d5-f46b8c02346f',
        filename='main.py',
        mode=MODE_BACKTEST,
        token='47ca47f849b3a0f66ec0f7013bb56bb667d63a70',
        backtest_start_time='2018-01-01 08:00:00',
        backtest_end_time='2022-07-24 16:00:00',
        backtest_adjust=ADJUST_PREV,
        backtest_initial_cash=1000000,
        backtest_commission_ratio=0.0001,
        backtest_slippage_ratio=0.0001)