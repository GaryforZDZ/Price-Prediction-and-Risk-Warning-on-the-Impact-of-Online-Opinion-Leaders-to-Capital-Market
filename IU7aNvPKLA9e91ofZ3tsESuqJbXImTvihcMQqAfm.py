# coding=utf-8
from __future__ import print_function, absolute_import
from gm.api import *
import math
import datetime
import numpy as np
import pandas as pd
import statsmodels.api as sm
import multiprocessing


# 策略中必须有init方法
def init(context):
    # 最大持股数量
    context.num = 30
    # 目标标的（All为全市场)
    context.base_security='ALL'
    # 每天的09:30 定时执行algo任务
    schedule(schedule_func=algo, date_rule='1d', time_rule='14:55:00') 


def algo(context):
    # 当前时间str
    today = context.now.strftime("%Y-%m-%d %H:%M:%S")
    # 下一个交易日
    next_date = get_next_trading_date(exchange='SZSE', date=today)
    # 上一个交易日
    last_date = get_previous_trading_date(exchange='SHSE', date=context.now)
    # # 每周最后一个交易日移仓换股
    # if context.now.weekday()>datetime.datetime.strptime(next_date, '%Y-%m-%d').weekday():
    # 每月最后一个交易日移仓换股
    if context.now.month!=datetime.datetime.strptime(next_date, '%Y-%m-%d').month:
        if context.base_security=='ALL':
            # 获取全A股票（剔除停牌股和ST股）
            all_stocks,all_stocks_str = get_normal_stocks(context.now)
        else:
            # 获取指数成分股
            all_stocks = get_history_constituents(index=context.base_security, start_date=last_date,end_date=last_date)
            all_stocks_str = ','.join(all_stocks[0]['constituents'].keys())
        # 计算因子
        factor = cal_StyleFactor_Size(security=all_stocks_str, date=last_date)
        # 获取最小因子的前N只股票
        trade_stocks = list(factor.replace([-np.inf,np.inf],np.nan).dropna().sort_values(ascending=True)[:context.num].index)
        print(context.now,'待买入股票{}只：{}'.format(len(trade_stocks),trade_stocks))

        ## 股票交易
        # 获取持仓
        positions = context.account().positions()
        # 卖出不在trade_stocks中的持仓(跌停不卖出)
        for position in positions:
            symbol = position['symbol']
            if symbol not in trade_stocks:
                price_limit = get_history_instruments(symbol, fields='lower_limit', start_date=context.now, end_date=context.now, df=True)
                new_price = history(symbol=symbol, frequency='60s', start_time=context.now, end_time=context.now, fields='close', df=True)
                if symbol not in trade_stocks and (len(new_price)==0 or len(price_limit)==0 or price_limit['lower_limit'][0]!=round(new_price['close'][0],2)):
                    # new_price为空时，是开盘后无成交的现象，此处忽略该情况，可能会包含涨跌停的股票
                    current_data = current(symbols=symbol)
                    order_target_percent(symbol=symbol, percent=0, order_type=OrderType_Limit, position_side=PositionSide_Long,price=current_data[0]['price'])
        # 买入股票（涨停不买入）
        for symbol in trade_stocks:
            price_limit = get_history_instruments(symbol, fields='upper_limit', start_date=context.now, end_date=context.now, df=True)
            new_price = history(symbol=symbol, frequency='60s', start_time=context.now, end_time=context.now, fields='close', df=True)
            if len(new_price)==0 or len(price_limit)==0 or price_limit['upper_limit'][0]!=round(new_price['close'][0],2):
                # new_price为空时，是开盘后无成交的现象，此处忽略该情况，可能会包含涨跌停的股票
                current_data = current(symbols=symbol)
                order_target_percent(symbol=symbol, percent=1/len(trade_stocks), order_type=OrderType_Limit, position_side=PositionSide_Long,price=current_data[0]['price'])


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


def cal_StyleFactor_Size(security, date):
    """
    计算风格因子 Size
    :param security 待筛选股票池（list)（这里是secucode）
    :param date 目标日期（int）
    return:Size(DataFrame)
    """
    # get_fundamentals_n中end_date对标的是财报季度最后一天，而非财报发布日期，所以获取的数据会有未来数据
    data = get_fundamentals(table='trading_derivative_indicator', symbols=security, start_date=date, end_date=date, fields='TOTMKTCAP',df=True)
    dfdata = data[data['pub_date']<=date]
    dfdata.set_index(['symbol'],inplace=True)
    Size = dfdata['TOTMKTCAP']
    Size = np.log(Size).replace([-np.inf,np.inf],np.nan).fillna(0)
    # 去极值、标准化、有效样本数量限制、市值中性化
    alpha_factor = winsorize_med(Size)
    alpha_factor = standardlize(alpha_factor)
    # alpha_factor = neutralize_MarketValue(alpha_factor,date)
    return alpha_factor


def winsorize_med(data, scale=3, inclusive=True, inf2nan=True):
    """
    去极值
    :param data：待处理数据[Series]
    :param scale：标准差倍数，默认为3
    :param inclusive：True为将边界外的数值调整为边界值，False为将边界外的数值调整为NaN
    :param inf2nan：True为将inf转化为nan，False不转化
    """
    data = data.astype('float')
    if inf2nan:
        data = data.replace([np.inf, -np.inf], np.nan)
    std_ = data.std()
    mean_ = data.mean()
    if inclusive:
        data[data>mean_+std_*scale]=mean_+std_*scale
        data[data<mean_-std_*scale]=mean_-std_*scale
    else:
        data[data>mean_+std_*scale]=np.nan
        data[data<mean_-std_*scale]=np.nan
    return data


def standardlize(data, inf2nan=True):
    """
    标准化
    :param data:待处理数据
    :param inf2nan：是否将inf转化为nan
    """
    if inf2nan:
        data = data.replace([np.inf, -np.inf], np.nan)
    return (data - data.mean()) / data.std()
    

def neutralize_MarketValue(data,date,counts=1):
    """
    市值中性化
    :param data:待处理数据
    :param date:目标日期
    :param counts：历史回溯天数
    """
    if isinstance(data,pd.Series):
        data = data.to_frame()
    data = data.dropna(how='any')
    security = data.index.to_list()
    market_value = get_fundamentals_n(table='trading_derivative_indicator', symbols=security, end_date=date, fields='TOTMKTCAP',count=counts, df=True)
    max_date = market_value['pub_date'].max()
    market_value = market_value[market_value['pub_date']==max_date][['symbol','TOTMKTCAP']].set_index('symbol').dropna(how='any')
    x = sm.add_constant(market_value)
    common_index = list(set(x.index) & set(data.index))
    x = x.loc[common_index,:]
    data = data.loc[common_index,:]
    residual = sm.OLS(data, x).fit().resid# 此处使用最小二乘回归计算
    return residual


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
        print('{}:标的：{}，操作：以{}{}，委托价格：{}，委托数量：{}'.format(context.now,symbol,order_type_word,side_effect,price,volume))
       

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
    run(strategy_id='5f888d5c-21c7-11ed-ad0d-f46b8c02346f',
        filename='main.py',
        mode=MODE_BACKTEST,
        token='47ca47f849b3a0f66ec0f7013bb56bb667d63a70',
        backtest_start_time='2022-01-01 08:00:00',
        backtest_end_time='2023-01-30 16:00:00',
        backtest_adjust=ADJUST_PREV,
        backtest_initial_cash=1000000,
        backtest_commission_ratio=0.0007,
        backtest_slippage_ratio=0.00123
        )
        