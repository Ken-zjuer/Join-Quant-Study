#三因子模型选股
#2020-01-01到2022-01-01,￥1000000,每天

import statsmodels.api as sm
from statsmodels import regression
import numpy as np
import pandas as pd
import time 
from datetime import date
from jqdata import *

#总体回测前要做的事情
def initialize(context):
    #这些设置的都是不变的全局变量
    set_params()
    #中间变量是会不断改变的
    set_variables()    
    set_backtest()     
    
#设置策参数
def set_params():
    g.tc = 15  #调仓频率
    g.yb = 63  #样本长度，用于计算因子收益率
    g.N = 15   #持仓数目

#设置中间变量
def set_variables():
    g.t = 0               #记录连续回测天数
    g.rf = 0.026          #无风险利率，十年期国债利率
    g.if_trade = False    #当天是否调仓
    #将2020-01-04至今所有交易日弄成列表输出
    #获取当日时间xxxx-xx-xx
    today = date.today()   
    #获取所有交易日
    a = get_all_trade_days()
    #获得len(a)维的单位向量
    g.ATD = ['']*len(a)      
    for i in range(0,len(a)):
        #转换所有交易日为iso格式，一种string
        g.ATD[i] = a[i].isoformat() 
        #将大于今天的列表全部砍掉
        if today<=a[i]:
            break
    #把后面的空字符串去掉
    g.ATD = g.ATD[:i]        

#设置回测条件
def set_backtest():
    set_benchmark('000300.XSHG')
    set_option('use_real_price', True) 
    #为股票设定滑点为百分比滑点
    set_slippage(PriceRelatedSlippage(0.00246),type='stock')  
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5), type='stock')

#每天开盘前要做的事情
def before_trading_start(context):
    if g.t%g.tc==0:
        #每15天准备调仓一次
        g.if_trade=True 
        #调仓当天获得当前开盘的沪深300股票池并剔除当前或者计算样本期间停牌的股票
        g.all_stocks = set_feasible_stocks(get_index_stocks('000300.XSHG'),g.yb,context)
    g.t+=1

#设置可行股票池：
def set_feasible_stocks(stock_list,days,context):
    #得到当天是否停牌的dataframe，停牌1，未停牌0
    suspened_info_df = get_price(list(stock_list),start_date=context.current_dt,end_date=context.current_dt, frequency='daily', fields='paused')['paused'].T
    #得到当日未停牌股票的代码list
    unsuspened_index = suspened_info_df.iloc[:,0]==0
    unsuspened_stocks = suspened_info_df[unsuspened_index].index
    #再筛选出前days天未曾停牌的股票list
    feasible_stocks = []
    current_data = get_current_data()
    for stock in unsuspened_stocks:
        #我们在回测当日选出来的股票在前days必须都不停牌
        if sum(attribute_history(stock,count=days,unit='1d',fields='paused',skip_paused=False))[0]==0:
            feasible_stocks.append(stock)
    return feasible_stocks

#每天交易时
def handle_data(context, data):
    #如果今天是调仓日
    if g.if_trade==True:
        #获得调仓日的日期字符串，保留年月日
        todayStr = str(context.current_dt)[0:10]
        #计算每个股票的α
        ais = FF(g.all_stocks,getDay(todayStr,-g.yb-1),getDay(todayStr,-1),g.rf)
        #为每个持仓股票分配资金
        g.everyStock = context.portfolio.total_value/g.N
        #依α排序，当前需要持仓的股票
        #默认升序（从上到下，是从小到大）
        try:
            stock_sort = ais.sort('score')['code']
        except AttributeError:
            stock_sort = ais.sort_values('score')['code']
        #执行买卖操作
        #购买α小的，也就是认为小的未来有超额回报
        order_stock_sell(context,data,stock_sort)
        order_stock_buy(context,data,stock_sort)       
    #执行完默认赋值为不交易           
    g.if_trade=False

#获得卖出信号，并执行卖出操作
def order_stock_sell(context,data,stock_sort):
    #对于不需要持仓的股票，全仓卖出
    for stock in context.portfolio.positions:
        #除去排名前g.N个股票（选股）
        if stock not in stock_sort[:g.N]:
            stock_sell = stock
            order_target_value(stock_sell,0)

#获得买入信号，并执行买入操作
def order_stock_buy(context,data,stock_sort):
    # 对于需要持仓的股票，按分配到的份额买入
    for stock in stock_sort[:g.N]:
        stock_buy = stock
        order_target_value(stock_buy,g.everyStock)

#按照Fama-French规则计算k个参数并且回归，计算出股票的α
def FF(stocks,begin,end,rf):
    LoS = len(stocks)
    #查询三因子，stocks必须是列表
    q = query(
        valuation.code,
        valuation.market_cap,
        (balance.total_owner_equities/valuation.market_cap/100000000.0).label("BTM")
        ).filter(valuation.code.in_(stocks))
    #根据begin日期前的数据得到最新因子值（并非收益率！）
    df = get_fundamentals(q,begin)
    #选出特征股票组合
    S = df.sort_values('market_cap')['code'][:int(LoS/3)]
    B = df.sort_values('market_cap')['code'][LoS-int(LoS/3):]
    L = df.sort_values('BTM')['code'][:int(LoS/3)]
    H = df.sort_values('BTM')['code'][LoS-int(LoS/3):]
    #获得样本期间的股票价格
    #细节：取64天，不然最前面的第63天收益率为Nan
    df2 = get_price(stocks,begin,end,frequency='daily')
    #取出所有收盘价
    df3 = df2['close'][:]
    #计算收益率
    df4 = df3.diff()/df3.shift(1)
    df4 = df4.iloc[1:]
    #根据股票代码在panel data里索引
    #利用排序法计算因子收益率
    SMB = sum(df4[S],axis=1)/len(S) - sum(df4[B],axis=1)/len(B)
    HMI = sum(df4[H],axis=1)/len(H) - sum(df4[L],axis=1)/len(L)
    #用沪深300作为大盘基准，计算市场因子的日收益率
    dp = get_price('000300.XSHG',begin,end,'1d')['close']
    #注意将无风险利率转为日收益率（不考虑复利）
    RM = np.mean((dp.diff()/dp.shift(1)).iloc[1:]) - rf/252
    #将三因子做成数据表
    X = pd.DataFrame({"RM":RM,"SMB":SMB,"HMI":HMI})
    #每次调仓的时候打印一次
    print(X)
    #对每个样本个股进行线性回归并计算αi
    #t_scores用于存储αi
    t_scores = [0.0]*LoS
    for i in range(LoS):
        t_stock = stocks[i]
        #用三因子对个股的63天的收益率序列进行回归
        t_r = linreg(X,df4[t_stock]-rf/252)
        t_scores[i] = t_r[0]
    #这个scores就是alpha，即超额收益 
    scores = pd.DataFrame({'code':stocks,'score':t_scores})
    return scores

#辅助线性回归的函数
def linreg(X,Y,columns=3):
    #columns表示X默认3列，即三因子
    #构建X，为第一列添加常数项
    X = sm.add_constant(array(X))
    Y = array(Y)
    results = regression.linear_model.OLS(Y,X).fit()
    return results.params

#日期计算：获得某个日期之前或者之后dt个交易日的日期
def getDay(present,dt):
    for i in range(0,len(g.ATD)):
        if present<=g.ATD[i]:
            t_temp = i
            #大于0向后位移
            if t_temp+dt>=0:
                #present偏移dt天后的日期
                #return会自动停止循环
                return g.ATD[t_temp+dt]
            #小于0向前位移
            else:
                t = datetime.datetime.strptime(g.ATD[0],'%Y-%m-%d')-datetime.timedelta(days=dt)
                t_str = datetime.datetime.strftime(t,'%Y-%m-%d')
                return t_str

#每天收盘后要做的事情
def after_trading_end(context):
    pass


