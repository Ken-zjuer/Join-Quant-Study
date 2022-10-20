#导入函数库
from jqdata import *
import datetime
import pandas as pd
import numpy as np

#初始化函数，设定基准等等
def initialize(context):
    set_benchmark('000300.XSHG')
    set_option('use_real_price', True)
    #手续费设定
    set_slippage(PriceRelatedSlippage(0.00246),type='stock')  
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    #调仓默认1个月一次，跟预测频率一样
    #获取每个月期初的交易日日期
    g.day_lst = get_month_firstday()
    
def get_month_firstday():
    '''
    返回：datetime格式的所有月初交易日
    '''
    day_lst = []
    month_lst = []
    year_lst = np.unique([i.year for i in get_all_trade_days()])
    for year in year_lst:
        for date in get_all_trade_days():
            if date.year==year:
                if len(month_lst)==12:
                    month_lst = []
                    break
                else:
                    if date.month not in month_lst:
                        day_lst.append(date)
                        month_lst.append(date.month)
    return day_lst
    
#开盘前
def before_trading_start(context):
    pass

#开盘时
def handle_data(context,data):
    #默认每个月月初调仓
    if context.current_dt.date() in g.day_lst:
        #end_date在count时被包括
        start_date = get_trade_days(end_date=context.current_dt.date(),count=17)[0]
        end_date = get_trade_days(end_date=context.current_dt.date(),count=17)[-2]
        data_open,data_close = get_all_sw_data(start_date,end_date)
        m0,m1 = ret(data_open,data_close)
        #不要将打分函数命名为score，该名字被聚宽内置了
        score = mgold(m0,m1)
        buy_list = list(score.T.sort_values(by=score.T.columns[0],axis=0,ascending=False)[:5].index)
        sell_list = list(score.T.sort_values(by=score.T.columns[0],axis=0,ascending=False)[-5:].index)
        print(score.T.sort_values(by=score.T.columns[0],axis=0,ascending=False))
        # #首先清仓
        # #这里必须使用循环来取出股票名称！
        # for stock in context.portfolio.positions:
        #     order_target_value(stock,0)
        # print('今天是月初第一个交易日，需要调仓')
        # for code in code_list:
        #     order_target_value(code,context.portfolio.available_cash)

#用来删除空值
def del_nul(data):
    for name,num in dict(data.apply(lambda x:sum(x.isnull()))).items():
        if num!=0:
            data = data.drop(name,axis=1)
            print(name+'有'+str(num)+'条缺失值！已删除该列！')
    return data
    
def get_all_sw_data(start_date,end_date):
    '''
    start_date：起始日期
    end_date：截止日期
    return：一个日度收盘价数据表、一个日度开盘价数据表，表头是所有的编号
    '''
    code_list = list(get_industries('sw_l1',date=end_date).index)
    data_open = pd.DataFrame()
    data_close = pd.DataFrame()
    for code in code_list:     
        q1 = query(
            finance.SW1_DAILY_PRICE.name,
            finance.SW1_DAILY_PRICE.open,
            finance.SW1_DAILY_PRICE.date
            ).filter(
            finance.SW1_DAILY_PRICE.code==code,
            finance.SW1_DAILY_PRICE.date>=start_date,
            finance.SW1_DAILY_PRICE.date<=end_date
            )
        q2 = query(
            finance.SW1_DAILY_PRICE.name,
            finance.SW1_DAILY_PRICE.close,
            finance.SW1_DAILY_PRICE.date
            ).filter(
            finance.SW1_DAILY_PRICE.code==code,
            finance.SW1_DAILY_PRICE.date>=start_date,
            finance.SW1_DAILY_PRICE.date<=end_date
            )
        code_data_open = finance.run_query(q1)
        code_data_close = finance.run_query(q2)
        data_open[code] = code_data_open.open
        data_close[code] = code_data_close.close
    data_open.index = code_data_open.date
    data_close.index = code_data_close.date
    #去除有空值的列，因为有些指数是2021年才有的
    data_open = del_nul(data_open)
    data_close = del_nul(data_close)
    return data_open,data_close

def ret(data_open,data_close):
    '''
    data_open：开盘价日度数据，index为datetime
    data_close：收盘价日读数据，index为datetime
    return：日内涨跌幅因子M0和隔夜涨跌幅因子M1
    '''
    ret_in = pd.DataFrame()
    ret_over = pd.DataFrame()
    for name in list(data_open.columns):
        #这个列表用于储存单只指数的每天开盘与收盘价
        name_open_close = pd.DataFrame()
        name_open_close['open'] = data_open[name]
        name_open_close['close'] = data_close[name]
        name_open_close.index = data_open.index
        #日内收益率为今收/今开-1
        name_open_close['ret_in'] = name_open_close['close']/name_open_close['open']-1
        #隔夜收益率为今开/昨收-1
        name_open_close['ret_over'] = name_open_close['open']/name_open_close['close'].shift(1)-1
        ret_in[name] = name_open_close['ret_in']
        ret_over[name] = name_open_close['ret_over']
    #由于隔夜收益率计算产生空值，方便起见我们直接删去第一行
    ret_in = ret_in.iloc[1:,:]
    ret_over = ret_over.dropna()
    #前十五日内收益率加总
    m0 = ret_in.rolling(15).sum()
    #前十五日隔夜收益率加总
    m1 = ret_over.rolling(15).sum()
    #同理删除空值所在行
    m0 = m0.dropna()
    m1 = m1.dropna()
    return m0,m1

def mgold(m0,m1):
    '''
    return：每只行业指数的打分
    '''
    score = pd.DataFrame(index=m0.T.index)
    for date in m0.T.columns:
        #m0从低到高打1~N分
        m0_sort = pd.DataFrame(m0.T[date].sort_values(ascending=True))
        m0_sort['score0'] =  range(1,len(m0_sort)+1)
        #m1从高到低打1~N分
        m1_sort = pd.DataFrame(m1.T[date].sort_values(ascending=False))
        m1_sort['score1'] =  range(1,len(m1_sort)+1)
        #根据行索引匹配
        sort = pd.concat([m0_sort,m1_sort],axis=1,join='inner')
        sort['score'] = sort['score0']+sort['score1']
        #让sort只保留score列
        sort = sort.drop([date,'score0','score1'],axis=1)
        score = pd.concat([score,sort],axis=1,join='inner')
    score = score.T
    #最后将列索引恢复成指数名，行索引恢复成日期
    score.index = m0.index
    return score

#收盘后
def after_trading_end(context):
    pass
