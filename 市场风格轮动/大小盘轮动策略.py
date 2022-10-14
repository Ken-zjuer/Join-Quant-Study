#导入函数库
from jqdata import *
from jqfactor import *
import numpy as np
import pandas as pd
import datetime
from sklearn.linear_model import LogisticRegression

#初始化函数，设定基准等等
def initialize(context):
    set_benchmark('000300.XSHG')
    set_option('use_real_price', True)
    #手续费设定
    set_slippage(PriceRelatedSlippage(0.00246),type='stock')  
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    #策略运行了几天
    g.days = 0
    #滚动窗口
    g.windows = 18
    #调仓默认1个月一次，跟预测频率一样
    #股票池
    g.security = ['000300.XSHG','000905.XSHG']
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
        #首先清仓
        #这里必须使用循环来取出股票名称！
        for stock in context.portfolio.positions:
            order_target_value(stock,0)
        print('今天是月初第一个交易日，需要调仓')
        X = get_Xdata(context.current_dt.date(),context)
        Y = get_Ydata(context.current_dt.date(),context)
        x_old = X.iloc[:g.windows]
        x_new = np.array(X.iloc[-1]).reshape(1,-1)
        y_old = Y.label.iloc[1:g.windows+1]
        fit = LogisticRegression().fit(x_old,y_old)
        signal = fit.predict(x_new)
        print(signal)
        #当预测为0时，说明大盘股战胜小盘股，全仓买入大盘股
        if signal==0:
            order_target_value(g.security[0],context.portfolio.available_cash)
        #反之，代表小盘股为市场风格
        else:
            order_target_value(g.security[1],context.portfolio.available_cash)
    
            
#对于日度数据：获取前22个月（包括该月，上个月用于预测，本月初数据没用）的数据
def get_Xdata(current_dt,context):
    '''
    current_dt：当下日期，datetime格式
    返回：数据表X，包含三个特征，共21行，前20行拟合，最后1行预测
    '''
    #获得22个月前的该月的最初交易日（不包括本月，即最前的那个月）
    start_day_daily = g.day_lst[g.day_lst.index(context.current_dt.date())-g.windows-1]
    start_day_monthly = g.day_lst[g.day_lst.index(context.current_dt.date())-g.windows-2]
    end_day = context.current_dt.date()
    #创建空数据表用于储存
    data = pd.DataFrame()
    #人民币兑换美元的现汇买入价(日级)
    q1 = query(
        macro.MAC_RMB_EXCHANGE_RATE.day,
        macro.MAC_RMB_EXCHANGE_RATE.cash_buy_rate   
        ).filter(
        macro.MAC_RMB_EXCHANGE_RATE.currency_id=='40',
        start_day_daily<=macro.MAC_RMB_EXCHANGE_RATE.day
        ).filter(
        macro.MAC_RMB_EXCHANGE_RATE.day<=end_day
        )
    fx = macro.run_query(q1).sort_values(by='day')
    #取出年份和月份，并转换成datetime格式
    fx['year_month'] = fx['day'].apply(lambda x:x[0:7])
    fx['yyyymm'] = fx['year_month'].apply(lambda x:datetime.datetime.strptime(x,"%Y-%m"))
    #选出每月第一条数据作为本月数据
    #最后这里产生21条数据（20个用于拟合，上个月的用于预测）
    data['fx'] = np.array(fx.groupby('yyyymm').head(1).cash_buy_rate[:-1])
    data['yyyymm01'] = np.array(fx['yyyymm'].unique()[:-1])
    
    #M2同比增长率月度数据
    q2 = query(
        macro.MAC_MONEY_SUPPLY_MONTH.stat_month,
        macro.MAC_MONEY_SUPPLY_MONTH.m2_yoy   
        ).filter(
        #注意这里的时间点作差问题
        start_day_monthly<=macro.MAC_MONEY_SUPPLY_MONTH.stat_month
        ).filter(
        macro.MAC_MONEY_SUPPLY_MONTH.stat_month<=end_day
        )
    m2 = macro.run_query(q2).sort_values(by='stat_month')
    data['m2_yoy'] = np.array(m2['m2_yoy'][:-1])
    
    #CPI同比增速月度数据
    q3 = query(
        macro.MAC_CPI_MONTH.stat_month,
        macro.MAC_CPI_MONTH.yoy 
        ).filter(
        start_day_monthly<=macro.MAC_CPI_MONTH.stat_month,
        macro.MAC_CPI_MONTH.area_code=='701001'
        ).filter(
        macro.MAC_CPI_MONTH.stat_month<=end_day
        )
    cpi = macro.run_query(q3).sort_values(by='stat_month')
    data['cpi_yoy'] = np.array(cpi['yoy'][:-1])
    
    #对每一列标准化
    X = pd.DataFrame()
    for col in ['fx','m2_yoy','cpi_yoy']:
        s = standardlize(data[col],inf2nan=True,axis=0)
        X[col] = np.array(s)
    return X

def get_Ydata(current_dt,context):
    '''
    current_dt：当下日期，datetime格式
    返回：数据表Y，1列
    '''
    #日度数据注意时间差
    start_day = g.day_lst[g.day_lst.index(context.current_dt.date())-g.windows-2]
    end_day = context.current_dt.date()   
    #创建空数据表用于储存
    data = pd.DataFrame()
    #沪深300
    hs300 = get_price('000300.XSHG',start_date=start_day,end_date=end_day,frequency='1d',fields='close',fq='pre')
    hs300['year_month'] = np.array((pd.DataFrame([*map(str,hs300.index)])[0]).apply(lambda x:x[0:7]))
    hs300['yyyymm'] = hs300['year_month'].apply(lambda x:datetime.datetime.strptime(x,"%Y-%m"))
    data['yyyymm'] = np.array(np.array(hs300['yyyymm'].unique())[:-1])
    data['hs300'] = np.array(hs300.groupby('yyyymm').head(1).close[:-1])
    #中证500
    zz500 = get_price('000905.XSHG',start_date=start_day,end_date=end_day,frequency='1d',fields='close',fq='pre')
    zz500['year_month'] = np.array((pd.DataFrame([*map(str,zz500.index)])[0]).apply(lambda x:x[0:7]))
    zz500['yyyymm'] = zz500['year_month'].apply(lambda x:datetime.datetime.strptime(x,"%Y-%m"))
    data['zz500'] = np.array(zz500.groupby('yyyymm').head(1).close[:-1])
    #计算收益率
    Y = pd.DataFrame()
    for col in ['hs300','zz500']:
        new_col = col+'ret'
        data[new_col] = data[col]/data[col].shift(1)-1
    #再把最前面那个月的nan去掉
    data = data.dropna()
    #这里以中证500和沪深300为例，当中证500收益率大于沪深300记为1，反之0
    Y['label'] = np.array([*map(int,data.hs300ret<data.zz500ret)])
    return Y
    
#收盘后
def after_trading_end(context):
    pass
