'''
参考聚宽高频因子挖掘大赛比赛专用模板
'''
# 导入函数库
from jqdata import *
import numpy as np
import pandas as pd
import jqfactor
from jqfactor import get_factor_values

##################################### 初始化设置 ###############################################
# 初始化函数，设定基准等等
def initialize(context):
    # 设定中证500作为基准
    g.benchmark = '000905.XSHG'
    set_benchmark(g.benchmark)
    set_option('use_real_price', True)
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5), type='stock')
    # 滑点
    set_slippage(PriceRelatedSlippage(0.00246),type='stock')

    # 初始化因子设置
    factor_analysis_initialize(context)
    # 定义股票池
    set_stockpool(context)

    # 策略运行了几天
    g.days = 0
    # 每次购买股票池10%的股票
    g.buy_num = int(len(g.stock_pool)*0.1)

    # 聚宽因子库因子名称
    g.factor_name = 'book_to_price_ratio'
    # 调仓周期
    g.period = 21
    # 买1分位还是10分位，等于1时买一分位因子值小的，不为1，买十分位因子值大的
    g.quantile = 10
    # 几点卖出
    g.sell_time = '14:55'
    # 几点买入
    g.buy_time = '09:31'

    # 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
    # 其实用handle_data也是一样的
    run_daily(set_stockpool, time='09:10', reference_security='000300.XSHG')
    run_daily(before_market_open, time='09:10', reference_security='000300.XSHG')
    run_daily(sell, time=g.sell_time, reference_security='000300.XSHG')
    run_daily(buy, time=g.buy_time, reference_security='000300.XSHG')
    run_daily(after_market_close, time='15:30', reference_security='000300.XSHG')

# 定义股票池
def set_stockpool(context):
    # 获取开盘前一天的中证500股票池
    stocks = get_index_stocks(g.benchmark, context.previous_date)
    # 获取股票池的个股开盘前一天的停牌情况（1为停牌），并变为单独的一列array
    paused_series = get_price(stocks,end_date=context.current_dt,count=1,fields='paused')['paused'].iloc[0]
    # 去除停牌的股票
    g.stock_pool =  paused_series[paused_series==0].index.tolist()

# 定义需要用到的全局变量
def factor_analysis_initialize(context):
    # g.weight_method为加权方式, "avg"按平均加权
    g.weight_method = "avg"
    weight_method_model = {"avg": "平均加权"}
    # g.sell为卖出股票权重列表
    g.sell = pd.Series(dtype=float)
    # g.buy为买入股票权重列表
    g.buy = pd.Series(dtype=float)
    # g.ind为行业分类
    g.ind = 'jq_l1'
    # g.d为获取昨天的时间点
    g.d = context.previous_date

####################################### 定义因子相关 ###############################################
# 定义因子
def calc_factor(context):
    '''
    用户自定义因子，要求返回一个 Series，index为股票code，value为因子值
    我们会买入「因子值最小」的20只，如果想买入「因子值最大」的20只股票，只需结果「乘以-1.0」即可
    自定义因子，ascending=True，far没有乘-1，买入的是far[:3]，因子数最小的
    自定义因子，ascending=True，far乘-1，买入的是far[-3:]，因子数最大的
    '''
    # 获取股票池和前一个交易日（不是前一天）
    stocks = g.stock_pool
    check_date = context.previous_date
    # count往回数包括end_date
    far = get_factor_values(stocks, factors=[g.factor_name], end_date=check_date, count=1)[g.factor_name].T
    # 特意转成Series，因为行业中性化必须用Series
    far = pd.Series(far[str(check_date)]).sort_values(ascending=True)
    # 中性化等数据处理模块
    # 中位数去极值，超过中位数加减3倍标准差之外的数值被替换成边界值
    far = jqfactor.winsorize_med(far, scale=3, inclusive=True, inf2nan=True)
    # 行业市值中性化
    far = jqfactor.neutralize(far, how=['market_cap'], date=g.d)
    # zscore标准化，所有的inf2nan表示无穷数表示为nan
    far = jqfactor.standardlize(far, inf2nan=True)
    # 去除nan值
    far = far.dropna()
    # 打印股票观察是否有误
    print(far[:3])
    print(far[-3:])
    print('='*50)

    # 买一分位，因子值小的
    if g.quantile == 1:
        far = far
    # 买十分位，因子值小的
    else:
        far = far * -1.0
    return far

####################################### 盘前盘后运行函数 ###############################################
# 开盘前运行函数
def before_market_open(context):
    pass

# 收盘后运行函数
def after_market_close(context):
    # 计算是否快调仓了
    print('period {0}, buy_num {1}'.format(g.period, g.quantile))
    print('='*50)

##################################### 交易部分 ###############################################

# 对因子进行分析计算出每日买入或卖出的股票
def fac(context):
    # 获取因子值
    far = calc_factor(context)
    # 买入股票池
    buy = far.index.tolist()[:g.buy_num]
    # 买卖股票权重
    if (g.weight_method == "avg") and (len(buy) > 0):
        buy_weight = pd.Series(1. / len(buy), index=buy)
    else:
    # 暂时先按照等权重来
        print('invalid weight_method')
    return buy_weight

# 买入股票
def buy(context):
    if g.days == 0:
        # 计算买入卖出的股票和权重
        try:
            factor_analysis_initialize(context)
            buy_weight = fac(context)
        except ValueError:
            if "Bin edges must be unique" in str(e):
                log.error("计算因子值过程出错！")
            else:
                raise
        long_cash = context.portfolio.total_value
        if buy_weight is not None:
            for s in buy_weight.index:
                order_target_value(s, buy_weight.loc[s]*long_cash)
    # 持仓天数加1
    g.days += 1

# 卖出股票
def sell(context):
    print("持仓了{0}天了".format(g.days))
    # 符合持仓天数后卖出
    if g.days%g.period == 0:
        for s in context.portfolio.positions.keys():
            order_target_value(s, 0)
        # 卖出后置持仓天数为0
        g.days = 0
