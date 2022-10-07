#双均线策略
#2020-01-01到2022-01-01,￥2000000,每天回测
from jqdata import *
'''
================================================================================
总体回测前
================================================================================
'''
#总体回测前要做的事情
def initialize(context):
    #设定沪深300作为基准
    set_benchmark('601838.XSHG')
    set_params()     #运行设置参数的函数
    set_backtrade()  #运行设置回测条件的函数
    
#设置策略参数
def set_params():
    #定义全局函数
    #单股票策略以成都银行为标的股票
    g.security = ["601838.XSHG"]

#设置回测条件
def set_backtrade():
    set_option('use_real_price',True) #用真实价格交易
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')


'''
================================================================================
每天开盘前
================================================================================
'''
#每天开盘前要做的事情
def before_trading_start(context):
    set_fee() 

# 根据不同的时间段设置滑点与手续费
def set_fee():
    #设置固定的交易滑点
    set_slippage(FixedSlippage(0.01))
    # 股票类每笔交易时的手续费
    set_order_cost(
        OrderCost(open_tax=0,
        close_tax=0.001,
        open_commission=0.0003,   #买入时佣金万分之三
        close_commission=0.0003,  #卖出时佣金万分之三加千分之一印花税
        close_today_commission=0,
        min_commission=5          #每笔交易佣金最低扣5块钱
            ), 
        type='stock'
        )

'''
================================================================================
每天交易时
================================================================================
'''
def handle_data(context,data):
    toSell = signal_sell(context)
    toBuy = signal_buy(context)
    #执行卖出操作以腾出资金
    if toSell[0]==1:
        order_target_value(g.security[0],0)
    # 执行买入操作(满仓)
    if toBuy[0]==1:
        order_target_value(g.security[0],context.portfolio.available_cash)  
    if not (1 in toBuy) or (1 in toSell):
        log.info("今日无操作")

#获得卖出信号
def signal_sell(context):
    sell = [0]
    #算出今天和昨天的两个指数移动均线的值
    #我们这里假设长线是20天，短线是5天
    ma_long_pre,ma_long_now = get_MA(g.security[0],20)
    ma_short_pre,ma_short_now = get_MA(g.security[0],5)
    #如果短均线从上往下穿越长均线，且可卖出的仓位大于0，标记卖出
    if ma_short_now < ma_long_now and ma_short_pre > ma_long_pre and context.portfolio.positions[g.security[0]].closeable_amount > 0:
        sell[0] = 1
        log.info('完成卖出')
    return sell

#获得买入信号
def signal_buy(context):
    buy = [0]
    #算出今天和昨天的两个指数移动均线的值
    #我们这里假设长线是20天，短线是5天
    (ma_long_pre,ma_long_now) = get_MA(g.security[0],60)
    (ma_short_pre,ma_short_now) = get_MA(g.security[0],1)
    #如果短均线从下往上穿越长均线，且为空仓状态时，标记买入
    if ma_short_now > ma_long_now and ma_short_pre < ma_long_pre and context.portfolio.positions[g.security[0]].closeable_amount == 0:
        buy[0] = 1
        log.info('完成买入')
    return buy

# 计算移动平均线数据
def get_MA(security_code,days):
    #获得前days+1天的数据
    a = attribute_history(security_code,days+1,'1d',['close'])
    #定义一个局部变量sum，用于求和
    sum_now = 0
    sum_pre = 0
    #对前days天的收盘价进行求和
    for i in range(1,days+1):
        sum_now += a['close'][-i]
    for j in range(2,days+2):
        sum_pre += a['close'][-j]    
    #求和之后除以天数就可以的得到算术平均值
    ma_now = sum_now/days
    ma_pre = sum_pre/days
    return ma_pre,ma_now

'''
================================================================================
每天收盘后
================================================================================
'''
#每日收盘后要做的事情（本策略中不需要）
def after_trading_end(context):
    pass


