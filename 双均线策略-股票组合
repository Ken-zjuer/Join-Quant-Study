#双均线策略
#2020-01-01到2022-01-01,￥2000000,每天回测

def initialize(context):
    set_benchmark('000300.XSHG')
    set_params() 
    set_backtrade() 
    
def set_params():
    g.tc = 15  #调仓频率
    g.N = 7    #持仓数目
    #选定股票池
    g.security = ["002129.XSHE","000738.XSHE","300357.XSHE",'002416.XSHE','002409.XSHE','688308.XSHG','300450.XSHE']

def set_backtrade():
    set_option('use_real_price', True)

#每轮回测前
def before_trading_start(context):
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5), type='stock')
    set_slippage(FixedSlippage(0.01)) 
    
#开始回测
def handle_data(context,data):
    #将总价值等分为g.N份，为每只股票配资
    capital_unit = context.portfolio.total_value/g.N
    toSell = signal_stock_sell(context,data)
    toBuy = signal_stock_buy(context,data)
    #执行卖出操作以腾出资金
    for i in range(len(g.security)):
        if toSell[i]==1:
            order_target_value(g.security[i],0)
    #执行买入操作
    for i in range(len(g.security)):
        if toBuy[i]==1:
            order_target_value(g.security[i],capital_unit)  
    if not (1 in toBuy) or (1 in toSell):
        log.info("今日无操作")

#获得卖出信号
def signal_stock_sell(context,data):
    sell = [0]*len(g.security)
    for i in range(len(g.security)):
    #算出21天和5天的两个指数移动均线的值
        (ema_long_pre,ema_long_now) = get_EMA(g.security[i],21,data)
        (ema_short_pre,ema_short_now) = get_EMA(g.security[i],5,data)
        #死叉，并且可卖出的仓位大于0
        if ema_short_now < ema_long_now and ema_short_pre > ema_long_pre and context.portfolio.positions[g.security[i]].closeable_amount > 0:
            sell[i]=1
    return sell
        
#获得买入信号
def signal_stock_buy(context,data):
    buy = [0]*len(g.security)
    for i in range(len(g.security)):
        (ema_long_pre,ema_long_now) = get_EMA(g.security[i],21,data)
        (ema_short_pre,ema_short_now) = get_EMA(g.security[i],5,data)
        #金叉，且没有仓位的时候
        if ema_short_now > ema_long_now and ema_short_pre < ema_long_pre and context.portfolio.positions[g.security[i]].closeable_amount == 0 :
            buy[i]=1
    return buy
    
#计算移动平均线数据
def get_MA(security_code,days):
    #获得前days天的数据
    a = attribute_history(security_code, days, '1d', ['close'])
    #定义一个局部变量sum，用于求和
    sum = 0
    #对前days天的收盘价进行求和
    for i in range(1,days+1):
        sum += a['close'][-i]
    #求和之后除以天数就可以的得到算术平均值啦
    return sum/days

#计算指数移动平均线数据
def get_EMA(security_code,days,data):
    #如果只有一天的话,前一天的收盘价就是移动平均
    if days==1:
    #一个作为上一期的移动平均值，后一个作为当期的移动平均值
        t = attribute_history(security_code,2,'1d',['close'])
        return t['close'][-2],t['close'][-1]
    else:
    #如果全局变量g.EMAs不存在的话，创建一个字典类型的变量，用来记录已经计算出来的EMA值
        if 'EMAs' not in dir(g):
            g.EMAs = {}
        #字典的关键字用股票编码和天数连接起来唯一确定
        key = "%s%d" %(security_code,days)
        #如果关键字存在，说明之前已经计算过EMA了，直接迭代即可
        if key in g.EMAs:
            #计算alpha值
            alpha = (days-1.0)/(days+1.0)
            #获得前一天的EMA（这个是保存下来的了）
            EMA_pre = g.EMAs[key]
            #EMA迭代计算
            EMA_now = EMA_pre*alpha + data[security_code].close*(1.0-alpha)
            #写入新的EMA值
            g.EMAs[key]=EMA_now
            #给用户返回昨天和今天的两个EMA值
            return (EMA_pre,EMA_now)
        #如果关键字不存在，说明之前没有计算过这个EMA，因此要初始化
        else:
            #获得days天的移动平均
            ma = get_MA(security_code,days) 
            #如果滑动平均存在（不返回NaN）的话，那么我们已经有足够数据可以对这个EMA初始化了
            if not(isnan(ma)):
                g.EMAs[key]=ma
                #因为刚刚初始化，所以前一期的EMA还不存在
                return (float("nan"),ma)
            else:
                #移动平均数据不足days天，只好返回NaN值
                return (float("nan"),float("nan"))

# 每日收盘后要做的事情（本策略中不需要）
def after_trading_end(context):
    pass


