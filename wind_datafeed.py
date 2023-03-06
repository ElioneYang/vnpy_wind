import time
from datetime import timedelta, datetime
from json import encoder
from typing import List, Optional
from pytz import timezone
from math import isnan
from pandas import DataFrame
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest,TickData

from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.database import BaseDatabase, get_database

from vnpy_spreadtrading.base import LegData, SpreadData
from vnpy.trader.utility import extract_vt_symbol

from WindPy import w


CHINA_TZ = timezone("Asia/Shanghai")

EXCHANGE_MAP = {
    Exchange.SSE: "SH",
    Exchange.SZSE: "SZ",
    Exchange.CFFEX: "CFE",
    Exchange.SHFE: "SHF",
    Exchange.CZCE: "CZC",
    Exchange.DCE: "DCE",
    Exchange.INE: "INE",
    Exchange.GFEX: "GFE",
}

WINDEXCHANGE_MAP = {v:k for k,v in EXCHANGE_MAP.items()}

INTERVAL_MAP = {
    Interval.MINUTE: "1",
    Interval.HOUR: "60"
}


class WindDatafeed(BaseDatafeed):
    """万得数据服务接口"""

    def init(self) -> bool:
        """初始化"""
        if w.isconnected():
            return True

        data: w.WindData = w.start()
        if data.ErrorCode:
            return False

        return True
    
    @property
    def inited(self):
        return self.init()


    def query_data_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """查询数据"""
        if not w.isconnected():
            self.init()

        if req.interval == Interval.TICK:
            return self.query_tick_history(req)

        elif req.interval == Interval.DAILY:
            return self.query_daily_bar_history(req)        
        else:
            return self.query_intraday_bar_history(req)


    def query_intraday_bar_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """查询日内K线数据"""
        # 参数转换
        wind_exchange: str = EXCHANGE_MAP[req.exchange]
        wind_symbol: str = f"{req.symbol.upper()}.{wind_exchange}"

        fields: List[str] = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amt",
            "oi"
        ]

        wind_interval: str = INTERVAL_MAP[req.interval]
        options: str = f"BarSize={wind_interval};Fill=Previous"

        
        # 发起查询
        error, df = w.wsi(
            codes=wind_symbol,
            fields=fields,
            beginTime=req.start,
            endTime=req.end+timedelta(1),
            options=options,
            usedf=True
        )

        # 检查错误
        if error:
            return []
        

        # 解析数据
        bars: List[BarData] = []
        for tp in df.itertuples():
            if isinstance(tp.Index,str):continue
            dt: datetime = tp.Index.to_pydatetime()

            # 检查是否有持仓量字段
            if isnan(tp.position):
                open_interest: int = 0
            else:
                open_interest: int = tp.position

            bar: BarData = BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                interval=req.interval,
                datetime=CHINA_TZ.localize(dt),
                open_price=tp.open,
                high_price=tp.high,
                low_price=tp.low,
                close_price=tp.close,
                volume=tp.volume,
                turnover=tp.amount,
                open_interest=open_interest,
                gateway_name="WIND"
            )
            bars.append(bar)

        return bars

    def query_daily_bar_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """查询日K线数据"""
        # 参数转换
        wind_exchange: str = EXCHANGE_MAP[req.exchange]
        wind_symbol: str = f"{req.symbol.upper()}.{wind_exchange}"

        fields: List[str] = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amt",
            "oi"
        ]

        # 发起查询
        error, df = w.wsd(
            codes=wind_symbol,
            fields=fields,
            beginTime=req.start,
            endTime=req.end+timedelta(1),
            options="Fill=Previous",
            usedf=True
        )

        # 检查错误
        if error:
            return []

        # 解析数据
        bars: List[BarData] = []
        for tp in df.itertuples():
            dt: datetime = datetime.combine(tp.Index, datetime.min.time())

            # 检查是否有持仓量字段
            if isnan(tp.OI):
                open_interest: int = 0
            else:
                open_interest: int = tp.OI

            bar: BarData = BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                interval=req.interval,
                datetime=CHINA_TZ.localize(dt),
                open_price=tp.OPEN,
                high_price=tp.HIGH,
                low_price=tp.LOW,
                close_price=tp.CLOSE,
                volume=tp.VOLUME,
                turnover=tp.AMT,
                open_interest=open_interest,
                gateway_name="WIND"
            )
            bars.append(bar)

        return bars
    
    def query_bar_history(self, req: HistoryRequest):
        return self.query_data_history(req)

    def query_tick_history(self, req: HistoryRequest) -> Optional[List[TickData]]:
        """"""
        if not w.isconnected():self.init()
            
        if req.exchange == Exchange.LOCAL:return []
        wind_exchange: str  = EXCHANGE_MAP[req.exchange]
        wind_symbol: str    = f"{req.symbol.upper()}.{wind_exchange}"
        fields = [
            'open', 
            'high', 
            'low', 
            'last', 
            'pre_close', 
            'volume', 
            'amt', 
            "oi",
            'limit_up', 
            'limit_down', 
            'bid1', 
            'bid2', 
            'bid3', 
            'bid4', 
            'bid5', 
            'ask1', 
            'ask2', 
            'ask3', 
            'ask4', 
            'ask5', 
            'bsize1', 
            'bsize2', 
            'bsize3', 
            'bsize4', 
            'bsize5', 
            'asize1', 
            'asize2', 
            'asize3', 
            'asize4', 
            'asize5'
        ]

        # 发起查询 # tick # 国内六大交易所（上海交易所、深圳交易所、中金所、郑商所、上金所、上期所、大商所）
        error, df = w.wst(
            codes=wind_symbol,
            fields=fields,
            beginTime=req.start,
            endTime=req.end+timedelta(1),
            usedf=True
        )

        # 检查错误
        if error:return []
        
        df.columns =[
            "open",
            "high",
            "low",
            "last",
            "prev_close",
            "volume",
            "total_turnover",
            "open_interest",
            "limit_up",
            "limit_down",
            "b1",
            "b2",
            "b3",
            "b4",
            "b5",
            "a1",
            "a2",
            "a3",
            "a4",
            "a5",
            "b1_v",
            "b2_v",
            "b3_v",
            "b4_v",
            "b5_v",
            "a1_v",
            "a2_v",
            "a3_v",
            "a4_v",
            "a5_v",
        ]

        df = df.fillna(0).astype(float) 

        data: List[TickData] = []
        if df is not None:
            for _, row in df.iterrows():
                dt = row.name.to_pydatetime()
                dt = CHINA_TZ.localize(dt)

                tick = TickData(
                    symbol=req.symbol,
                    exchange=req.exchange,
                    datetime=dt,
                    open_price=row["open"],
                    high_price=row["high"],
                    low_price=row["low"],
                    pre_close=row["prev_close"],
                    last_price=row["last"],
                    volume=row["volume"],
                    turnover=row["total_turnover"],
                    open_interest=row["open_interest"],
                    limit_up=row["limit_up"],
                    limit_down=row["limit_down"],
                    bid_price_1=row["b1"],
                    bid_price_2=row["b2"],
                    bid_price_3=row["b3"],
                    bid_price_4=row["b4"],
                    bid_price_5=row["b5"],
                    ask_price_1=row["a1"],
                    ask_price_2=row["a2"],
                    ask_price_3=row["a3"],
                    ask_price_4=row["a4"],
                    ask_price_5=row["a5"],
                    bid_volume_1=row["b1_v"],
                    bid_volume_2=row["b2_v"],
                    bid_volume_3=row["b3_v"],
                    bid_volume_4=row["b4_v"],
                    bid_volume_5=row["b5_v"],
                    ask_volume_1=row["a1_v"],
                    ask_volume_2=row["a2_v"],
                    ask_volume_3=row["a3_v"],
                    ask_volume_4=row["a4_v"],
                    ask_volume_5=row["a5_v"],
                    gateway_name="WIND"
                )
                data.append(tick)
        return data



    def query_bars_once(self, vt_symbols:list, days: int, interval: Interval) -> Optional[List[BarData]]:
        """ 一次批量读取bar data"""
        # 用于portfolio/script
        if not w.isconnected():
            self.init()

        _dt  = datetime.now()
        _dts = (_dt-timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        _dte = _dt.strftime("%Y-%m-%d %H:%M:%S")

        wind_symbol_map = {}
        for vt_symbol in vt_symbols:
            symbol,exchange     = vt_symbol.split(".")
            exchange            = Exchange(exchange)
            wind_exchange       = EXCHANGE_MAP[exchange]
            wind_symbol         = f"{symbol.upper()}.{wind_exchange}"
            wind_symbol_map[wind_symbol] = [vt_symbol,symbol,exchange]
        
        # wsi单次提取股票数有限制
        # wsi支持多品种多指标,单次提取一个品种支持近三年数据，若单次提多个品种,则品种数*天数≤100。

        symbols_all     = list(wind_symbol_map.keys())
        length          = len(symbols_all)
        data = DataFrame()
        n    = int(100/days)
        i    = 0
        while i<length:
            i_ = min(i+n,length)
            errorcode,data_i  = w.wsi(",".join(symbols_all[i:i_]), "open,high,low,close,volume", _dts, _dte, "BarSize=1;Fill=Previous",usedf=True)
            error_count = 0
            while errorcode!=0:
                error_count +=1
                time.sleep(3)
                errorcode,data_i  = w.wsi(",".join(symbols_all[i:i_]), "open,high,low,close,volume", _dts, _dte, "BarSize=1;Fill=Previous",usedf=True)
                if error_count>3:break
            data = data.append(data_i)
            i = i_

        bars_all = []
        dt_all = data.index.drop_duplicates().sort_values()
        for tt in dt_all:
            bars = {}
            df   = data.loc[tt]
            # if len(df)!=length:continue
            for t,row in df.iterrows():
                t = t.to_pydatetime()
                t = CHINA_TZ.localize(t)
                bar = BarData(
                    symbol=wind_symbol_map[row["windcode"]][1],
                    exchange=wind_symbol_map[row["windcode"]][2],
                    datetime=t,
                    open_price=row['open'],
                    high_price=row['high'],
                    low_price=row['low'],
                    close_price=row['close'],
                    volume=row['volume'],
                    interval=Interval.MINUTE,
                    gateway_name="WIND"
                )
                bars[bar.vt_symbol] = bar
            bars_all.append(bars)
        return bars_all


    def get_index_component(self,index_symbol,date=""):
        """获取股指的成分信息"""
        if not w.isconnected():
            self.init()

        # index_symbol="000300.SH",date="2022-10-10"
        cs    = 300 if index_symbol in ["IF","IH"] else 200
        trans = {
            "IF":"000300.SH",
            "IC":"000905.SH",
            "IM":"000852.SH",
            "IH":"000016.SH",
        }
        
        if index_symbol in trans:index_symbol = trans[index_symbol]
        if date=="":date = datetime.now().strftime("%Y-%m-%d")

        _,df = w.wset("indexconstituent",f"date={date};windcode={index_symbol}",usedf=True)
        _,hq = w.wsq([index_symbol]+df["wind_code"].to_list(), "rt_latest,rt_bid1,rt_ask1",usedf=True)
        
        df.set_index("wind_code",inplace=True)
        df[hq.columns] = hq
        df["vt_symbol"]   = df.index.map(lambda x: x.replace("SH","SSE").replace("SZ","SZSE"))
        df["weight"]      = df["i_weight"]/100
        df["label"]       = index_symbol
        df["i_divisor"]   = hq.loc[index_symbol]["RT_LATEST"]*df["weight"]/df["RT_LATEST"]
        df["i_share"]     = cs * df["i_divisor"]
        return df.set_index("vt_symbol").to_dict(orient="index")
        # '000001.SZSE': {'date': Timestamp('2022-10-10 00:00:00'),
        #   'sec_name': '平安银行',
        #   'i_weight': 0.6386,
        #   'industry': '金融',
        #   'RT_LATEST': 11.48,
        #   'RT_BID1': 11.48,
        #   'RT_ASK1': 11.49,
        #   'weight': 0.006385999999999999,
        #   'label': '000300.SH',
        #   'i_divisor': 2.078960074912892},

