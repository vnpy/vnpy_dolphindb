""""""
from typing import List
import pandas as pd
from datetime import datetime
import numpy as np

import dolphindb as ddb

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    DB_TZ,
    convert_tz
)

# dolphindb脚本，用于在dolphindb中判断是否已存在目标数据库，并在未存在时新建
script = """
    barPath = "dfs://vnpy_bar"
    if((existsDatabase(barPath).not())){
        bar_exchange = database(, VALUE, ["vnpy"])
        bar_symbol = database(, VALUE, ["vnpy"])
        bar_interval = database(, VALUE, ["vnpy"])
        db_bar = database(barPath, COMPO, [bar_exchange, bar_symbol, bar_interval], engine=`TSDB)
        bar_t = table(100:100,
                     `symbol`exchange`datetime`interval`volume`turnover`open_interest`open_price`high_price`low_price`close_price,
                     [SYMBOL,SYMBOL,NANOTIMESTAMP,SYMBOL,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE])
        db_bar.createPartitionedTable(bar_t,
                                      `bar,
                                      partitionColumns=["exchange", "symbol", "interval"],
                                      sortColumns=["symbol","datetime"],
                                      keepDuplicates=LAST)
    }

    tickPath = "dfs://vnpy_tick"
    if((existsDatabase(tickPath).not())){
        tick_exchange = database(, VALUE, ["vnpy"])
        tick_symbol = database(, VALUE, ["vnpy"])
        db_tick = database(tickPath, COMPO, [tick_exchange, tick_symbol], engine=`TSDB)
        tick_t = table(100:100,
                       `symbol`exchange`datetime`name`volume`turnover`open_interest`last_price`last_volume`limit_up`limit_down\
                       `open_price`high_price`low_price`pre_close`localtime\
                       `bid_price_1`bid_price_2`bid_price_3`bid_price_4`bid_price_5\
                       `ask_price_1`ask_price_2`ask_price_3`ask_price_4`ask_price_5\
                       `bid_volume_1`bid_volume_2`bid_volume_3`bid_volume_4`bid_volume_5\
                       `ask_volume_1`ask_volume_2`ask_volume_3`ask_volume_4`ask_volume_5,
                       [SYMBOL,SYMBOL,NANOTIMESTAMP,SYMBOL,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,\
                       DOUBLE,DOUBLE,DOUBLE,DOUBLE,NANOTIMESTAMP,\
                       DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,\
                       DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,\
                       DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,\
                       DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE])
        db_tick.createPartitionedTable(tick_t,
                                       `tick,
                                       partitionColumns=["exchange", "symbol"],
                                       sortColumns=["symbol","datetime"],
                                       keepDuplicates=LAST)
    }

    overviewPath = "dfs://vnpy_overview"
    if((existsDatabase(overviewPath).not())){
        overview_exchange = database(, VALUE, ["vnpy"])
        overview_symbol = database(, VALUE, ["vnpy"])
        overview_interval = database(, VALUE, ["vnpy"])
        db_overview = database(overviewPath, COMPO, [overview_exchange, overview_symbol, overview_interval], engine=`TSDB)
        overview_t = table(100:100,
                           `symbol`exchange`interval`count`start`end,
                           [SYMBOL,SYMBOL,SYMBOL,INT,NANOTIMESTAMP,NANOTIMESTAMP])
        db_overview.createPartitionedTable(overview_t,
                                           `overview,
                                           partitionColumns=["exchange", "symbol", "interval"],
                                           sortColumns=["symbol","start"],
                                           keepDuplicates=LAST)
    }
"""


class DolphindbDatabase(BaseDatabase):
    """DolphinDB数据接口"""

    def __init__(self) -> None:
        """初始化数据库"""

        self.dbPath = "dfs://vnpy_"
        # 连接数据库
        self.session = ddb.session()
        self.session.connect("localhost", 8848, "admin", "123456")
        # 连接池用于多线程并发写入
        self.pool = ddb.DBConnectionPool("localhost", 8848, 20, "admin", "123456")
        # dolphindb初始化脚本，用于在第一次时创建数据库和表结构
        self.session.run(script)

    def save_bar_data(self, bars: List[BarData]) -> bool:
        """保存k线数据"""
        # 读取主键参数
        bar: BarData = bars[0]
        symbol = bar.symbol
        exchange = bar.exchange
        interval = bar.interval

        key: List[str] = [i for i in bar.__dict__]

        key.remove("gateway_name")
        key.remove("vt_symbol")

        # 将BarData转化为DafaFrame，并调整时区，存入数据库
        test_dict = {i: [] for i in key}
        for bar in bars:
            test_dict["symbol"].append(str(bar.symbol))
            test_dict["exchange"].append(str(bar.exchange.value))
            test_dict["datetime"].append(np.datetime64(convert_tz(bar.datetime)))
            test_dict["interval"].append(str(bar.interval.value))
            test_dict["volume"].append(float(bar.volume))
            test_dict["turnover"].append(float(bar.turnover))
            test_dict["open_interest"].append(float(bar.open_interest))
            test_dict["open_price"].append(float(bar.open_price))
            test_dict["high_price"].append(float(bar.high_price))
            test_dict["low_price"].append(float(bar.low_price))
            test_dict["close_price"].append(float(bar.close_price))
        data_frame = pd.DataFrame(test_dict)
        appender = ddb.PartitionedTableAppender("dfs://vnpy_bar", "bar", "symbol", self.pool)
        appender.append(data_frame)

        # 读取存入数据的K线汇总数据
        trade = self.session.loadTable(tableName="bar", dbPath="dfs://vnpy_bar")
        df_start = trade.where(
            f"symbol='{symbol}'").where(
            f"exchange='{exchange.value}'").where(
            f"interval='{interval.value}'").sort(bys=["datetime"]).top(1).toDF()

        df_end = trade.where(
            f"symbol='{symbol}'").where(
            f"exchange='{exchange.value}'").where(
            f"interval='{interval.value}'").sort(bys=["datetime desc"]).top(1).toDF()

        df_count = trade.select(
            "count(*)").where(
            f"symbol='{symbol}'").where(
            f"exchange='{exchange.value}'").where(
            f"interval='{interval.value}'").toDF()

        count = df_count["count"][0]
        start = df_start["datetime"][0]
        end = df_end["datetime"][0]

        # 更新K线汇总数据
        data_frame = pd.DataFrame({"symbol": [str(symbol)],
                                   "exchange": [str(exchange.value)],
                                   "interval": [str(interval.value)],
                                   "count": [count],
                                   "start": [start],
                                   "end": [end]})
        appender = ddb.PartitionedTableAppender("dfs://vnpy_overview", "overview", "symbol", self.pool)
        appender.append(data_frame)

    def save_tick_data(self, ticks: List[TickData]) -> bool:
        """保存TICK数据"""
        tick = ticks[0]

        key = [i for i in tick.__dict__]

        key.remove("gateway_name")
        key.remove("vt_symbol")

        # 将TickData转化为DafaFrame，并调整时区，存入数据库
        test_dict = {i: [] for i in key}
        for tick in ticks:
            test_dict["symbol"].append(str(tick.symbol))
            test_dict["exchange"].append(str(tick.exchange.value))
            test_dict["datetime"].append(np.datetime64(convert_tz(tick.datetime)))

            test_dict["name"].append(str(tick.name))
            test_dict["volume"].append(float(tick.volume))
            test_dict["turnover"].append(float(tick.turnover))
            test_dict["open_interest"].append(float(tick.open_interest))
            test_dict["last_price"].append(float(tick.last_price))
            test_dict["last_volume"].append(float(tick.last_volume))
            test_dict["limit_up"].append(float(tick.limit_up))
            test_dict["limit_down"].append(float(tick.limit_down))

            test_dict["open_price"].append(float(tick.open_price))
            test_dict["high_price"].append(float(tick.high_price))
            test_dict["low_price"].append(float(tick.low_price))
            test_dict["pre_close"].append(float(tick.pre_close))

            test_dict["bid_price_1"].append(float(tick.bid_price_1))
            test_dict["bid_price_2"].append(float(tick.bid_price_2))
            test_dict["bid_price_3"].append(float(tick.bid_price_3))
            test_dict["bid_price_4"].append(float(tick.bid_price_4))
            test_dict["bid_price_5"].append(float(tick.bid_price_5))

            test_dict["ask_price_1"].append(float(tick.ask_price_1))
            test_dict["ask_price_2"].append(float(tick.ask_price_2))
            test_dict["ask_price_3"].append(float(tick.ask_price_3))
            test_dict["ask_price_4"].append(float(tick.ask_price_4))
            test_dict["ask_price_5"].append(float(tick.ask_price_5))

            test_dict["bid_volume_1"].append(float(tick.bid_volume_1))
            test_dict["bid_volume_2"].append(float(tick.bid_volume_2))
            test_dict["bid_volume_3"].append(float(tick.bid_volume_3))
            test_dict["bid_volume_4"].append(float(tick.bid_volume_4))
            test_dict["bid_volume_5"].append(float(tick.bid_volume_5))

            test_dict["ask_volume_1"].append(float(tick.ask_volume_1))
            test_dict["ask_volume_2"].append(float(tick.ask_volume_2))
            test_dict["ask_volume_3"].append(float(tick.ask_volume_3))
            test_dict["ask_volume_4"].append(float(tick.ask_volume_4))
            test_dict["ask_volume_5"].append(float(tick.ask_volume_5))

            test_dict["localtime"].append(np.datetime64(convert_tz(tick.localtime)))
        data_frame = pd.DataFrame(test_dict)
        appender = ddb.PartitionedTableAppender("dfs://vnpy_tick", "tick", "symbol", self.pool)
        appender.append(data_frame)

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """读取K线数据"""
        # 将输入的时间格式修改为dolphindb可识别的格式
        start = np.datetime64(start)
        end = np.datetime64(end)
        start = str(start).replace("-", ".")
        end = str(end).replace("-", ".")

        # 读取dolphindb中数据并转化为python可识别的dataframe格式
        trade = self.session.loadTable(tableName="bar", dbPath="dfs://vnpy_bar")
        df = trade.where(
            f"symbol='{symbol}'").where(
            f"exchange='{exchange.value}'").where(
            f"interval='{interval.value}'").where(
            f"datetime>={start}").where(
            f"datetime<={end}").toDF()

        bars: List[BarData] = []
        for symbol, exchange, date_time, interval, volume, open_interest, turnover,\
            open_price, high_price, low_price, close_price\
            in zip(df["symbol"], df["exchange"], df["datetime"], df["interval"], df["volume"], df["turnover"],
                   df["open_interest"], df["open_price"], df["high_price"], df["low_price"], df["close_price"]):

            tz_time = datetime.fromtimestamp(date_time.timestamp(), DB_TZ)
            bar = BarData("DB", symbol, Exchange(exchange), tz_time)
            bar.symbol = symbol
            bar.volume = volume
            bar.turnover = turnover
            bar.open_price = open_price
            bar.high_price = high_price
            bar.low_price = low_price
            bar.close_price = close_price
            bar.open_interest = open_interest
            bar.interval = Interval(interval)
            bars.append(bar)
        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> List[TickData]:
        """读取Tick数据"""
        # 将输入的时间格式修改为dolphindb可识别的格式
        start = np.datetime64(start)
        end = np.datetime64(end)
        start = str(start).replace("-", ".")
        end = str(end).replace("-", ".")

        # 读取dolphindb中数据并转化为python可识别的dataframe格式
        trade = self.session.loadTable(tableName="tick", dbPath="dfs://vnpy_tick")
        df = trade.where(
            f"symbol='{symbol}'").where(
            f"exchange='{exchange.value}'").where(
            f"datetime>={start}").where(
            f"datetime<={end}").toDF()

        ticks: List[TickData] = []
        for symbol, exchange, date_time, name, volume, turnover,\
            open_interest, last_price, last_volume, limit_up, limit_down,\
            open_price, high_price, low_price, pre_close,\
            bid_price_1, bid_price_2, bid_price_3, bid_price_4, bid_price_5,\
            ask_price_1, ask_price_2, ask_price_3, ask_price_4, ask_price_5,\
            bid_volume_1, bid_volume_2, bid_volume_3, bid_volume_4, bid_volume_5,\
            ask_volume_1, ask_volume_2, ask_volume_3, ask_volume_4, ask_volume_5, localtime\
            in zip(df["symbol"], df["exchange"], df["datetime"], df["name"], df["volume"], df["turnover"],
                   df["last_price"], df["last_volume"], df["limit_up"], df["limit_down"],
                   df["open_price"], df["high_price"], df["low_price"], df["pre_close"],
                   df["bid_price_1"], df["bid_price_2"], df["bid_price_3"], df["bid_price_4"], df["bid_price_5"],
                   df["ask_price_1"], df["ask_price_2"], df["ask_price_3"], df["ask_price_4"], df["ask_price_5"],
                   df["bid_volume_1"], df["bid_volume_2"], df["bid_volume_3"], df["bid_volume_4"], df["bid_volume_5"],
                   df["ask_volume_1"], df["ask_volume_2"], df["ask_volume_3"], df["ask_volume_4"], df["ask_volume_5"],
                   df["localtime"]):

            tz_time = datetime.fromtimestamp(date_time.timestamp(), DB_TZ)
            tick = TickData("DB", symbol, Exchange(exchange), tz_time)
            tick.symbol = symbol

            tick.name = name
            tick.volume = volume
            tick.turnover = turnover
            tick.open_interest = open_interest
            tick.last_price = last_price
            tick.last_volume = last_volume
            tick.limit_up = limit_up
            tick.limit_down = limit_down

            tick.open_price = open_price
            tick.high_price = high_price
            tick.low_price = low_price
            tick.pre_close = pre_close

            tick.bid_price_1 = bid_price_1
            tick.bid_price_2 = bid_price_2
            tick.bid_price_3 = bid_price_3
            tick.bid_price_4 = bid_price_4
            tick.bid_price_5 = bid_price_5

            tick.ask_price_1 = ask_price_1
            tick.ask_price_2 = ask_price_2
            tick.ask_price_3 = ask_price_3
            tick.ask_price_4 = ask_price_4
            tick.ask_price_5 = ask_price_5

            tick.bid_volume_1 = bid_volume_1
            tick.bid_volume_2 = bid_volume_2
            tick.bid_volume_3 = bid_volume_3
            tick.bid_volume_4 = bid_volume_4
            tick.bid_volume_5 = bid_volume_5

            tick.ask_volume_1 = ask_volume_1
            tick.ask_volume_2 = ask_volume_2
            tick.ask_volume_3 = ask_volume_3
            tick.ask_volume_4 = ask_volume_4
            tick.ask_volume_5 = ask_volume_5

            tick.localtime = localtime

            ticks.append(tick)
        return ticks

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """删除K线数据"""
        trade = self.session.loadTable(tableName="bar", dbPath="dfs://vnpy_bar")
        df = trade.select(
            "count(*)").where(
            f"symbol='{symbol}'").where(
            f"exchange='{exchange.value}'").where(
            f"interval='{interval.value}'").toDF()

        count = df["count"][0]
        self.session.dropPartition(dbPath="dfs://vnpy_bar",
                                   partitionPaths=[f"'{exchange.value}'", f"'{symbol}'", f"'{interval.value}'"],
                                   tableName="bar")
        # 删除K线汇总数据
        self.session.dropPartition(dbPath="dfs://vnpy_overview",
                                   partitionPaths=[f"'{exchange.value}'", f"'{symbol}'", f"'{interval.value}'"],
                                   tableName="overview")
        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除Tick数据"""
        trade = self.session.loadTable(tableName="tick", dbPath="dfs://vnpy_tick")
        df = trade.select(
            "count(*)").where(
            f"symbol='{symbol}'").where(
            f"exchange='{exchange.value}'").toDF()

        count = df["count"][0]
        self.session.dropPartition(dbPath="dfs://vnpy_tick",
                                   partitionPaths=[f"'{exchange.value}'", f"'{symbol}'"],
                                   tableName="tick")
        return count

    def get_bar_overview(self) -> List[BarOverview]:
        """"查询数据库中的K线汇总信息"""
        trade = self.session.loadTable(tableName="overview", dbPath="dfs://vnpy_overview")
        df = trade.select("*").toDF()
        overviews: List[BarOverview] = []
        for symbol, exchange, interval, count, start, end\
            in zip(df["symbol"], df["exchange"], df["interval"],
                   df["count"], df["start"], df["end"]):

            overview = BarOverview()
            overview.symbol = symbol
            overview.exchange = Exchange(exchange)
            overview.interval = Interval(interval)
            overview.count = count
            overview.start = datetime.fromtimestamp(start.timestamp(), DB_TZ)
            overview.end = datetime.fromtimestamp(end.timestamp(), DB_TZ)
            overviews.append(overview)
        return overviews

    def drop(self) -> None:
        """删除数据库"""
        start = self.session.existsDatabase(self.dbPath + "bar")
        self.session.dropDatabase(self.dbPath + "bar")
        end = self.session.existsDatabase(self.dbPath + "bar")
        if start and not end:
            print("bar数据库已删除")
        else:
            print("bar未正常删除数据库")

        start = self.session.existsDatabase(self.dbPath + "tick")
        self.session.dropDatabase(self.dbPath + "tick")
        end = self.session.existsDatabase(self.dbPath + "tick")
        if start and not end:
            print("tick数据库已删除")
        else:
            print("tick未正常删除数据库")

        start = self.session.existsDatabase(self.dbPath + "overview")
        self.session.dropDatabase(self.dbPath + "overview")
        end = self.session.existsDatabase(self.dbPath + "overview")
        if start and not end:
            print("overview数据库已删除")
        else:
            print("overview未正常删除数据库")
