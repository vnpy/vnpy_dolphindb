from typing import Dict, List
from datetime import datetime

import numpy as np
import pandas as pd
import dolphindb as ddb

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    DB_TZ,
    convert_tz
)
from vnpy.trader.setting import SETTINGS

from .dolphindb_script import (
    create_database,
    create_bar_table,
    create_tick_table,
    create_overview_table
)


class DolphindbDatabase(BaseDatabase):
    """DolphinDB数据库接口"""

    def __init__(self) -> None:
        """"""
        self.user: str = SETTINGS["database.user"]
        self.password: str = SETTINGS["database.password"]
        self.host: str = SETTINGS["database.host"]
        self.port: int = SETTINGS["database.port"]
        self.db_path: str = "dfs://" + SETTINGS["database.database"]

        # 连接数据库
        self.session = ddb.session()
        self.session.connect(self.host, self.port, self.user, self.password)

        # 创建连接池（用于多线程并发写入）
        self.pool = ddb.DBConnectionPool(self.host, self.port, 1, self.user, self.password)

        # dolphindb初始化脚本，用于在第一次时创建数据库和表结构
        if not self.session.existsDatabase(self.db_path):
            self.session.run(create_database)
            self.session.run(create_bar_table)
            self.session.run(create_tick_table)
            self.session.run(create_overview_table)

    def save_bar_data(self, bars: List[BarData]) -> bool:
        """保存k线数据"""
        # 读取主键参数
        bar: BarData = bars[0]
        symbol: str = bar.symbol
        exchange: Exchange = bar.exchange
        interval: Interval = bar.interval

        data: List[dict] = []

        for bar in bars:
            dt = np.datetime64(convert_tz(bar.datetime))

            d = {
                "symbol": symbol,
                "exchange": exchange.value,
                "datetime": dt,
                "interval": interval.value,
                "volume": float(bar.volume),
                "turnover": float(bar.turnover),
                "open_interest": float(bar.open_interest),
                "open_price": float(bar.open_price),
                "high_price": float(bar.high_price),
                "low_price": float(bar.low_price),
                "close_price": float(bar.close_price)
            }

            data.append(d)

        df: pd.DataFrame = pd.DataFrame.from_records(data)

        appender = ddb.PartitionedTableAppender(self.db_path, "bar", "datetime", self.pool)
        appender.append(df)

        # 读取存入数据的K线汇总数据
        trade = self.session.loadTable(tableName="bar", dbPath=self.db_path)

        df_start: pd.DataFrame = (
            trade.select("*")
            .where(f"symbol='{symbol}'")
            .where(f"exchange='{exchange.value}'")
            .where(f"interval='{interval.value}'")
            .sort(bys=["datetime"]).top(1)
            .toDF()
        )

        df_end: pd.DataFrame = (
            trade.select("*")
            .where(f"symbol='{symbol}'")
            .where(f"exchange='{exchange.value}'")
            .where(f"interval='{interval.value}'")
            .sort(bys=["datetime desc"]).top(1)
            .toDF()
        )

        df_count: pd.DataFrame = (
            trade.select("count(*)")
            .where(f"symbol='{symbol}'")
            .where(f"exchange='{exchange.value}'")
            .where(f"interval='{interval.value}'")
            .toDF()
        )

        count: int = df_count["count"][0]
        start = df_start["datetime"][0]
        end = df_end["datetime"][0]

        # 更新K线汇总数据
        data: List[dict] = []

        dt = np.datetime64(datetime(2022, 1, 1))
        d: Dict = {
            "symbol": symbol,
            "exchange": exchange.value,
            "interval": interval.value,
            "count": count,
            "start": start,
            "end": end,
            "datetime": dt,
        }
        data.append(d)

        df: pd.DataFrame = pd.DataFrame.from_records(data)

        appender = ddb.PartitionedTableAppender(self.db_path, "overview", "datetime", self.pool)
        appender.append(df)

        return True

    def save_tick_data(self, ticks: List[TickData]) -> bool:
        """保存TICK数据"""
        data: List[dict] = []

        for tick in ticks:
            dt = np.datetime64(convert_tz(tick.datetime))

            d: Dict = {
                "symbol": tick.symbol,
                "exchange": tick.exchange.value,
                "datetime": dt,

                "name": tick.name,
                "volume": tick.volume,
                "turnover": float(tick.turnover),
                "open_interest": tick.open_interest,
                "last_price": tick.last_price,
                "last_volume": float(tick.last_volume),
                "limit_up": tick.limit_up,
                "limit_down": tick.limit_down,

                "open_price": tick.open_price,
                "high_price": tick.high_price,
                "low_price": tick.low_price,
                "pre_close": tick.pre_close,
                "interval": "tick",

                "bid_price_1": tick.bid_price_1,
                "bid_price_2": tick.bid_price_2,
                "bid_price_3": tick.bid_price_3,
                "bid_price_4": tick.bid_price_4,
                "bid_price_5": tick.bid_price_5,

                "ask_price_1": tick.ask_price_1,
                "ask_price_2": tick.ask_price_2,
                "ask_price_3": tick.ask_price_3,
                "ask_price_4": tick.ask_price_4,
                "ask_price_5": tick.ask_price_5,

                "bid_volume_1": tick.bid_volume_1,
                "bid_volume_2": tick.bid_volume_2,
                "bid_volume_3": tick.bid_volume_3,
                "bid_volume_4": tick.bid_volume_4,
                "bid_volume_5": tick.bid_volume_5,

                "ask_volume_1": tick.ask_volume_1,
                "ask_volume_2": tick.ask_volume_2,
                "ask_volume_3": tick.ask_volume_3,
                "ask_volume_4": tick.ask_volume_4,
                "ask_volume_5": tick.ask_volume_5,

                "localtime": np.datetime64(tick.localtime),
            }

            data.append(d)

        df: pd.DataFrame = pd.DataFrame.from_records(data)

        appender = ddb.PartitionedTableAppender(self.db_path, "tick", "datetime", self.pool)
        appender.append(df)

        return True

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
        start: str = str(start).replace("-", ".")
        end: str = str(end).replace("-", ".")

        # 读取dolphindb中数据并转化为python可识别的dataframe格式
        trade = self.session.loadTable(tableName="bar", dbPath=self.db_path)
        df: pd.DataFrame = (
            trade.select("*")
            .where(f"symbol='{symbol}'")
            .where(f"exchange='{exchange.value}'")
            .where(f"interval='{interval.value}'")
            .where(f"datetime>={start}")
            .where(f"datetime<={end}")
            .toDF()
        )
        bars: List[BarData] = []

        for tp in df.itertuples():
            dt = datetime.fromtimestamp(tp.datetime.to_pydatetime().timestamp(), DB_TZ)

            bar = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=dt,
                interval=interval,
                volume=tp.volume,
                turnover=tp.turnover,
                open_interest=tp.open_interest,
                open_price=tp.open_price,
                high_price=tp.high_price,
                low_price=tp.low_price,
                close_price=tp.close_price,
                gateway_name="DB"
            )
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
        start: str = str(start).replace("-", ".")
        end: str = str(end).replace("-", ".")

        # 读取dolphindb中数据并转化为python可识别的dataframe格式
        trade = self.session.loadTable(tableName="tick", dbPath=self.db_path)
        df: pd.DataFrame = (
            trade.select("*")
            .where(f"symbol='{symbol}'")
            .where(f"exchange='{exchange.value}'")
            .where(f"datetime>={start}")
            .where(f"datetime<={end}")
            .toDF()
        )
        ticks: List[TickData] = []

        for tp in df.itertuples():
            dt = datetime.fromtimestamp(tp.datetime.to_pydatetime().timestamp(), DB_TZ)

            tick = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=dt,
                name=tp.name,
                volume=tp.volume,
                turnover=tp.turnover,
                open_interest=tp.open_interest,
                last_price=tp.last_price,
                last_volume=tp.last_volume,
                limit_up=tp.limit_up,
                limit_down=tp.limit_down,
                open_price=tp.open_price,
                high_price=tp.high_price,
                low_price=tp.low_price,
                pre_close=tp.pre_close,
                bid_price_1=tp.bid_price_1,
                bid_price_2=tp.bid_price_2,
                bid_price_3=tp.bid_price_3,
                bid_price_4=tp.bid_price_4,
                bid_price_5=tp.bid_price_5,
                ask_price_1=tp.ask_price_1,
                ask_price_2=tp.ask_price_2,
                ask_price_3=tp.ask_price_3,
                ask_price_4=tp.ask_price_4,
                ask_price_5=tp.ask_price_5,
                bid_volume_1=tp.bid_volume_1,
                bid_volume_2=tp.bid_volume_2,
                bid_volume_3=tp.bid_volume_3,
                bid_volume_4=tp.bid_volume_4,
                bid_volume_5=tp.bid_volume_5,
                ask_volume_1=tp.ask_volume_1,
                ask_volume_2=tp.ask_volume_2,
                ask_volume_3=tp.ask_volume_3,
                ask_volume_4=tp.ask_volume_4,
                ask_volume_5=tp.ask_volume_5,
                localtime=tp.localtime,
                gateway_name="DB"
            )
            ticks.append(tick)
        return ticks

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """删除K线数据"""
        trade = self.session.loadTable(tableName="bar", dbPath=self.db_path)

        df: pd.DataFrame = (
            trade.select("count(*)")
            .where(f"symbol='{symbol}'")
            .where(f"exchange='{exchange.value}'")
            .where(f"interval='{interval.value}'")
            .toDF()
        )

        count = df["count"][0]

        (
            trade.delete()
            .where(f"symbol='{symbol}'")
            .where(f"exchange='{exchange.value}'")
            .where(f"interval='{interval.value}'")
            .execute()
        )

        trade = self.session.loadTable(tableName="overview", dbPath=self.db_path)
        (
            trade.delete()
            .where(f"symbol='{symbol}'")
            .where(f"exchange='{exchange.value}'")
            .where(f"interval='{interval.value}'")
            .execute()
        )

        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除Tick数据"""
        trade = self.session.loadTable(tableName="tick", dbPath=self.db_path)

        df: pd.DataFrame = (
            trade.select("count(*)")
            .where(f"symbol='{symbol}'")
            .where(f"exchange='{exchange.value}'")
            .toDF()
        )

        count: int = df["count"][0]

        (
            trade.delete()
            .where(f"symbol='{symbol}'")
            .where(f"exchange='{exchange.value}'")
            .execute()
        )

        return count

    def get_bar_overview(self) -> List[BarOverview]:
        """"查询数据库中的K线汇总信息"""
        trade = self.session.loadTable(tableName="overview", dbPath=self.db_path)
        df: pd.DataFrame = (
            trade.select("*")
            .toDF()
        )

        overviews: List[BarOverview] = []

        for tp in df.itertuples():
            overview = BarOverview(
                symbol=tp.symbol,
                exchange=Exchange(tp.exchange),
                interval=Interval(tp.interval),
                count=tp.count,
                start=datetime.fromtimestamp(tp.start.to_pydatetime().timestamp(), DB_TZ),
                end=datetime.fromtimestamp(tp.end.to_pydatetime().timestamp(), DB_TZ),
            )
            overviews.append(overview)
        return overviews

    def drop(self) -> None:
        """删除数据库"""

        start = self.session.existsDatabase(self.db_path)
        self.session.dropDatabase(self.db_path)
        end = self.session.existsDatabase(self.db_path)
        if start and not end:
            print("数据库已删除")
        else:
            print("未正常删除数据库")