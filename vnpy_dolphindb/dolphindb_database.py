from datetime import datetime

import numpy as np
import pandas as pd
import dolphindb as ddb

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    TickOverview,
    DB_TZ,
    convert_tz
)
from vnpy.trader.setting import SETTINGS

from .dolphindb_script import (
    CREATE_DATABASE_SCRIPT,
    CREATE_BAR_TABLE_SCRIPT,
    CREATE_TICK_TABLE_SCRIPT,
    CREATE_BAROVERVIEW_TABLE_SCRIPT,
    CREATE_TICKOVERVIEW_TABLE_SCRIPT
)


class DolphindbDatabase(BaseDatabase):
    """DolphinDB数据库接口"""

    def __init__(self) -> None:
        """构造函数"""
        self.user: str = SETTINGS["database.user"]
        self.password: str = SETTINGS["database.password"]
        self.host: str = SETTINGS["database.host"]
        self.port: int = SETTINGS["database.port"]
        self.db_path: str = "dfs://" + SETTINGS["database.database"]

        # 连接数据库
        self.session: ddb.session = ddb.session()
        self.session.connect(self.host, self.port, self.user, self.password)

        # 创建连接池（用于数据写入）
        self.pool: ddb.DBConnectionPool = ddb.DBConnectionPool(self.host, self.port, 1, self.user, self.password)

        # 初始化数据库和数据表
        if not self.session.existsDatabase(self.db_path):
            self.session.run(CREATE_DATABASE_SCRIPT)
            self.session.run(CREATE_BAR_TABLE_SCRIPT)
            self.session.run(CREATE_TICK_TABLE_SCRIPT)
            self.session.run(CREATE_BAROVERVIEW_TABLE_SCRIPT)
            self.session.run(CREATE_TICKOVERVIEW_TABLE_SCRIPT)

    def __del__(self) -> None:
        """析构函数"""
        if not self.session.isClosed():
            self.session.close()

    def save_bar_data(self, bars: list[BarData], stream: bool = False) -> bool:
        """保存k线数据"""
        # 读取主键参数
        bar: BarData = bars[0]
        symbol: str = bar.symbol
        exchange: Exchange = bar.exchange
        interval: Interval = bar.interval

        # 转换为DatFrame写入数据库
        data: list[dict] = []

        for bar in bars:
            dt: np.datetime64 = np.datetime64(convert_tz(bar.datetime))

            d: dict = {
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

        appender: ddb.PartitionedTableAppender = ddb.PartitionedTableAppender(self.db_path, "bar", "datetime", self.pool)
        appender.append(df)

        # 计算已有K线数据的汇总
        overview_table = self.session.loadTable(tableName="baroverview", dbPath=self.db_path)
        overview: pd.DataFrame = (
            overview_table.select('*')
            .where(f'symbol="{symbol}"')
            .where(f'exchange="{exchange.value}"')
            .where(f'interval="{interval.value}"')
            .toDF()
        )

        if overview.empty:
            start: datetime = np.datetime64(bars[0].datetime)
            end: datetime = np.datetime64(bars[-1].datetime)
            count: int = len(bars)
        elif stream:
            start: datetime = overview["start"][0]
            end: datetime = np.datetime64(bars[-1].datetime)
            count: int = overview["count"][0] + len(bars)
        else:
            start: datetime = min(np.datetime64(bars[0].datetime), overview["start"][0])
            end: datetime = max(np.datetime64(bars[-1].datetime), overview["end"][0])

            bar_table = self.session.loadTable(tableName="bar", dbPath=self.db_path)

            df_count: pd.DataFrame = (
                bar_table.select('count(*)')
                .where(f'symbol="{symbol}"')
                .where(f'exchange="{exchange.value}"')
                .where(f'interval="{interval.value}"')
                .toDF()
            )

            count: int = df_count["count"][0]

        # 更新K线汇总数据
        data: list[dict] = []

        dt: np.datetime64 = np.datetime64(datetime(2022, 1, 1))    # 该时间戳仅用于分区

        d: dict = {
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

        appender: ddb.PartitionedTableAppender = ddb.PartitionedTableAppender(self.db_path, "baroverview", "datetime", self.pool)
        appender.append(df)

        return True

    def save_tick_data(self, ticks: list[TickData], stream: bool = False) -> bool:
        """保存TICK数据"""
        # 读取主键参数
        tick: BarData = ticks[0]
        symbol: str = tick.symbol
        exchange: Exchange = tick.exchange

        data: list[dict] = []

        for tick in ticks:
            dt: np.datetime64 = np.datetime64(convert_tz(tick.datetime))

            d: dict = {
                "symbol": tick.symbol,
                "exchange": tick.exchange.value,
                "datetime": dt,

                "name": tick.name,
                "volume": float(tick.volume),
                "turnover": float(tick.turnover),
                "open_interest": float(tick.open_interest),
                "last_price": float(tick.last_price),
                "last_volume": float(tick.last_volume),
                "limit_up": float(tick.limit_up),
                "limit_down": float(tick.limit_down),

                "open_price": float(tick.open_price),
                "high_price": float(tick.high_price),
                "low_price": float(tick.low_price),
                "pre_close": float(tick.pre_close),

                "bid_price_1": float(tick.bid_price_1),
                "bid_price_2": float(tick.bid_price_2),
                "bid_price_3": float(tick.bid_price_3),
                "bid_price_4": float(tick.bid_price_4),
                "bid_price_5": float(tick.bid_price_5),

                "ask_price_1": float(tick.ask_price_1),
                "ask_price_2": float(tick.ask_price_2),
                "ask_price_3": float(tick.ask_price_3),
                "ask_price_4": float(tick.ask_price_4),
                "ask_price_5": float(tick.ask_price_5),

                "bid_volume_1": float(tick.bid_volume_1),
                "bid_volume_2": float(tick.bid_volume_2),
                "bid_volume_3": float(tick.bid_volume_3),
                "bid_volume_4": float(tick.bid_volume_4),
                "bid_volume_5": float(tick.bid_volume_5),

                "ask_volume_1": float(tick.ask_volume_1),
                "ask_volume_2": float(tick.ask_volume_2),
                "ask_volume_3": float(tick.ask_volume_3),
                "ask_volume_4": float(tick.ask_volume_4),
                "ask_volume_5": float(tick.ask_volume_5),

                "localtime": np.datetime64(tick.localtime),
            }

            data.append(d)

        df: pd.DataFrame = pd.DataFrame.from_records(data)

        appender: ddb.PartitionedTableAppender = ddb.PartitionedTableAppender(self.db_path, "tick", "datetime", self.pool)
        appender.append(df)

        # 计算已有Tick数据的汇总
        overview_table = self.session.loadTable(tableName="tickoverview", dbPath=self.db_path)
        overview: pd.DataFrame = (
            overview_table.select('*')
            .where(f'symbol="{symbol}"')
            .where(f'exchange="{exchange.value}"')
            .toDF()
        )

        if overview.empty:
            start: datetime = np.datetime64(ticks[0].datetime)
            end: datetime = np.datetime64(ticks[-1].datetime)
            count: int = len(ticks)
        elif stream:
            start: datetime = overview["start"][0]
            end: datetime = np.datetime64(ticks[-1].datetime)
            count: int = overview["count"][0] + len(ticks)
        else:
            start: datetime = min(np.datetime64(ticks[0].datetime), overview["start"][0])
            end: datetime = max(np.datetime64(ticks[-1].datetime), overview["end"][0])

            bar_table = self.session.loadTable(tableName="tick", dbPath=self.db_path)

            df_count: pd.DataFrame = (
                bar_table.select('count(*)')
                .where(f'symbol="{symbol}"')
                .where(f'exchange="{exchange.value}"')
                .toDF()
            )

            count: int = df_count["count"][0]

        # 更新Tick汇总数据
        data: list[dict] = []

        dt: np.datetime64 = np.datetime64(datetime(2022, 1, 1))    # 该时间戳仅用于分区

        d: dict = {
            "symbol": symbol,
            "exchange": exchange.value,
            "count": count,
            "start": start,
            "end": end,
            "datetime": dt,
        }
        data.append(d)

        df: pd.DataFrame = pd.DataFrame.from_records(data)

        appender: ddb.PartitionedTableAppender = ddb.PartitionedTableAppender(self.db_path, "tickoverview", "datetime", self.pool)
        appender.append(df)

        return True

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> list[BarData]:
        """读取K线数据"""
        # 转换时间格式
        start = np.datetime64(start)
        start: str = str(start).replace("-", ".")

        end = np.datetime64(end)
        end: str = str(end).replace("-", ".")

        table: ddb.Table = self.session.loadTable(tableName="bar", dbPath=self.db_path)

        df: pd.DataFrame = (
            table.select('*')
            .where(f'symbol="{symbol}"')
            .where(f'exchange="{exchange.value}"')
            .where(f'interval="{interval.value}"')
            .where(f'datetime>={start}')
            .where(f'datetime<={end}')
            .toDF()
        )

        if df.empty:
            return []

        df.set_index("datetime", inplace=True)
        df = df.tz_localize(DB_TZ.key)

        # 转换为BarData格式
        bars: list[BarData] = []

        for tp in df.itertuples():
            bar = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=tp.Index.to_pydatetime(),
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
    ) -> list[TickData]:
        """读取Tick数据"""
        # 转换时间格式
        start = np.datetime64(start)
        start: str = str(start).replace("-", ".")

        end = np.datetime64(end)
        end: str = str(end).replace("-", ".")

        # 读取数据DataFrame
        table: ddb.Table = self.session.loadTable(tableName="tick", dbPath=self.db_path)

        df: pd.DataFrame = (
            table.select('*')
            .where(f'symbol="{symbol}"')
            .where(f'exchange="{exchange.value}"')
            .where(f'datetime>={start}')
            .where(f'datetime<={end}')
            .toDF()
        )

        if df.empty:
            return []

        df.set_index("datetime", inplace=True)
        df = df.tz_localize(DB_TZ.key)

        # 转换为TickData格式
        ticks: list[TickData] = []

        for tp in df.itertuples():
            tick: TickData = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=tp.Index.to_pydatetime(),
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
        # 加载数据表
        table: ddb.Table = self.session.loadTable(tableName="bar", dbPath=self.db_path)

        # 统计数据量
        df: pd.DataFrame = (
            table.select('count(*)')
            .where(f'symbol="{symbol}"')
            .where(f'exchange="{exchange.value}"')
            .where(f'interval="{interval.value}"')
            .toDF()
        )
        count: int = df["count"][0]

        # 删除K线数据
        (
            table.delete()
            .where(f'symbol="{symbol}"')
            .where(f'exchange="{exchange.value}"')
            .where(f'interval="{interval.value}"')
            .execute()
        )

        # 删除K线汇总
        table: ddb.Table = self.session.loadTable(tableName="baroverview", dbPath=self.db_path)
        (
            table.delete()
            .where(f'symbol="{symbol}"')
            .where(f'exchange="{exchange.value}"')
            .where(f'interval="{interval.value}"')
            .execute()
        )

        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除Tick数据"""
        # 加载数据表
        table: ddb.Table = self.session.loadTable(tableName="tick", dbPath=self.db_path)

        # 统计数据量
        df: pd.DataFrame = (
            table.select('count(*)')
            .where(f'symbol="{symbol}"')
            .where(f'exchange="{exchange.value}"')
            .toDF()
        )
        count: int = df["count"][0]

        # 删除Tick数据
        (
            table.delete()
            .where(f'symbol="{symbol}"')
            .where(f'exchange="{exchange.value}"')
            .execute()
        )

        # 删除Tick汇总
        table: ddb.Table = self.session.loadTable(tableName="tickoverview", dbPath=self.db_path)
        (
            table.delete()
            .where(f'symbol="{symbol}"')
            .where(f'exchange="{exchange.value}"')
            .execute()
        )

        return count

    def get_bar_overview(self) -> list[BarOverview]:
        """"查询数据库中的K线汇总信息"""
        table: ddb.Table = self.session.loadTable(tableName="baroverview", dbPath=self.db_path)
        df: pd.DataFrame = table.select('*').toDF()

        overviews: list[BarOverview] = []

        for tp in df.itertuples():
            overview: BarOverview = BarOverview(
                symbol=tp.symbol,
                exchange=Exchange(tp.exchange),
                interval=Interval(tp.interval),
                count=tp.count,
                start=datetime.fromtimestamp(tp.start.to_pydatetime().timestamp(), DB_TZ),
                end=datetime.fromtimestamp(tp.end.to_pydatetime().timestamp(), DB_TZ),
            )
            overviews.append(overview)

        return overviews

    def get_tick_overview(self) -> list[TickOverview]:
        """"查询数据库中的K线汇总信息"""
        table: ddb.Table = self.session.loadTable(tableName="tickoverview", dbPath=self.db_path)
        df: pd.DataFrame = table.select('*').toDF()

        overviews: list[TickOverview] = []

        for tp in df.itertuples():
            overview: TickOverview = TickOverview(
                symbol=tp.symbol,
                exchange=Exchange(tp.exchange),
                count=tp.count,
                start=datetime.fromtimestamp(tp.start.to_pydatetime().timestamp(), DB_TZ),
                end=datetime.fromtimestamp(tp.end.to_pydatetime().timestamp(), DB_TZ),
            )
            overviews.append(overview)

        return overviews
