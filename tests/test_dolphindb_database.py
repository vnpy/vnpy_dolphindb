import importlib
import sys
import types
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import DB_TZ, convert_tz
from vnpy.trader.object import BarData, TickData
from vnpy.trader.setting import SETTINGS


# dolphindb 客户端未安装时，先放入不连接的模块，再导入数据库类。
def _prepare_dolphindb() -> None:
    try:
        importlib.import_module("dolphindb")
    except Exception:
        for name in list(sys.modules):
            if name == "dolphindb" or name.startswith("dolphindb."):
                del sys.modules[name]
        module: types.ModuleType = types.ModuleType("dolphindb")
        module.session = object
        module.DBConnectionPool = object
        module.PartitionedTableAppender = object
        sys.modules["dolphindb"] = module


_prepare_dolphindb()

SETTINGS["database.database"] = "vnpy_test"
SETTINGS["database.host"] = "127.0.0.1"
SETTINGS["database.port"] = 8848
SETTINGS["database.user"] = "admin"
SETTINGS["database.password"] = "123456"

from vnpy_dolphindb.dolphindb_database import DolphindbDatabase  # noqa: E402
from vnpy_dolphindb.dolphindb_script import (  # noqa: E402
    CREATE_BAR_TABLE_SCRIPT,
    CREATE_BAROVERVIEW_TABLE_SCRIPT,
    CREATE_DATABASE_SCRIPT,
    CREATE_TICK_TABLE_SCRIPT,
    CREATE_TICKOVERVIEW_TABLE_SCRIPT,
)
import vnpy_dolphindb.dolphindb_database as ddb_mod  # noqa: E402


frames: dict[str, pd.DataFrame] = {}
appenders: list["_Appender"] = []


class _Query:
    def __init__(self, table: "_Table", kind: str, expr: str | None) -> None:
        self.table = table
        self.kind = kind
        self.expr = expr
        self.wheres: list[str] = []

    def where(self, cond: str) -> "_Query":
        self.wheres.append(cond)
        return self

    def toDF(self) -> pd.DataFrame:
        self.table.calls.append((self.kind, self.expr, list(self.wheres)))
        if self.expr == "count(*)":
            return pd.DataFrame({"count": [2]})
        if self.expr == "*" and self.table.frame is not None:
            frame: pd.DataFrame = self.table.frame
            self.table.frame = None
            return frame
        return pd.DataFrame()

    def execute(self) -> None:
        self.table.calls.append(("execute", self.expr, list(self.wheres)))


class _Table:
    def __init__(self, name: str, db_path: str) -> None:
        self.name = name
        self.db_path = db_path
        self.calls: list[tuple[str, str | None, list[str]]] = []
        self.frame: pd.DataFrame | None = frames.pop(name, None)

    def select(self, expr: str) -> _Query:
        return _Query(self, "select", expr)

    def delete(self) -> _Query:
        return _Query(self, "delete", None)


class _Session:
    def __init__(self) -> None:
        self.connected: tuple[object, ...] | None = None
        self.runs: list[str] = []
        self.tables: list[_Table] = []
        self.closed = False

    def connect(self, host: str, port: int, user: str, password: str) -> None:
        self.connected = (host, port, user, password)

    def existsDatabase(self, _path: str) -> bool:
        return False

    def run(self, script: str) -> None:
        self.runs.append(script)

    def loadTable(self, tableName: str, dbPath: str) -> _Table:
        table: _Table = _Table(tableName, dbPath)
        self.tables.append(table)
        return table

    def isClosed(self) -> bool:
        return self.closed

    def close(self) -> None:
        self.closed = True


class _Pool:
    def __init__(self, host: str, port: int, size: int, user: str, password: str) -> None:
        self.args = (host, port, size, user, password)


class _Appender:
    def __init__(self, db_path: str, table: str, partition: str, pool: object) -> None:
        self.db_path = db_path
        self.table = table
        self.partition = partition
        self.pool = pool
        self.frames: list[pd.DataFrame] = []
        appenders.append(self)

    def append(self, frame: pd.DataFrame) -> None:
        self.frames.append(frame.copy())


def _make_bars(symbol: str) -> list[BarData]:
    start: datetime = datetime(2024, 1, 15, 10, 0, tzinfo=DB_TZ)
    end: datetime = datetime(2024, 1, 15, 10, 1, tzinfo=DB_TZ)
    return [
        BarData(
            gateway_name="TEST",
            symbol=symbol,
            exchange=Exchange.SHFE,
            datetime=start,
            interval=Interval.MINUTE,
            volume=12.0,
            turnover=1.5,
            open_interest=3.0,
            open_price=100.0,
            high_price=110.0,
            low_price=90.0,
            close_price=105.0,
        ),
        BarData(
            gateway_name="TEST",
            symbol=symbol,
            exchange=Exchange.SHFE,
            datetime=end,
            interval=Interval.MINUTE,
            volume=8.0,
            open_price=105.0,
            high_price=112.0,
            low_price=101.0,
            close_price=108.0,
        ),
    ]


def _make_ticks(symbol: str) -> list[TickData]:
    start: datetime = datetime(2024, 1, 16, 10, 0, tzinfo=DB_TZ)
    end: datetime = datetime(2024, 1, 16, 10, 0, 1, tzinfo=DB_TZ)
    return [
        TickData(
            gateway_name="TEST",
            symbol=symbol,
            exchange=Exchange.SHFE,
            datetime=start,
            name="au",
            last_price=400.5,
            volume=20.0,
            localtime=start,
        ),
        TickData(
            gateway_name="TEST",
            symbol=symbol,
            exchange=Exchange.SHFE,
            datetime=end,
            name="au",
            last_price=401.0,
            volume=21.0,
            localtime=end,
        ),
    ]


def _dotted(dt: datetime) -> str:
    return str(np.datetime64(dt)).replace("-", ".")


@pytest.fixture
def database(monkeypatch: pytest.MonkeyPatch) -> DolphindbDatabase:
    frames.clear()
    appenders.clear()
    monkeypatch.setattr(ddb_mod.ddb, "session", _Session)
    monkeypatch.setattr(ddb_mod.ddb, "DBConnectionPool", _Pool)
    monkeypatch.setattr(ddb_mod.ddb, "PartitionedTableAppender", _Appender)
    return DolphindbDatabase()


def test_init_runs_create_scripts_without_connecting(database: DolphindbDatabase) -> None:
    session: _Session = database.session
    assert session.connected == ("127.0.0.1", 8848, "admin", "123456")
    assert session.runs == [
        CREATE_DATABASE_SCRIPT,
        CREATE_BAR_TABLE_SCRIPT,
        CREATE_TICK_TABLE_SCRIPT,
        CREATE_BAROVERVIEW_TABLE_SCRIPT,
        CREATE_TICKOVERVIEW_TABLE_SCRIPT,
    ]
    assert database.db_path == "dfs://vnpy_test"
    assert database.pool.args == ("127.0.0.1", 8848, 1, "admin", "123456")


def test_save_bar_data_appends_ohlcv_frame(database: DolphindbDatabase) -> None:
    symbol: str = "rb2405"
    bars: list[BarData] = _make_bars(symbol)
    assert database.save_bar_data(bars) is True
    assert [item.table for item in appenders] == ["bar", "baroverview"]
    assert appenders[0].db_path == "dfs://vnpy_test"
    assert appenders[0].partition == "datetime"

    frame: pd.DataFrame = appenders[0].frames[0]
    assert frame["symbol"].tolist() == [symbol, symbol]
    assert frame["exchange"].tolist() == [Exchange.SHFE.value, Exchange.SHFE.value]
    assert frame["interval"].tolist() == [Interval.MINUTE.value, Interval.MINUTE.value]
    assert frame["volume"].tolist() == [12.0, 8.0]
    assert frame["turnover"].tolist() == [1.5, 0.0]
    assert frame["open_interest"].tolist() == [3.0, 0.0]
    assert frame["open_price"].tolist() == [100.0, 105.0]
    assert frame["high_price"].tolist() == [110.0, 112.0]
    assert frame["low_price"].tolist() == [90.0, 101.0]
    assert frame["close_price"].tolist() == [105.0, 108.0]
    assert frame["datetime"].tolist() == [
        np.datetime64(convert_tz(bars[0].datetime)),
        np.datetime64(convert_tz(bars[1].datetime)),
    ]

    overview: pd.DataFrame = appenders[1].frames[0]
    assert overview["symbol"].iloc[0] == symbol
    assert overview["exchange"].iloc[0] == Exchange.SHFE.value
    assert overview["interval"].iloc[0] == Interval.MINUTE.value
    assert int(overview["count"].iloc[0]) == 2

    session: _Session = database.session
    overview_table: _Table = session.tables[-1]
    assert overview_table.name == "baroverview"
    assert overview_table.db_path == "dfs://vnpy_test"
    assert overview_table.calls[-1][2] == [
        f'symbol="{symbol}"',
        f'exchange="{Exchange.SHFE.value}"',
        f'interval="{Interval.MINUTE.value}"',
    ]


def test_load_bar_data_filters_symbol_and_datetime_bounds(database: DolphindbDatabase) -> None:
    symbol: str = "rb2405"
    start: datetime = datetime(2024, 1, 1)
    end: datetime = datetime(2024, 2, 1)
    frames["bar"] = pd.DataFrame(
        {
            "datetime": [pd.Timestamp("2024-01-15 10:00:00")],
            "volume": [12.0],
            "turnover": [1.5],
            "open_interest": [3.0],
            "open_price": [100.0],
            "high_price": [110.0],
            "low_price": [90.0],
            "close_price": [105.0],
        }
    )

    loaded: list[BarData] = database.load_bar_data(
        symbol,
        Exchange.SHFE,
        Interval.MINUTE,
        start,
        end,
    )
    bar_table: _Table = [table for table in database.session.tables if table.name == "bar"][-1]
    assert bar_table.calls[-1][0] == "select"
    assert bar_table.calls[-1][1] == "*"
    assert bar_table.calls[-1][2] == [
        f'symbol="{symbol}"',
        f'exchange="{Exchange.SHFE.value}"',
        f'interval="{Interval.MINUTE.value}"',
        f"datetime>={_dotted(start)}",
        f"datetime<={_dotted(end)}",
    ]
    assert "-" not in bar_table.calls[-1][2][3]
    assert "2024.01.01" in bar_table.calls[-1][2][3]
    assert "2024.02.01" in bar_table.calls[-1][2][4]
    assert len(loaded) == 1
    assert loaded[0].symbol == symbol
    assert loaded[0].exchange == Exchange.SHFE
    assert loaded[0].interval == Interval.MINUTE
    assert loaded[0].volume == 12.0
    assert loaded[0].open_price == 100.0
    assert loaded[0].high_price == 110.0
    assert loaded[0].low_price == 90.0
    assert loaded[0].close_price == 105.0
    assert loaded[0].gateway_name == "DB"
    assert loaded[0].datetime == pd.Timestamp("2024-01-15 10:00:00", tz=DB_TZ.key).to_pydatetime()


def test_delete_bar_data_uses_symbol_exchange_interval(database: DolphindbDatabase) -> None:
    symbol: str = "rb2405"
    count: int = database.delete_bar_data(symbol, Exchange.SHFE, Interval.MINUTE)
    assert count == 2
    key: list[str] = [
        f'symbol="{symbol}"',
        f'exchange="{Exchange.SHFE.value}"',
        f'interval="{Interval.MINUTE.value}"',
    ]
    bar_table: _Table = [table for table in database.session.tables if table.name == "bar"][-1]
    overview: _Table = [table for table in database.session.tables if table.name == "baroverview"][-1]
    assert ("select", "count(*)", key) in bar_table.calls
    assert ("execute", None, key) in bar_table.calls
    assert ("execute", None, key) in overview.calls


def test_save_tick_data_appends_last_price(database: DolphindbDatabase) -> None:
    symbol: str = "au2406"
    ticks: list[TickData] = _make_ticks(symbol)
    assert database.save_tick_data(ticks) is True
    assert [item.table for item in appenders] == ["tick", "tickoverview"]
    frame: pd.DataFrame = appenders[0].frames[0]
    assert frame["symbol"].tolist() == [symbol, symbol]
    assert frame["exchange"].tolist() == [Exchange.SHFE.value, Exchange.SHFE.value]
    assert frame["name"].tolist() == ["au", "au"]
    assert frame["last_price"].tolist() == [400.5, 401.0]
    assert frame["volume"].tolist() == [20.0, 21.0]
    assert frame["datetime"].tolist() == [
        np.datetime64(convert_tz(ticks[0].datetime)),
        np.datetime64(convert_tz(ticks[1].datetime)),
    ]
    assert int(appenders[1].frames[0]["count"].iloc[0]) == 2


def test_load_tick_data_filters_symbol_and_datetime_bounds(database: DolphindbDatabase) -> None:
    symbol: str = "au2406"
    start: datetime = datetime(2024, 1, 1)
    end: datetime = datetime(2024, 2, 1)
    loaded: list[TickData] = database.load_tick_data(symbol, Exchange.SHFE, start, end)
    assert loaded == []
    tick_table: _Table = [table for table in database.session.tables if table.name == "tick"][-1]
    assert tick_table.calls[-1][2] == [
        f'symbol="{symbol}"',
        f'exchange="{Exchange.SHFE.value}"',
        f"datetime>={_dotted(start)}",
        f"datetime<={_dotted(end)}",
    ]


def test_delete_tick_data_uses_symbol_and_exchange(database: DolphindbDatabase) -> None:
    symbol: str = "au2406"
    count: int = database.delete_tick_data(symbol, Exchange.SHFE)
    assert count == 2
    key: list[str] = [
        f'symbol="{symbol}"',
        f'exchange="{Exchange.SHFE.value}"',
    ]
    tick_table: _Table = [table for table in database.session.tables if table.name == "tick"][-1]
    overview: _Table = [table for table in database.session.tables if table.name == "tickoverview"][-1]
    assert ("select", "count(*)", key) in tick_table.calls
    assert ("execute", None, key) in tick_table.calls
    assert ("execute", None, key) in overview.calls
