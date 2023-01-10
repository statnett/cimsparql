import datetime
from decimal import Decimal
from typing import Optional

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cimsparql import type_mapper
from cimsparql.graphdb import GraphDBClient, default_namespaces


def apply_mp(monkeypatch, sparql_type_df: Optional[pd.DataFrame] = None):
    if sparql_type_df is None:
        sparql_type_df = pd.DataFrame()

    monkeypatch.setattr(GraphDBClient, "get_table", lambda *args: (sparql_type_df, {}))
    monkeypatch.setattr(GraphDBClient, "get_prefixes", lambda *args: default_namespaces())


def test_python_type_map_bool():
    assert type_mapper.XSD_TYPE_MAP["boolean"]("TRUE")
    assert not type_mapper.XSD_TYPE_MAP["boolean"]("FALSE")


def test_get_map_empty_pandas(monkeypatch):
    apply_mp(monkeypatch)
    mapper = type_mapper.TypeMapper()
    assert mapper.get_map() == {}


def test_map_data_types(monkeypatch):
    df = pd.DataFrame(
        {
            "sparql_type": ["http://c#Degrees", "http://c#Status", "http://c#Amount"],
            "range": ["http://x#Float", "http://x#Bool", "http://x#Integer"],
        }
    )
    apply_mp(monkeypatch, df)

    types = {"http://c#Degrees": float, "http://c#Status": bool, "http://c#Amount": int}
    tm = type_mapper.TypeMapper(custom_additions=types)

    df = pd.DataFrame(
        {
            "angle": ["1.0", "2.0", "3.0"],
            "active": ["true", "true", "false"],
            "number": ["1", "2", "3"],
        }
    )

    expect_df = df.astype({"angle": float, "active": bool, "number": int})

    col_map = {
        "angle": "http://c#Degrees",
        "active": "http://c#Status",
        "number": "http://c#Amount",
    }
    result = tm.map_data_types(df, col_map)

    assert_frame_equal(result, expect_df)


@pytest.mark.parametrize(
    "dtype,test",
    [
        ("boolean", ("true", True)),
        ("boolean", ("false", False)),
        ("boolean", ("1", True)),
        ("boolean", ("0", False)),
        ("date", ("2021-12-14", pd.Timestamp("2021-12-14 00:00:00"))),
        ("dateTime", ("2002-10-14T12:00:00", datetime.datetime(2002, 10, 14, 12, 0, 0))),
        ("dateTime", ("2002-10-14T12:00:00Z", pd.Timestamp("2002-10-14 12:00:00+0000", tz="UTC"))),
        ("duration", ("P365D", datetime.timedelta(365))),
        ("duration", ("-P365D", datetime.timedelta(-365))),
        (
            "duration",
            ("P3DT5H20M30.123S", datetime.timedelta(days=3, seconds=5 * 3600 + 20 * 60 + 30.123)),
        ),
        ("decimal", ("2.3", Decimal("2.3"))),
        ("time", ("21:32:52", pd.Timestamp("21:32:52"))),
        ("time", ("21:32:52Z", pd.Timestamp("21:32:52", tz="UTC"))),
    ],
)
def test_xsd_types(dtype: str, test: tuple):
    value, expect = test
    assert type_mapper.XSD_TYPE_MAP[dtype](value) == expect


@pytest.mark.parametrize(
    "cim_version, have_cim_version",
    [
        ("10", False),
        # Because of the default prefixes, 16 is default. Thus, it should be present
        ("16", True),
    ],
)
def test_have_cim_version(monkeypatch, cim_version: int, have_cim_version: bool):
    apply_mp(monkeypatch)
    tm = type_mapper.TypeMapper()
    assert tm.have_cim_version(cim_version) == have_cim_version
