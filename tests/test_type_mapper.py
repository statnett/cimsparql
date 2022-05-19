import datetime
import warnings
from decimal import Decimal

import pandas as pd
import pytest
from mock import MagicMock
from pandas.testing import assert_frame_equal

from cimsparql import type_mapper


@pytest.fixture
def mocked_graphdb(sparql_data_types, prefixes):
    cli = MagicMock()
    cli.get_table.return_value = sparql_data_types
    cli.configure_mock(prefixes=prefixes)
    return cli


@pytest.fixture
def type_mapper_instance(mocked_graphdb):
    return type_mapper.TypeMapper(mocked_graphdb)


def test_get_type(type_mapper_instance):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        identity_result = type_mapper_instance.get_type("missing_type")(123)
        missing_result = type_mapper_instance.get_type("missing_type", None)
    assert identity_result == 123
    assert missing_result is None
    assert len(w) == 2


def test_python_type_map_bool():
    assert type_mapper.XSD_TYPE_MAP["boolean"]("TRUE")
    assert not type_mapper.XSD_TYPE_MAP["boolean"]("FALSE")


def test_get_map_empty_pandas():
    cli = MagicMock()
    cli.get_table.return_value = pd.DataFrame()
    mapper = type_mapper.TypeMapper(cli)
    assert mapper.get_map(cli) == {}


def test_map_data_types(type_mapper_instance, type_dataframe, data_row, type_dataframe_ref):
    col_map = {
        column: data.get("datatype", data.get("type", None)) for column, data in data_row.items()
    }
    result = type_mapper_instance.map_data_types(type_dataframe, col_map)
    assert_frame_equal(result, type_dataframe_ref)


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
