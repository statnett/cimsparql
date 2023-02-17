import datetime
from decimal import Decimal
from typing import Optional

import numpy as np
import pandas as pd
import pandera as pa
import pytest
from pandas.testing import assert_frame_equal

from cimsparql import type_mapper
from cimsparql.data_models import JsonSchemaOut
from cimsparql.graphdb import RestApi
from cimsparql.model import ServiceConfig


def init_triple_store_server(httpserver, sparql_result: Optional[dict] = None) -> ServiceConfig:
    """
    Create a triple store server that returns sparql_result_data when a call is made
    """
    sparql_result = sparql_result or empty_sparql_result()
    httpserver.expect_request("/sparql").respond_with_json(sparql_result)
    return ServiceConfig(
        server=httpserver.url_for("/sparql"), rest_api=RestApi.DIRECT_SPARQL_ENDPOINT
    )


def empty_sparql_result():
    return {"head": {"vars": []}, "results": {"bindings": []}}


def test_python_type_map_bool():
    assert type_mapper.XSD_TYPE_MAP["boolean"]("TRUE")
    assert not type_mapper.XSD_TYPE_MAP["boolean"]("FALSE")


def test_get_map_empty_pandas(httpserver):
    service_cfg = init_triple_store_server(httpserver)
    mapper = type_mapper.TypeMapper(service_cfg)
    assert mapper.get_map() == {}


def test_map_data_types(httpserver):
    results = {
        "sparql_type": ["http://c#Degrees", "http://c#Status", "http://c#Amount"],
        "range": ["http://x#Float", "http://x#Bool", "http://x#Integer"],
    }

    # Represent result as a sparql_result
    sparql_result = {
        "head": {"vars": list(results.keys())},
        "results": {
            "bindings": [
                {k: {"type": "literal", "value": v[i]} for k, v in results.items()}
                for i in range(3)
            ]
        },
    }
    service_cfg = init_triple_store_server(httpserver, sparql_result)

    types = {"http://c#Degrees": float, "http://c#Status": bool, "http://c#Amount": int}
    tm = type_mapper.TypeMapper(service_cfg, custom_additions=types)

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
def test_have_cim_version(httpserver, cim_version: int, have_cim_version: bool):
    service_cfg = init_triple_store_server(httpserver)
    tm = type_mapper.TypeMapper(service_cfg)
    assert tm.have_cim_version(cim_version) == have_cim_version


class FloatSchema(JsonSchemaOut):
    float_col: pa.typing.Series[float] = pa.Field(nullable=True)


@pytest.mark.parametrize(
    "in_data, out_data",
    [
        ({"float_col": ["1.0", "2.0"]}, {"float_col": [1.0, 2.0]}),
        ({"float_col": ["1.0", None]}, {"float_col": [1.0, np.nan]}),
    ],
)
def test_coerce_missing_type(httpserver, in_data: dict, out_data: dict):
    client = init_triple_store_server(httpserver)
    in_df, out_df = pd.DataFrame(in_data), pd.DataFrame(out_data)
    mapper = type_mapper.TypeMapper(client)

    col_map = {"float_col": "http://non-existent#type"}
    df = FloatSchema(mapper.map_data_types(in_df, col_map))
    pd.testing.assert_frame_equal(df, out_df)
