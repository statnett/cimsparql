import warnings

import pandas as pd
import pytest
from mock import MagicMock, patch
from pandas.testing import assert_frame_equal

from cimsparql import type_mapper


@pytest.fixture
def mocked_graphdb(sparql_data_types):
    cli = MagicMock()
    cli.get_table.return_value = sparql_data_types
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
    assert type_mapper.python_type_map["boolean"]("TRUE")
    assert not type_mapper.python_type_map["boolean"]("FALSE")


@patch("cimsparql.type_mapper.TypeMapper.__init__", new=MagicMock(return_value=None))
def test_get_map_empty_pandas():
    cli = MagicMock()
    cli.get_table.return_value = pd.DataFrame()
    mapper = type_mapper.TypeMapper()
    assert mapper.get_map(cli) == {}


def test_map_data_types(type_mapper_instance, type_dataframe, data_row, type_dataframe_ref):
    columns = {"prefixed_col": lambda x: x.split("_")[-1]}
    entsoe_profile = "http://entsoe.eu/Secretariat/ProfileExtension/1"
    custom_maps = {f"{entsoe_profile}#AsynchronousMachine.converterFedDrive": lambda x: x == "True"}
    col_map = {
        column: data.get("datatype", data.get("type", None))
        for column, data in data_row.items()
        if column not in columns.keys()
    }
    result = type_mapper_instance.map_data_types(
        type_dataframe, col_map, custom_maps=custom_maps, columns=columns
    )
    assert_frame_equal(result, type_dataframe_ref)
