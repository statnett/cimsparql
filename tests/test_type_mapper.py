import pytest
from cimsparql import type_mapper
from mock import Mock
from pandas.testing import assert_frame_equal
import warnings


@pytest.fixture
def mocked_graphdb(sparql_data_types):
    cli = Mock()
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


def test_map_data_types(type_mapper_instance, type_dataframe, data_row, type_dataframe_ref):
    columns = {"prefixed_col": lambda x: x.split("_")[-1]}
    entsoe_profile = "http://entsoe.eu/Secretariat/ProfileExtension/1"
    custom_maps = {f"{entsoe_profile}#AsynchronousMachine.converterFedDrive": lambda x: x == "True"}
    result = type_mapper_instance.map_data_types(
        type_dataframe, data_row, custom_maps=custom_maps, columns=columns
    )
    assert_frame_equal(result, type_dataframe_ref)
