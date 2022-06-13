from typing import Dict, Optional, Union

import pytest

from cimsparql import query_support as qs

cim_version = 15


@pytest.fixture(scope="module")
def region_kwargs() -> Dict[str, Union[bool, str]]:
    return {"sub_region": False, "container": ""}


def test_region_query_empty(region_kwargs: Dict[str, Union[bool, str]]):
    assert not qs.region_query(region=None, **region_kwargs)


def test_region_query(region_kwargs: Dict[str, Union[bool, str]]):
    regions = qs.region_query(region="NO", **region_kwargs)
    assert len(regions) == 3


@pytest.fixture(scope="module")
def terminal_kwargs() -> Dict[str, Optional[Union[str]]]:
    return {"node": None, "mrid_subject": "?_mrid"}


def test_default_terminal_where_query(terminal_kwargs: Dict[str, Optional[str]]):
    assert len(qs.terminal_where_query(cim_version, con="con", **terminal_kwargs)) == 3


def test_terminal_where_query_no_var(terminal_kwargs: Dict[str, Optional[str]]):
    assert len(qs.terminal_where_query(cim_version, con=None, **terminal_kwargs)) == 2


def test_terminal_where_query_no_var_with_sequence(terminal_kwargs: Dict[str, Optional[str]]):
    assert (
        len(
            qs.terminal_where_query(
                cim_version, con=None, with_sequence_number=True, **terminal_kwargs
            )
        )
        == 3
    )
