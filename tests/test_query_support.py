from typing import Dict, Union

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
