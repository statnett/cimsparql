import os
from string import Template

import pytest

import tests.t_utils.common as t_common


@pytest.mark.parametrize(
    ("query", "expect"),
    [
        ("select ?name where {ex:Picasso foaf:firstName ?name}", [{"name": "Pablo"}]),
        (
            "select ?city {ex:Picasso ex:homeAddress ?address . ?address ex:city ?city}",
            [{"city": "Madrid"}],
        ),
    ],
)
def test_rdf4j_picasso_data(query: str, expect: list[dict[str, str]]):
    try:
        client = t_common.initialized_rdf4j_repo()
    except Exception as exc:
        if os.getenv("CI"):
            pytest.fail(f"{exc}")
        else:
            pytest.skip(f"{exc}")

    prefixes = Template("PREFIX ex:<${ex}>\nPREFIX foaf:<${foaf}>").substitute(client.prefixes)
    result = client.exec_query(f"{prefixes}\n{query}").results.values_as_dict()
    assert result == expect
