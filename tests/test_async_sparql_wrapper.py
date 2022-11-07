import os
from string import Template
from typing import Optional

import pytest

from cimsparql.graphdb import GraphDBClient, make_async


@pytest.mark.parametrize(
    "query, expect",
    [
        ("select ?name where {ex:Picasso foaf:firstName ?name}", [{"name": "Pablo"}]),
        (
            "select ?city {ex:Picasso ex:homeAddress ?address . ?address ex:city ?city}",
            [{"city": "Madrid"}],
        ),
    ],
)
@pytest.mark.asyncio
async def test_async_rdf4j_picasso_data(rdf4j_gdb: Optional[GraphDBClient], query, expect):
    if rdf4j_gdb is None and not os.getenv("CI"):
        pytest.skip("Require access to RDF4J service")

    prefixes = Template("PREFIX ex:<${ex}>\nPREFIX foaf:<${foaf}>").substitute(rdf4j_gdb.prefixes)
    client = make_async(rdf4j_gdb)
    result = await client.exec_query(f"{prefixes}\n{query}")
    assert result == expect
