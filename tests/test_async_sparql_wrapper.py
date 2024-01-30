import asyncio
import os
from string import Template

import pytest
import t_utils.common as t_common

from cimsparql.async_sparql_wrapper import retry_task
from cimsparql.graphdb import make_async


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
async def test_async_rdf4j_picasso_data(query: str, expect: list[dict[str, str]]):
    try:
        client = t_common.initialized_rdf4j_repo()
    except Exception as exc:
        if os.getenv("CI"):
            pytest.fail(f"{exc}")
        else:
            pytest.skip(f"{exc}")

    prefixes = Template("PREFIX ex:<${ex}>\nPREFIX foaf:<${foaf}>").substitute(client.prefixes)
    client = make_async(client)
    result = await client.exec_query(f"{prefixes}\n{query}")
    assert result == expect


@pytest.mark.asyncio
async def test_return_immediately_on_cancel():
    async def task() -> None:
        await asyncio.sleep(10.0)
        raise RuntimeError("This task always fails")

    retry_10_times_task = asyncio.create_task(retry_task(lambda: task(), 10, 2))
    retry_10_times_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await retry_10_times_task
