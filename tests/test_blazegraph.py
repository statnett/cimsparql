import os

import pytest


def test_blazegraph(micro_t1_nl_bg):
    if micro_t1_nl_bg is None and not os.getenv("CI"):
        pytest.skip("Require blazegraph")
    res = micro_t1_nl_bg.client.exec_query("SELECT * WHERE {?s ?p ?o} limit 10")
    assert len(res) == 10
