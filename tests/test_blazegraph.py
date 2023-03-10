import os

import pytest
import t_utils.entsoe_models as t_entsoe


def test_blazegraph():
    model = t_entsoe.micro_t1_nl_bg().model
    if not (model or os.getenv("CI")):
        pytest.skip("Require blazegraph")
    res = model.client.exec_query("SELECT * WHERE {?s ?p ?o} limit 10")
    assert len(res) == 10
