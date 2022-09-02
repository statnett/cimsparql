import os
from typing import Dict, Optional

import pandas as pd
import pytest

from cimsparql.model import CimModel


def check_service_available(model: Optional[CimModel]):
    if model is None and os.getenv("CI"):
        pytest.fail("Service should always be available in CI")
    elif model is None:
        pytest.skip("Require access to external Graph service")


@pytest.mark.slow
@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_eq_query(smallgrid_models: Dict[str, CimModel], server):
    model = smallgrid_models[server]
    check_service_available(model)

    query = "SELECT ?voltage {?s rdf:type cim:BaseVoltage; cim:BaseVoltage.nominalVoltage ?voltage}"
    df = model.get_table_and_convert(query).sort_values("voltage").reset_index(drop=True)
    expect = pd.DataFrame({"voltage": [33.0, 132.0, 220.0]})
    pd.testing.assert_frame_equal(df, expect)


@pytest.mark.slow
@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_tpsv_query(smallgrid_models, server):
    model = smallgrid_models[server]
    check_service_available(model)

    repo = model.config.system_state_repo
    query = f"select * where {{service <{repo}> {{?s rdf:type cim:TopologicalNode}}}}"
    df = model.get_table_and_convert(query)
    assert df.shape == (115, 1)
