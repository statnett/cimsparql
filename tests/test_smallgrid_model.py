import os
from string import Template
from typing import Dict, Optional

import pandas as pd
import pytest

from cimsparql.model import CimModel


def check_service_available(model: Optional[CimModel], server: str):
    if model is None:
        if os.getenv("CI") and server != "graphdb":
            pytest.fail("Service should always be available in CI")
        else:
            pytest.skip("Require access to external Graph service")


@pytest.mark.slow
@pytest.mark.asyncio
@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
async def test_eq_query(smallgrid_models: Dict[str, CimModel], server):
    model = smallgrid_models[server]
    check_service_available(model, server)
    query_template = Template(
        "PREFIX rdf:<${rdf}>\nPREFIX cim:<${cim}>\n"
        "SELECT ?voltage {?s rdf:type cim:BaseVoltage; cim:BaseVoltage.nominalVoltage ?voltage}"
    )
    query = model.template_to_query(query_template)
    df = await model.get_table_and_convert(query)
    df = df.sort_values("voltage").reset_index(drop=True)
    expect = pd.DataFrame({"voltage": [33.0, 132.0, 220.0]})
    pd.testing.assert_frame_equal(df, expect)


@pytest.mark.slow
@pytest.mark.asyncio
@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
async def test_tpsv_query(smallgrid_models, server):
    model = smallgrid_models[server]
    check_service_available(model, server)
    repo = model.config.system_state_repo
    query_template = Template(
        "PREFIX rdf:<${rdf}>\nPREFIX cim:<${cim}>\n"
        f"select * where {{service <{repo}> {{?s rdf:type cim:TopologicalNode}}}}"
    )
    query = model.template_to_query(query_template)
    df = await model.get_table_and_convert(query)
    assert df.shape == (115, 1)
