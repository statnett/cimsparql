import os
from string import Template

import pandas as pd
import pytest

import tests.t_utils.common as t_common
import tests.t_utils.entsoe_models as t_entsoe


def check_service_available(test_model: t_common.ModelTest):
    if not test_model.model:
        if os.getenv("CI") and test_model.must_run_in_ci:
            pytest.fail("Service should always be available in CI")
        else:
            pytest.skip("Require access to external Graph service")


@pytest.mark.slow
@pytest.mark.parametrize("test_model", t_entsoe.smallgrid_models())
def test_eq_query(test_model: t_common.ModelTest):
    check_service_available(test_model)
    query_template = Template(
        "PREFIX rdf:<${rdf}>\nPREFIX cim:<${cim}>\n"
        "SELECT ?voltage {?s rdf:type cim:BaseVoltage; cim:BaseVoltage.nominalVoltage ?voltage}"
    )

    model = test_model.model
    assert model
    query = model.template_to_query(query_template)
    voltage = model.get_table_and_convert(query)
    voltage = voltage.sort_values("voltage").reset_index(drop=True)
    expect = pd.DataFrame({"voltage": [33.0, 132.0, 220.0]})
    pd.testing.assert_frame_equal(voltage, expect)


@pytest.mark.slow
@pytest.mark.parametrize("test_model", t_entsoe.smallgrid_models())
def test_tpsv_query(test_model: t_common.ModelTest):
    check_service_available(test_model)
    model = test_model.model
    assert model
    repo = model.config.system_state_repo
    query_template = Template(
        "PREFIX rdf:<${rdf}>\nPREFIX cim:<${cim}>\n"
        f"select * where {{service <{repo}> {{?s rdf:type cim:TopologicalNode}}}}"
    )
    query = model.template_to_query(query_template)
    topological_nodes = model.get_table_and_convert(query)
    assert topological_nodes.shape == (115, 1)
