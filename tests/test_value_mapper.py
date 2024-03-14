from copy import deepcopy
from string import Template

import pandas as pd
import t_utils.common as t_common
import t_utils.entsoe_models as t_entsoe

from cimsparql.value_mapper import MridMapper

subj_template = Template(
    """
prefix cim:<${cim}>
select (?terminal as ?t_mrid) ?connected
where {
    ?terminal cim:ACDCTerminal.connected ?connected
}
"""
)

mrid_template = Template(
    """
prefix cim:<${cim}>
select ?t_mrid ?connected where
{
    ?terminal cim:ACDCTerminal.connected ?connected;
        cim:IdentifiedObject.mRID ?t_mrid
}
"""
)


def test_subj_conversion():
    tm = t_entsoe.micro_t1_nl()
    t_common.check_model(tm)
    model = tm.model
    model2 = deepcopy(model)
    model2.config.value_mappers = [MridMapper()]

    subj_query = model.template_to_query(subj_template)
    mrid_query = model.template_to_query(mrid_template)

    dfs = [model2.get_table_and_convert(subj_query), model.get_table_and_convert(mrid_query)]

    for i, df in enumerate(dfs):
        dfs[i] = df.sort_values("t_mrid").reset_index(drop=True)
    pd.testing.assert_frame_equal(dfs[0], dfs[1])
