import numpy as np

from cimsparql.data_models import PhaseTapChangerDataFrame, PhaseTapChangerSchema


def test_empty_phase_tap_changer():
    schema = PhaseTapChangerSchema.to_schema()
    columns = list(schema.columns.keys())
    phase_tap_changer = PhaseTapChangerDataFrame(columns=columns)

    # Check that the target value is correctly casted from object to np.float64
    assert phase_tap_changer.dtypes.to_dict()["target_value"] == np.float64
