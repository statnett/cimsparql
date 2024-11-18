import numpy as np
import pandas as pd

from cimsparql.data_models import PhaseTapChangerDataFrame, PhaseTapChangerSchema


def test_empty_phase_tap_changer():
    schema = PhaseTapChangerSchema.to_schema()
    columns = list(schema.columns.keys())
    df = pd.DataFrame([], columns=columns)

    validated_df = PhaseTapChangerDataFrame(df)

    # Check that the target value is correctly casted from object to np.float64
    assert validated_df.dtypes.to_dict()["target_value"] == np.float64
