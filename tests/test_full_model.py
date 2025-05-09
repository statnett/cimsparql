import pytest

import tests.t_utils.custom_models as t_custom


def test_full_model_1():
    model = t_custom.federated_model().model
    if not model:
        pytest.skip("Require access to GRAPPHDB")
        return
    full_model = model.full_model()
    assert len(full_model) == 4
    assert full_model[~full_model["profile"].str.contains("Equipment")]["time"].unique().size == 1
