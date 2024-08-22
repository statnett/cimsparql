import pytest

import tests.t_utils.custom_models as t_custom


def test_full_model_1():
    model = t_custom.federated_model().model
    if not model:
        pytest.skip("Require access to GRAPPHDB")
    full_model = model.full_model()
    assert len(full_model) == 3
    assert full_model["time"].unique().size == 1
