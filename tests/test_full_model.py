import os

import pytest

from cimsparql.model import CimModel


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER", None) is None, reason="Need graphdb server to run")
def test_full_model_1(model: CimModel):
    full_model = model.full_model
    assert len(full_model) == 3
    assert len(full_model["time"].unique()) == 1
    assert (full_model["description"] == "statnett").all()
