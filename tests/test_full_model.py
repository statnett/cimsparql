import os

import pytest

from cimsparql.model import SingleClientModel


@pytest.mark.asyncio
@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER", None) is None, reason="Need graphdb server to run")
async def test_full_model_1(model: SingleClientModel):
    full_model = await model.full_model()
    assert len(full_model) == 3
    assert len(full_model["time"].unique()) == 1
    assert (full_model["description"] == "statnett").all()
