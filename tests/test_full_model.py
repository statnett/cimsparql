import pytest
import t_utils.custom_models as t_custom


@pytest.mark.asyncio
async def test_full_model_1():
    model = t_custom.combined_model().model
    if not model:
        pytest.skip("Require access to GRAPPHDB")
    full_model = await model.full_model()
    assert len(full_model) == 3
    assert len(full_model["time"].unique()) == 1
