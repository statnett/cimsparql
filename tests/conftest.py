import logging
import pathlib
from collections.abc import Generator
from contextlib import suppress

import pytest
import t_utils.common as t_common
import t_utils.entsoe_models as t_entsoe

this_dir = pathlib.Path(__file__).parent

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def delete_models() -> Generator[None, None, None]:
    """
    Fixture for deleting micro model repos created in during tests
    """
    yield
    models = t_entsoe.micro_models() + t_entsoe.smallgrid_models()
    for test_model in filter(lambda tm: tm.model is not None and tm.cleanup, models):
        for client in test_model.model.distinct_clients:
            with suppress(Exception):
                client.delete_repo()


@pytest.fixture(scope="session", autouse=True)
def delete_picasso_repo() -> Generator[None, None, None]:
    yield
    with suppress(Exception):
        client = t_common.initialized_rdf4j_repo()
        client.delete_repo()
