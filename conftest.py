import os
import pytest
import pathlib
import pandas as pd

from cimsparql.graphdb import GraphDBClient
from cimsparql.redland import Model
from cimsparql.url import service, GraphDbConfig

this_dir = pathlib.Path(__file__).parent

ssh_repo = "20190522T0730Z"
eq_repo = "20190521T0030Z"

cim_date = "20190522_070618"


def local_server():
    return os.getenv("GRAPHDB_LOCAL_TEST_SERVER", "127.0.0.1:7200")


local_graphdb = GraphDbConfig(local_server(), protocol="http")


need_local_graphdb_ssh = pytest.mark.skipif(
    ssh_repo not in local_graphdb.repos, reason=f"Need {ssh_repo} in local repository"
)

need_local_graphdb_eq = pytest.mark.skipif(
    eq_repo not in local_graphdb.repos, reason=f"Need {eq_repo} in local repository"
)

need_local_graphdb_cim = pytest.mark.skipif(
    cim_date not in local_graphdb.repos, reason=f"Need {cim_date} in local repository"
)


@pytest.fixture(scope="session")
def local_graphdb_config():
    return local_graphdb


@pytest.fixture(scope="session")
def gcli_eq():
    return GraphDBClient(service=service(server=local_server(), repo=eq_repo, protocol="http"))


@pytest.fixture(scope="session")
def gcli_ssh():
    return GraphDBClient(service=service(server=local_server(), repo=ssh_repo, protocol="http"))


@pytest.fixture(scope="session")
def gcli_cim():
    return GraphDBClient(service=service(server=local_server(), repo=cim_date, protocol="http"))


@pytest.fixture(scope="session")
def ieee118():
    return Model(pathlib.Path(this_dir / "data" / f"IEEE118.xml"), base_uri="ieee118_case")


@pytest.fixture(scope="session")
def root_dir():
    return this_dir


@pytest.fixture(scope="session")
def graphdb_repo():
    return os.getenv("GRAPHDB_REPO", "SNMST-MasterCim15-VERSION-LATEST")


@pytest.fixture(scope="session")
def gdb_cli(graphdb_repo):
    return GraphDBClient(service(repo=graphdb_repo))


@pytest.fixture
def type_dataframe():
    return pd.DataFrame(
        {
            "str_col": ["a", "b", "c"],
            "int_col": [1.0, 2.0, 3.0],
            "float_col": ["2.2", "3.3", "4.4"],
            "prefixed_col": ["prefix_a", "prefix_b", "prefix_c"],
            "boolean_col": ["True", "True", "False"],
        }
    )


@pytest.fixture
def type_dataframe_ref():
    return pd.DataFrame(
        {
            "str_col": ["a", "b", "c"],
            "int_col": [1, 2, 3],
            "float_col": [2.2, 3.3, 4.4],
            "prefixed_col": ["a", "b", "c"],
            "boolean_col": [True, True, False],
        }
    ).astype({"int_col": int})


@pytest.fixture
def sparql_data_types():
    return pd.DataFrame(
        {
            "sparql_type": [
                "http://www.alstom.com/grid/CIM-schema-cim15-extension#Stage.priority",
                "http://iec.ch/TC57/2010/CIM-schema-cim15#PerCent",
            ],
            "type": ["Integer", "float"],
            "prefix": [None, None],
        }
    )


@pytest.fixture
def data_row():
    return {
        "str_col": {"type": "literal", "value": "a"},
        "int_col": {
            "datatype": "http://www.alstom.com/grid/CIM-schema-cim15-extension#Stage.priority",
            "type": "literal",
            "value": "1",
        },
        "float_col": {
            "datatype": "http://iec.ch/TC57/2010/CIM-schema-cim15#PerCent",
            "type": "literal",
            "value": "2.2",
        },
        "prefixed_col": {"type": "uri", "value": "prefixed_a"},
        "boolean_col": {
            "datatype": "http://entsoe.eu/Secretariat/ProfileExtension/1#AsynchronousMachine.converterFedDrive"
        },
    }
