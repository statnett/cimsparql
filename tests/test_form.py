import logging
import os
from typing import Dict

import pytest

from cimsparql.graphdb import GraphDBClient
from cimsparql.url import service

logger = logging.getLogger(__name__)

prefixes: Dict[str, str] = {
    "wgs": "http://www.w3.org/2003/01/geo/wgs84_pos",
    "owl": "http://www.w3.org/2002/07/owl",
    "cim": "http://iec.ch/TC57/2013/CIM-schema-cim16",
    "gn": "http://www.geonames.org/ontology",
    "xsd": "http://www.w3.org/2001/XMLSchema",
    "fn": "http://www.w3.org/2005/xpath-functions",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema",
    "ENTSOE": "http://entsoe.eu/Secretariat/ProfileExtension/1",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns",
    "md": "http://iec.ch/TC57/61970-552/ModelDescription/1",
    "sesame": "http://www.openrdf.org/schema/sesame",
    "SN": "http://www.statnett.no/CIM-schema-cim15-extension",
    "ALG": "http://www.alstom.com/grid/CIM-schema-cim15-extension",
}


@pytest.fixture
def gdb(monkeypatch) -> GraphDBClient:
    monkeypatch.setenv("GRAPHDB_USER", "abot")
    monkeypatch.setenv("GRAPHDB_USER_PASSWD", os.getenv("FORM_USER_PASSWD"))
    g = GraphDBClient(service(os.getenv("FORM_REPO"), os.getenv("FORM_SERVER")))
    for key, value in prefixes.items():
        g.prefixes[key] = value
    return g


def test_prefixes(gdb: GraphDBClient):
    logger.info(f"\n{gdb.prefixes}")


def test_bus_data_with_nodes(gdb: GraphDBClient):
    data = gdb.bus_data(with_dummy_buses=True, dry_run=False)
    logger.info(f"\n{data}")


def test_aclines_with_nodes(gdb: GraphDBClient):
    data = gdb.ac_lines(rates=("Normal",), nodes="node")
    logger.info(f"\n{data.set_index(['node_1','node_2','mrid'])}")


def test_transformer_with_nodes(gdb: GraphDBClient):
    winding = gdb.two_winding_transformers(nodes="node", rates=None)
    columns = ["node_1", "node_2", "name", "r", "x", "un"]
    logger.info(f"\n{winding}")
    logger.info(f"\n{winding.dtypes}")

    # columns = ["t_mrid_1", "t_mrid_2", "name"]
    logger.info(f"\n{winding[columns].to_string(index=False)}")


def test_three_winding_transformer_with_nodes(gdb: GraphDBClient):
    winding = gdb.three_winding_transformers(nodes="node", rates=["Normal"], dry_run=True)
    logger.info(f"\n{winding}")


def test_three_winding_transformer_with_nodes_sql(gdb: GraphDBClient):
    winding = gdb.three_winding_transformers(nodes="node", rates=["Normal"], dry_run=True)
    logger.info(f"\n{winding}")


def test_series_compensators_with_nodes(gdb: GraphDBClient):
    data = gdb.series_compensators(nodes="node")
    logger.info(f"\n{data.set_index(['node_1','node_2','mrid'])}")


def test_load_with_nodes(gdb: GraphDBClient):
    loads = gdb.loads(load_type=["ConformLoad", "NonConformLoad"], nodes="bus")
    logger.info(f"\n{loads}")


def test_gen_with_nodes(gdb: GraphDBClient):
    gen = gdb.synchronous_machines(nodes="bus")
    logger.info(f"\n{gen}")
