from __future__ import annotations

import hashlib
import re
import uuid
from contextlib import suppress
from pathlib import Path
from string import Template
from typing import TYPE_CHECKING

from rdflib import ConjunctiveGraph
from rdflib.namespace import XSD
from rdflib.plugins.sparql import prepareUpdate
from rdflib.query import ResultRow
from rdflib.term import BNode, Literal, URIRef

from cimsparql.graphdb import default_namespaces

if TYPE_CHECKING:
    from collections.abc import Iterable

    from rdflib import Graph


class XmlModelAdaptor:
    eq_predicate = "http://entsoe.eu/CIM/EquipmentCore/3/1"

    def __init__(self, filenames: Iterable[Path], sparql_folder: Path | None = None) -> None:
        self.graph = ConjunctiveGraph()
        for filename in filenames:
            profile = filename.stem.rpartition("/")[-1]
            uri = URIRef(f"http://cimsparql/xml-adpator/{profile}")
            destination_graph = self.graph.get_context(uri)
            destination_graph.parse(filename, publicID="http://cim")
        self.ns = default_namespaces()
        self.sparql_folder = sparql_folder or Path(__file__).parent / "sparql" / "test_configuration_modifications"

    def namespaces(self) -> dict[str, str]:
        return self.ns | {str(prefix): str(name) for prefix, name in self.graph.namespaces()}

    def graphs(self) -> set[Graph]:
        return {graph for _, _, _, graph in self.graph.quads() if graph}

    @classmethod
    def from_folder(cls, folder: Path) -> XmlModelAdaptor:
        return XmlModelAdaptor(list(folder.glob("*.xml")))

    def add_mrid(self) -> None:
        """
        Adds cim:IdentifiedObject.mRID if not present
        """
        ns = self.namespaces()
        identified_obj_mrid = URIRef(f"{ns['cim']}IdentifiedObject.mRID")
        for result in self.graph.query("select ?s ?g where {graph ?g {?s cim:IdentifiedObject.name ?name}}", initNs=ns):
            assert isinstance(result, ResultRow)
            mrid_str = str(result["s"]).rpartition("#_")[-1]
            mrid = mrid_str if is_uuid(mrid_str) else generate_uuid(mrid_str)

            ctx = self.graph.get_context(result["g"])
            self.graph.add((result["s"], identified_obj_mrid, Literal(mrid), ctx))

    def set_generation_type(self) -> None:
        self.graph.update(
            """
            delete {?gen_unit a ?gen_unit_type}
            insert {?gen_unit a cim:ThermalGeneratingUnit}
            where {
              values ?gen_unit_type {cim:GeneratingUnit}
              ?gen_unit a ?gen_unit_type
            }
            """
        )

    def adapt(self, eq_uri: str) -> None:
        self.add_zero_sv_power_flow()
        self.add_zero_sv_injection()
        self.add_generating_unit()
        self.add_mrid()
        self.add_dtypes()
        self.set_generation_type()
        self.add_internal_eq_link(eq_uri)
        self.add_eic_code()
        self.add_network_analysis_enable()

    def update_graph(self, filename: str) -> None:
        """Update graph file sparql query."""
        with (self.sparql_folder / filename).open() as f:
            query = Template(f.read())

        prepared_update_query = prepareUpdate(query.substitute(self.namespaces()))
        self.graph.update(prepared_update_query)

    def add_zero_sv_power_flow(self) -> None:
        self.update_graph("add_zero_sv_power.sparql")

    def add_dtypes(self) -> None:
        fields = {
            "endNumber": XSD.integer,
            "sequenceNumber": XSD.integer,
            "phaseAngleClock": XSD.integer,
            "SvPowerFlow.p": XSD.float,
            ".open": XSD.boolean,
            ".connected": XSD.boolean,
            ".nominalVoltage": XSD.float,
        }
        for s, predicate, o, g in self.graph.quads():
            with suppress(StopIteration):
                f = next(f for f in fields if f in str(predicate))
                self.graph.remove((s, predicate, o, g))

                literal = Literal(str(o), datatype=fields[f])
                self.graph.add((s, predicate, literal, g))

    def tpsvssh_contexts(self) -> list[Graph]:
        return [ctx for ctx in self.graph.contexts() if any(token in str(ctx) for token in ("SSH", "TP", "SV"))]

    def nq_bytes(self, contexts: list[Graph] | None = None) -> bytes:
        """
        Return the contexts as bytes. If contexts is None, the entire graph
        is exported
        """
        if contexts is None:
            return self.graph.serialize(format="nquads", encoding="utf8")

        graph = ConjunctiveGraph()
        for ctx in contexts:
            graph += ctx
        return graph.serialize(format="nquads", encoding="utf8")

    def add_internal_eq_link(self, eq_uri: str) -> None:
        # Insert in one SV graph
        ctx = next(c for c in self.tpsvssh_contexts() if "SV" in str(c))
        self.graph.get_context(ctx.identifier).add((BNode(), URIRef(self.eq_predicate), URIRef(eq_uri)))

    def add_zero_sv_injection(self) -> None:
        self.update_graph("add_sv_injection.sparql")

    def add_eic_code(self) -> None:
        self.update_graph("add_eic_bidding_area_code.sparql")

    def add_network_analysis_enable(self) -> None:
        self.update_graph("add_network_analysis_enable.sparql")

    def add_generating_unit(self) -> None:
        self.update_graph("add_gen_unit_mrid.sparql")


def is_uuid(x: str) -> bool:
    return re.match("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", x) is not None


def generate_uuid(x: str) -> str:
    h = hashlib.md5(x.encode(), usedforsecurity=False)
    return str(uuid.UUID(hex=h.hexdigest()))
