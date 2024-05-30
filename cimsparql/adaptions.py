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
from rdflib.term import BNode, Literal, URIRef

if TYPE_CHECKING:
    from collections.abc import Iterable

    from rdflib import Graph


class XmlModelAdaptor:
    eq_predicate = "http://entsoe.eu/CIM/EquipmentCore/3/1"

    def __init__(self, filenames: Iterable[Path]) -> None:
        self.graph = ConjunctiveGraph()
        for filename in filenames:
            profile = filename.stem.rpartition("/")[-1]
            uri = URIRef(f"http://cimsparql/xml-adpator/{profile}")
            destination_graph = self.graph.get_context(uri)
            destination_graph.parse(filename, publicID="http://cim")

    def namespaces(self) -> dict[str, str]:
        return {str(prefix): str(name) for prefix, name in self.graph.namespaces()}

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
        for result in self.graph.query(
            "select ?s ?g where {graph ?g {?s cim:IdentifiedObject.name ?name}}", initNs=ns
        ):
            mrid_str = str(result["s"]).rpartition("#_")[-1]
            mrid = mrid_str if is_uuid(mrid_str) else generate_uuid(mrid_str)
            self.graph.add((result["s"], identified_obj_mrid, Literal(mrid), result["g"]))

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
        self.add_mrid()
        self.add_dtypes()
        self.set_generation_type()
        self.add_internal_eq_link(eq_uri)

    def add_zero_sv_power_flow(self) -> None:
        with open(
            Path(__file__).parent
            / "sparql/test_configuration_modifications/add_zero_sv_power.sparql"
        ) as f:
            query = Template(f.read())

        prepared_update_query = prepareUpdate(query.substitute(self.namespaces()))
        self.graph.update(prepared_update_query)

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
                f = next(f for f in fields if f in predicate)
                self.graph.remove((s, predicate, o, g))

                literal = Literal(str(o), datatype=fields[f])
                self.graph.add((s, predicate, literal, g))

    def tpsvssh_contexts(self) -> list[URIRef]:
        return [
            ctx
            for ctx in self.graph.contexts()
            if any(token in str(ctx) for token in ("SSH", "TP", "SV"))
        ]

    def nq_bytes(self, contexts: list[URIRef] | None = None) -> bytes:
        """
        Return the contexts as bytes. If contexts is None, the entire graph
        is exported
        """
        if contexts is None:
            return self.graph.serialize(format="nquads", encoding="utf8")

        graph = ConjunctiveGraph()
        for ctx in contexts:
            graph += self.graph.get_context(ctx.identifier)
        return graph.serialize(format="nquads", encoding="utf8")

    def add_internal_eq_link(self, eq_uri: str) -> None:
        # Insert in one SV graph
        ctx = next(c for c in self.tpsvssh_contexts() if "SV" in str(c))
        self.graph.get_context(ctx.identifier).add(
            (BNode(), URIRef(self.eq_predicate), URIRef(eq_uri))
        )

    def add_zero_sv_injection(self) -> None:
        with open(
            Path(__file__).parent
            / "sparql/test_configuration_modifications/add_sv_injection.sparql"
        ) as f:
            query = Template(f.read())

        prepared_update_query = prepareUpdate(query.substitute(self.namespaces()))
        self.graph.update(prepared_update_query)


def is_uuid(x: str) -> bool:
    return re.match("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", x) is not None


def generate_uuid(x: str) -> str:
    h = hashlib.md5(x.encode(), usedforsecurity=False)
    return str(uuid.UUID(hex=h.hexdigest()))
