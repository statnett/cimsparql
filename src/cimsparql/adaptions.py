"""Adaption functionality used to modify test cases."""

from __future__ import annotations

import hashlib
import re
import uuid
from contextlib import suppress
from typing import TYPE_CHECKING

from rdflib import Graph
from rdflib.graph import Dataset
from rdflib.namespace import RDF, XSD
from rdflib.term import BNode, Literal, Node, URIRef

from cimsparql.graphdb import default_namespaces

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path


class XmlModelAdaptor:
    eq_predicate = "http://entsoe.eu/CIM/EquipmentCore/3/1"

    def __init__(self, filenames: Iterable[Path]) -> None:
        self.graph = Dataset()
        self.ns = default_namespaces()
        for prefix, value in self.ns.items():
            self.graph.bind(prefix, value)

        for filename in filenames:
            profile = filename.stem.rpartition("/")[-1]
            uri = URIRef(f"http://cimsparql/xml-adpator/{profile}")
            graph = Graph(identifier=uri).parse(filename, publicID="http://cim")
            self.graph.addN([(s, p, o, graph) for s, p, o in graph])

    def namespaces(self) -> dict[str, str]:
        return self.ns | {str(prefix): str(name) for prefix, name in self.graph.namespaces()}

    def graphs(self) -> set[str]:
        return {graph for _, _, _, graph in self.graph.quads() if graph}

    @classmethod
    def from_folder(cls, folder: Path) -> XmlModelAdaptor:
        return XmlModelAdaptor(list(folder.glob("*.xml")))

    def add_mrid(self) -> None:
        """Add cim:IdentifiedObject.mRID if not present."""
        ns = self.namespaces()
        identified_obj_mrid = URIRef(f"{ns['cim']}IdentifiedObject.mRID")
        for ctx in self.graph.contexts():
            subjects = {s for s, _, _ in ctx}
            for s in subjects:
                mrid_str = str(s).rpartition("#_")[-1]
                mrid = mrid_str if is_uuid(mrid_str) else generate_uuid(mrid_str)
                ctx.add((s, identified_obj_mrid, Literal(mrid)))

    def add_market_code_to_non_conform_load(self) -> None:
        updated = set[Node]()
        for load, _, _, ctx in self.graph.quads((None, RDF.type, URIRef(self.ns["cim"] + "EnergyConsumer"), None)):
            if load in updated:
                continue

            if "EQ" not in str(ctx):
                continue
            updated.add(load)
            assert ctx

            current_graph = self.graph.get_graph(ctx)
            assert current_graph

            load_group = BNode()
            schedule_resource = BNode()
            current_graph.addN(
                [
                    (load, URIRef(self.ns["cim"] + "NonConformLoad.LoadGroup"), load_group, current_graph),
                    (
                        load_group,
                        URIRef(self.ns["cim"] + "IdentifiedObject.name"),
                        Literal("created-group", datatype=XSD.string),
                        current_graph,
                    ),
                    (
                        load_group,
                        URIRef(self.ns["SN"] + "NonConformLoadGroup.ScheduleResource"),
                        schedule_resource,
                        current_graph,
                    ),
                    (
                        schedule_resource,
                        URIRef(self.ns["SN"] + "ScheduleResource.marketCode"),
                        Literal("market001", datatype=XSD.string),
                        current_graph,
                    ),
                ]
            )

    def adapt(self, eq_uri: str) -> None:
        self.add_zero_sv_power_flow()
        self.add_zero_sv_injection()
        self.add_generating_unit()
        self.add_market_code_to_non_conform_load()
        self.add_mrid()
        self.add_dtypes()
        self.add_internal_eq_link(eq_uri)
        self.add_eic_code()
        self.add_network_analysis_enable()

    def add_zero_sv_power_flow(self) -> None:
        sv_graph = Graph(identifier=URIRef("http://cimsparql/xml-adpator/SV"))
        sv_power_flow_terminal = URIRef(self.ns["cim"] + "SvPowerFlow.Terminal")

        zero_sv_flow_added = set()

        for terminal, _, _, _ in self.graph.quads((None, RDF.type, URIRef(self.ns["cim"] + "Terminal"), None)):
            # Do not add SvPowerFlow if it already exists
            if any(self.graph.quads((None, sv_power_flow_terminal, terminal, None))) or terminal in zero_sv_flow_added:
                continue
            zero_sv_flow_added.add(terminal)
            power_flow = BNode()
            sv_graph.addN(
                (
                    (power_flow, sv_power_flow_terminal, terminal, sv_graph),
                    (power_flow, URIRef(self.ns["cim"] + "SvPowerFlow.p"), Literal(0.0), sv_graph),
                    (power_flow, URIRef(self.ns["cim"] + "SvPowerFlow.q"), Literal(0.0), sv_graph),
                )
            )
        self.graph.add_graph(sv_graph)

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
            assert g
            current_graph = self.graph.get_graph(g)
            assert current_graph
            with suppress(StopIteration):
                f = next(f for f in fields if f in str(predicate))
                current_graph.remove((s, predicate, o))

                literal = Literal(str(o), datatype=fields[f])
                current_graph.add((s, predicate, literal))

    def tpsvssh_contexts(self) -> list[Graph]:
        return [ctx for ctx in self.graph.contexts() if any(token in str(ctx) for token in ("SSH", "TP", "SV"))]

    def eq_contexts(self) -> list[Graph]:
        return [ctx for ctx in self.graph.contexts() if any(token in str(ctx) for token in ("EQ", "GL"))]

    def nq_bytes(self, contexts: Iterable[Graph] | None = None) -> bytes:
        """Return the contexts as bytes. If contexts is None, the entire graph is exported."""
        if contexts is None:
            return self.graph.serialize(format="nquads", encoding="utf8")

        graph = Dataset()
        for ctx in contexts:
            graph.addN((s, p, o, ctx) for s, p, o in ctx)
        return graph.serialize(format="nquads", encoding="utf8")

    def add_internal_eq_link(self, eq_uri: str) -> None:
        # Insert in one SV graph
        ctx = next(c for c in self.tpsvssh_contexts() if "SV" in str(c))
        graph = self.graph.get_graph(ctx.identifier)
        assert graph
        graph.add((BNode(), URIRef(self.eq_predicate), URIRef(eq_uri)))

    def add_zero_sv_injection(self) -> None:
        tp_node_type = URIRef(self.ns["cim"] + "TopologicalNode")
        sv_inj_type = URIRef(self.ns["cim"] + "SvInjection")

        graph = Graph(identifier=URIRef("http://cimsparql/xml-adpator/SV-injection"))
        used_nodes = set()
        for s, _, _, _ in self.graph.quads((None, RDF.type, tp_node_type, None)):
            if s in used_nodes:
                continue
            used_nodes.add(s)
            sv_injection = BNode()
            graph.addN(
                (
                    (sv_injection, RDF.type, sv_inj_type, graph),
                    (sv_injection, URIRef(self.ns["cim"] + "SvInjection.TopologicalNode"), s, graph),
                    (
                        sv_injection,
                        URIRef(self.ns["cim"] + "SvInjection.pInjection"),
                        Literal(0.0, datatype=XSD.float),
                        graph,
                    ),
                )
            )
        self.graph.add_graph(graph)

    def add_eic_code(self) -> None:
        for substation, _, _, ctx in self.graph.quads((None, RDF.type, URIRef(self.ns["cim"] + "Substation"), None)):
            assert ctx
            current_graph = self.graph.get_graph(ctx)
            assert current_graph
            market_delivery_point = BNode()
            bidding_area = BNode()
            eic_code = Literal("10Y1001A1001A48H", datatype=XSD.string)
            current_graph.addN(
                (
                    (
                        substation,
                        URIRef(f"{self.ns['SN']}Substation.MarketDeliveryPoint"),
                        market_delivery_point,
                        current_graph,
                    ),
                    (
                        market_delivery_point,
                        URIRef(f"{self.ns['SN']}MarketDeliveryPoint.BiddingArea"),
                        bidding_area,
                        current_graph,
                    ),
                    (
                        bidding_area,
                        URIRef(f"{self.ns['entsoeSecretariat']}IdentifiedObject.energyIdentCodeEIC"),
                        eic_code,
                        current_graph,
                    ),
                )
            )

    def add_network_analysis_enable(self) -> None:
        used_equipment = set()
        for _, _, equipment, ctx in self.graph.quads(
            (None, URIRef(f"{self.ns['cim']}Terminal.ConductingEquipment"), None, None)
        ):
            if equipment in used_equipment:
                continue
            used_equipment.add(equipment)
            assert ctx
            graph = self.graph.get_graph(ctx)
            assert graph
            graph.add(
                (
                    equipment,
                    URIRef(f"{self.ns['SN']}Equipment.networkAnalysisEnable"),
                    Literal("true", datatype=XSD.boolean),
                ),
            )

    def add_generating_unit(self) -> None:
        updated = set()
        for sync_machine, _, _, ctx in self.graph.quads(
            (None, RDF.type, URIRef(self.ns["cim"] + "SynchronousMachine"), None)
        ):
            assert ctx
            if "EQ" not in str(ctx):
                continue
            if sync_machine in updated:
                continue

            generating_unit = BNode()
            schedule_resource = BNode()
            current_graph = self.graph.get_graph(ctx)
            assert current_graph
            updated.add(sync_machine)
            current_graph.addN(
                (
                    (
                        sync_machine,
                        URIRef(self.ns["cim"] + "SynchronousMachine.GeneratingUnit"),
                        generating_unit,
                        current_graph,
                    ),
                    (generating_unit, RDF.type, URIRef(self.ns["cim"] + "ThermalGeneratingUnit"), current_graph),
                    (
                        generating_unit,
                        URIRef(self.ns["cim"] + "IdentifiedObject.name"),
                        Literal("GeneratingUnit", datatype=XSD.string),
                        current_graph,
                    ),
                    (
                        generating_unit,
                        URIRef(self.ns["SN"] + "GeneratingUnit.ScheduleResource"),
                        schedule_resource,
                        current_graph,
                    ),
                    (schedule_resource, RDF.type, URIRef(self.ns["SN"] + "ScheduleResource"), current_graph),
                    (
                        schedule_resource,
                        URIRef(self.ns["SN"] + "ScheduleResource.marketCode"),
                        Literal("market001", datatype=XSD.string),
                        current_graph,
                    ),
                )
            )


def is_uuid(x: str) -> bool:
    return re.match("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", x) is not None


def generate_uuid(x: str) -> str:
    h = hashlib.md5(x.encode(), usedforsecurity=False)
    return str(uuid.UUID(hex=h.hexdigest()))
