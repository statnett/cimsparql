"""Adaption functionality used to modify test cases."""

from __future__ import annotations

import hashlib
import re
import uuid
from contextlib import suppress
from pathlib import Path
from string import Template
from typing import TYPE_CHECKING

from rdflib import Graph
from rdflib.graph import Dataset
from rdflib.namespace import RDF, XSD
from rdflib.plugins.sparql import prepareUpdate
from rdflib.query import ResultRow
from rdflib.term import BNode, Literal, URIRef

from cimsparql.graphdb import default_namespaces

if TYPE_CHECKING:
    from collections.abc import Iterable


class XmlModelAdaptor:
    eq_predicate = "http://entsoe.eu/CIM/EquipmentCore/3/1"

    def __init__(self, filenames: Iterable[Path], sparql_folder: Path | None = None) -> None:
        self.graph = Dataset()

        for filename in filenames:
            profile = filename.stem.rpartition("/")[-1]
            uri = URIRef(f"http://cimsparql/xml-adpator/{profile}")
            graph = Graph(identifier=uri).parse(filename, publicID="http://entsoe.eu/mico-model")
            self.graph.addN([(s, p, o, uri) for s, p, o in graph])
        self.ns = default_namespaces()
        for prefix, value in self.ns.items():
            self.graph.bind(prefix, value)
        self.sparql_folder = sparql_folder or Path(__file__).parent / "sparql" / "test_configuration_modifications"

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
        new_quads = []
        for result in self.graph.query("select ?s ?g where {graph ?g {?s cim:IdentifiedObject.name ?name}}", initNs=ns):
            assert isinstance(result, ResultRow)
            mrid_str = str(result["s"]).rpartition("#_")[-1]
            mrid = mrid_str if is_uuid(mrid_str) else generate_uuid(mrid_str)

            ctx = self.graph.get_context(result["g"]).identifier
            new_quads.append((result["s"], identified_obj_mrid, Literal(mrid), ctx))
        self.graph.addN(new_quads)

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

    def add_market_code_to_non_conform_load(self) -> None:
        updated = set()
        for load, _, _, ctx in self.graph.quads((None, RDF.type, URIRef(self.ns["cim"] + "EnergyConsumer"), None)):
            if load in updated:
                continue
            updated.add(load)
            assert ctx
            load_group = BNode()
            schedule_resource = BNode()
            self.graph.addN(
                (
                    (load, URIRef(self.ns["cim"] + "NonConformLoad.LoadGroup"), load_group, ctx),
                    (
                        load_group,
                        URIRef(self.ns["cim"] + "IdentifiedObject.name"),
                        Literal("created-group", datatype=XSD.string),
                        ctx,
                    ),
                    (
                        load_group,
                        URIRef(self.ns["SN"] + "NonConformLoadGroup.ScheduleResource"),
                        schedule_resource,
                        ctx,
                    ),
                    (
                        schedule_resource,
                        URIRef(self.ns["SN"] + "ScheduleResource.marketCode"),
                        Literal("market001", datatype=XSD.string),
                        ctx,
                    ),
                )
            )

    def adapt(self, eq_uri: str) -> None:
        self.add_zero_sv_power_flow()
        self.add_zero_sv_injection()
        self.add_generating_unit()
        self.add_market_code_to_non_conform_load()
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
        for terminal, _, _, _ in self.graph.quads((None, RDF.type, URIRef(self.ns["cim"] + "Terminal"), None)):
            # Do not add SvPowerFlow if it already exists
            if any(self.graph.quads((None, URIRef(self.ns["cim"] + "SvPowerFlow.Terminal"), terminal, None))):
                continue

            ctx = URIRef("http://cimsparql/xml-adpator/SV")
            power_flow = BNode()
            self.graph.addN(
                (
                    (power_flow, URIRef(self.ns["cim"] + "SvPowerFlow.Terminal"), terminal, ctx),
                    (power_flow, URIRef(self.ns["cim"] + "SvPowerFlow.p"), Literal(0.0), ctx),
                    (power_flow, URIRef(self.ns["cim"] + "SvPowerFlow.q"), Literal(0.0), ctx),
                )
            )

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

    def nq_bytes(self, contexts: Iterable[Graph] | None = None) -> bytes:
        """Return the contexts as bytes. If contexts is None, the entire graph is exported."""
        if contexts is None:
            return self.graph.serialize(format="nquads", encoding="utf8")

        graph = Dataset()
        for ctx in contexts:
            graph.addN((s, p, o, ctx.identifier) for s, p, o in ctx)
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
        for s, _, _, __ in self.graph.quads((None, RDF.type, tp_node_type, None)):
            ctx = URIRef("http://cimsparql/xml-adpator/SV")
            sv_injection = BNode()
            self.graph.addN(
                (
                    (sv_injection, RDF.type, sv_inj_type, ctx),
                    (sv_injection, URIRef(self.ns["cim"] + "SvInjection.TopologicalNode"), s, ctx),
                    (
                        sv_injection,
                        URIRef(self.ns["cim"] + "SvInjection.pInjection"),
                        Literal(0.0, datatype=XSD.float),
                        ctx,
                    ),
                )
            )

    def add_eic_code(self) -> None:
        for substation, _, _, ctx in self.graph.quads((None, RDF.type, URIRef(self.ns["cim"] + "Substation"), None)):
            assert ctx
            market_delivery_point = BNode()
            bidding_area = BNode()
            eic_code = Literal("10Y1001A1001A48H", datatype=XSD.string)
            self.graph.addN(
                (
                    (substation, URIRef(f"{self.ns['SN']}Substation.MarketDeliveryPoint"), market_delivery_point, ctx),
                    (
                        market_delivery_point,
                        URIRef(f"{self.ns['SN']}MarketDeliveryPoint.BiddingArea"),
                        bidding_area,
                        ctx,
                    ),
                    (
                        bidding_area,
                        URIRef(f"{self.ns['entsoeSecretariat']}IdentifiedObject.energyIdentCodeEIC"),
                        eic_code,
                        ctx,
                    ),
                )
            )

    def add_network_analysis_enable(self) -> None:
        for _, _, equipment, ctx in self.graph.quads(
            (None, URIRef(f"{self.ns['cim']}Terminal.ConductingEquipment"), None, None)
        ):
            assert ctx
            self.graph.add(
                (
                    equipment,
                    URIRef(f"{self.ns['SN']}Equipment.networkAnalysisEnable"),
                    Literal("true", datatype=XSD.boolean),
                    ctx,
                ),
            )

    def add_generating_unit(self) -> None:
        updated = set()
        for sync_machine, _, _, ctx in self.graph.quads(
            (None, RDF.type, URIRef(self.ns["cim"] + "SynchronousMachine"), None)
        ):
            assert ctx
            generating_unit = BNode()
            schedule_resource = BNode()
            if sync_machine in updated:
                continue
            updated.add(sync_machine)
            self.graph.addN(
                (
                    (sync_machine, URIRef(self.ns["cim"] + "SynchronousMachine.GeneratingUnit"), generating_unit, ctx),
                    (generating_unit, RDF.type, URIRef(self.ns["SN"] + "GeneratingUnit"), ctx),
                    (
                        generating_unit,
                        URIRef(self.ns["cim"] + "IdentifiedObject.name"),
                        Literal("GeneratingUnit", datatype=XSD.string),
                        ctx,
                    ),
                    (
                        generating_unit,
                        URIRef(self.ns["SN"] + "GeneratingUnit.ScheduleResource"),
                        schedule_resource,
                        ctx,
                    ),
                    (schedule_resource, RDF.type, URIRef(self.ns["SN"] + "ScheduleResource"), ctx),
                    (
                        schedule_resource,
                        URIRef(self.ns["SN"] + "ScheduleResource.marketCode"),
                        Literal("market001", datatype=XSD.string),
                        ctx,
                    ),
                )
            )


def is_uuid(x: str) -> bool:
    return re.match("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", x) is not None


def generate_uuid(x: str) -> str:
    h = hashlib.md5(x.encode(), usedforsecurity=False)
    return str(uuid.UUID(hex=h.hexdigest()))
