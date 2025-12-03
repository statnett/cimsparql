"""Adaption functionality used to modify test cases."""

from __future__ import annotations

import hashlib
import itertools
import re
import uuid
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyoxigraph import BlankNode, DefaultGraph, Literal, NamedNode, Quad, QuerySolutions, RdfFormat, Store

from cimsparql.graphdb import default_namespaces

if TYPE_CHECKING:
    from collections.abc import Container, Iterator, Mapping
    from pathlib import Path


class StandardNamespaces:
    rdf_type = NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
    xsd_integer = NamedNode("http://www.w3.org/2001/XMLSchema#integer")
    xsd_boolean = NamedNode("http://www.w3.org/2001/XMLSchema#boolean")
    xsd_float = NamedNode("http://www.w3.org/2001/XMLSchema#float")


@dataclass
class ProtectiveActionEquipment:
    equipment: NamedNode
    name: str
    flow_shift: bool
    flow_shift_flip: bool
    load_contribution: bool
    unit_contribution: bool

    def to_quads(self, ns: Mapping[str, str], graph: NamedNode | BlankNode) -> list[Quad]:
        rpact = BlankNode()
        collection = BlankNode()

        return [
            Quad(rpact, StandardNamespaces.rdf_type, NamedNode(ns["ALG"] + "ProtectiveActionEquipment"), graph),
            Quad(rpact, NamedNode(ns["ALG"] + "ProtectiveActionEquipment.Equipment"), self.equipment, graph),
            Quad(rpact, NamedNode(ns["cim"] + "IdentifiedObject.name"), Literal(self.name), graph),
            Quad(rpact, NamedNode(ns["ALG"] + "ProtectiveAction.flowShift"), Literal(self.flow_shift), graph),
            Quad(rpact, NamedNode(ns["ALG"] + "ProtectiveAction.flowShiftFlip"), Literal(self.flow_shift_flip), graph),
            Quad(
                rpact,
                NamedNode(ns["ALG"] + "ProtectiveAction.loadContribution"),
                Literal(self.load_contribution),
                graph,
            ),
            Quad(
                rpact,
                NamedNode(ns["ALG"] + "ProtectiveAction.unitContribution"),
                Literal(self.unit_contribution),
                graph,
            ),
            Quad(rpact, NamedNode(ns["ALG"] + "ProtectiveAction.ProtectiveActionCollection"), collection, graph),
            Quad(collection, StandardNamespaces.rdf_type, NamedNode(ns["ALG"] + "ProtectiveActionCollection"), graph),
        ]


class XmlModelAdaptor:
    eq_predicate = "http://entsoe.eu/CIM/EquipmentCore/3/1"

    def __init__(self, filenames: Iterator[Path]) -> None:
        self.store = Store()
        self.ns = default_namespaces()

        for filename in filenames:
            profile = filename.stem.rpartition("/")[-1]
            uri = NamedNode(f"http://cimsparql/xml-adpator/{profile}")
            profile_store = Store()
            profile_store.load(None, RdfFormat.RDF_XML, path=filename, base_iri="http://cim.example.org#")
            for quad in profile_store.quads_for_pattern(None, None, None, None):
                self.store.add(Quad(quad.subject, quad.predicate, quad.object, uri))

    def select_query(self, query: str, *, prefixes: dict[str, str] | None = None) -> QuerySolutions:
        result = self.store.query(query, prefixes=prefixes)
        assert isinstance(result, QuerySolutions)
        return result

    def namespaces(self) -> dict[str, str]:
        return self.ns

    def graphs(self) -> set[str]:
        return {graph for _, _, _, graph in self.store.quads_for_pattern(None, None, None, None) if graph}

    def all_quads(self) -> Iterator[Quad]:
        return self.store.quads_for_pattern(None, None, None, None)

    @classmethod
    def from_folder(cls, folder: Path) -> XmlModelAdaptor:
        return XmlModelAdaptor(folder.glob("*.xml"))

    def add_mrid(self) -> None:
        """Add cim:IdentifiedObject.mRID if not present."""
        ns = self.namespaces()
        identified_obj_mrid = NamedNode(f"{ns['cim']}IdentifiedObject.mRID")
        has_mrid = set(self.store.quads_for_pattern(None, identified_obj_mrid, None, None))
        missing_mrid = set(self.all_quads()) - has_mrid

        seen_subjects = set[NamedNode | BlankNode]()

        # Sort the quads such that quads in the EQ graph is considered before the other profiles
        for quad in sorted(missing_mrid, key=lambda quad: "EQ" not in str(quad.graph_name)):
            if quad.subject in seen_subjects:
                continue
            assert isinstance(quad.subject, (NamedNode, BlankNode))
            seen_subjects.add(quad.subject)
            mrid_str = str(quad.subject.value).rpartition("#_")[-1]
            mrid = mrid_str if is_uuid(mrid_str) else generate_uuid(mrid_str)
            self.store.add(Quad(quad.subject, identified_obj_mrid, Literal(mrid), quad.graph_name))

    def add_market_code_to_non_conform_load(self) -> None:
        updated = set[NamedNode | BlankNode]()
        for load, _, _, ctx in self.store.quads_for_pattern(
            None, StandardNamespaces.rdf_type, NamedNode(self.ns["cim"] + "EnergyConsumer"), None
        ):
            if load in updated:
                continue

            if "EQ" not in str(ctx):
                continue
            updated.add(load)

            load_group = BlankNode()
            schedule_resource = BlankNode()
            self.store.add(Quad(load, NamedNode(self.ns["cim"] + "NonConformLoad.LoadGroup"), load_group, ctx))
            self.store.add(
                Quad(load_group, NamedNode(self.ns["cim"] + "IdentifiedObject.name"), Literal("created_group"), ctx)
            )
            self.store.add(
                Quad(
                    load_group,
                    NamedNode(self.ns["SN"] + "NonConformLoadGroup.ScheduleResource"),
                    schedule_resource,
                    ctx,
                )
            )
            self.store.add(
                Quad(
                    schedule_resource,
                    NamedNode(self.ns["SN"] + "ScheduleResource.marketCode"),
                    Literal("market001"),
                    ctx,
                )
            )

    def adapt(self, eq_uri: str) -> None:
        self.add_zero_sv_power_flow()
        self.add_zero_sv_injection()
        self.add_generating_unit()
        self.add_market_code_to_non_conform_load()
        self.add_protective_action_equipment()
        self.add_mrid()
        self.add_dtypes()
        self.add_internal_eq_link(eq_uri)
        self.add_eic_code()
        self.add_network_analysis_enable()

    def add_zero_sv_power_flow(self) -> None:
        sv_graph = NamedNode("http://cimsparql/xml-adpator/SV")
        sv_power_flow_terminal = NamedNode(self.ns["cim"] + "SvPowerFlow.Terminal")
        has_sv_power_flow = {
            quad.object for quad in self.store.quads_for_pattern(None, sv_power_flow_terminal, None, None)
        }

        zero_sv_flow_added = set[NamedNode | BlankNode]()

        for terminal, _, _, _ in self.store.quads_for_pattern(
            None, StandardNamespaces.rdf_type, NamedNode(self.ns["cim"] + "Terminal"), None
        ):
            # Do not add SvPowerFlow if it already exists
            if terminal in has_sv_power_flow or terminal in zero_sv_flow_added:
                continue
            zero_sv_flow_added.add(terminal)
            power_flow = BlankNode()
            self.store.add(Quad(power_flow, sv_power_flow_terminal, terminal, sv_graph))
            self.store.add(Quad(power_flow, NamedNode(self.ns["cim"] + "SvPowerFlow.p"), Literal(0.0), sv_graph))
            self.store.add(Quad(power_flow, NamedNode(self.ns["cim"] + "SvPowerFlow.q"), Literal(0.0), sv_graph))

    def add_dtypes(self) -> None:
        fields = {
            "endNumber": int,
            "sequenceNumber": int,
            "phaseAngleClock": int,
            "SvPowerFlow.p": float,
            ".open": to_boolean,
            ".connected": to_boolean,
            ".nominalVoltage": float,
        }
        for quad in self.all_quads():
            with suppress(StopIteration):
                f = next(f for f in fields if f in str(quad.predicate))
                self.store.remove(quad)

                caster = fields[f]
                assert isinstance(quad.object, Literal)
                literal = Literal(caster(quad.object.value))
                self.store.add(Quad(quad.subject, quad.predicate, literal, quad.graph_name))

    def tpsvssh_contexts(self) -> Iterator[NamedNode | BlankNode]:
        return (
            node for node in self.store.named_graphs() if any(substr in str(node) for substr in ("SV", "TP", "SSH"))
        )

    def eq_contexts(self) -> Iterator[NamedNode | BlankNode]:
        return (node for node in self.store.named_graphs() if any(substr in str(node) for substr in ("EQ", "GL")))

    def contexts(self) -> Iterator[NamedNode | BlankNode]:
        return self.store.named_graphs()

    def nq_bytes(self, contexts: Container[NamedNode | BlankNode | DefaultGraph] | None = None) -> bytes:
        """Return the contexts as bytes. If contexts is None, the entire graph is exported."""
        store = Store()
        for quad in self.all_quads():
            if contexts is None or quad.graph_name in contexts:
                store.add(quad)

        result = store.dump(format=RdfFormat.N_QUADS)
        assert isinstance(result, bytes)
        return result

    def add_internal_eq_link(self, eq_uri: str) -> None:
        # Insert in one SV graph
        ctx = next(c for c in self.tpsvssh_contexts() if "SV" in str(c))
        self.store.add(Quad(BlankNode(), NamedNode(self.eq_predicate), NamedNode(eq_uri), ctx))

    def add_zero_sv_injection(self) -> None:
        tp_node_type = NamedNode(self.ns["cim"] + "TopologicalNode")
        sv_inj_type = NamedNode(self.ns["cim"] + "SvInjection")

        graph = NamedNode("http://cimsparql/xml-adpator/SV-injection")
        used_nodes = set[NamedNode | BlankNode]()
        for s, _, _, _ in self.store.quads_for_pattern(None, StandardNamespaces.rdf_type, tp_node_type, None):
            if s in used_nodes:
                continue
            used_nodes.add(s)
            sv_injection = BlankNode()
            self.store.add(Quad(sv_injection, StandardNamespaces.rdf_type, sv_inj_type, graph))
            self.store.add(Quad(sv_injection, NamedNode(self.ns["cim"] + "SvInjection.TopologicalNode"), s, graph))
            self.store.add(
                Quad(sv_injection, NamedNode(self.ns["cim"] + "SvInjection.pInjection"), Literal(0.0), graph)
            )

    def add_eic_code(self) -> None:
        for substation, _, _, ctx in self.store.quads_for_pattern(
            None, StandardNamespaces.rdf_type, NamedNode(self.ns["cim"] + "Substation"), None
        ):
            market_delivery_point = BlankNode()
            bidding_area = BlankNode()
            eic_code = Literal("10Y1001A1001A48H")
            self.store.add(
                Quad(
                    substation, NamedNode(f"{self.ns['SN']}Substation.MarketDeliveryPoint"), market_delivery_point, ctx
                )
            )
            self.store.add(
                Quad(
                    market_delivery_point,
                    NamedNode(f"{self.ns['SN']}MarketDeliveryPoint.BiddingArea"),
                    bidding_area,
                    ctx,
                )
            )
            self.store.add(
                Quad(
                    bidding_area,
                    NamedNode(f"{self.ns['entsoeSecretariat']}IdentifiedObject.energyIdentCodeEIC"),
                    eic_code,
                    ctx,
                )
            )

    def add_network_analysis_enable(self) -> None:
        conducting_equipment = NamedNode(f"{self.ns['cim']}Terminal.ConductingEquipment")
        used_equipment = set[NamedNode | BlankNode]()
        for _, _, equipment, ctx in self.store.quads_for_pattern(None, conducting_equipment, None, None):
            if equipment in used_equipment:
                continue
            used_equipment.add(equipment)
            assert ctx
            network_analysis_enable = NamedNode(f"{self.ns['SN']}Equipment.networkAnalysisEnable")
            self.store.add(Quad(equipment, network_analysis_enable, Literal(value=True), ctx))

    def add_generating_unit(self) -> None:
        updated = set[NamedNode | BlankNode]()
        sync_machine_type = NamedNode(self.ns["cim"] + "SynchronousMachine")
        for sync_machine, _, _, ctx in self.store.quads_for_pattern(
            None, StandardNamespaces.rdf_type, sync_machine_type, None
        ):
            if "EQ" not in str(ctx):
                continue
            if sync_machine in updated:
                continue
            generating_unit = BlankNode()
            schedule_resource = BlankNode()
            updated.add(sync_machine)

            gen_unit_pred = NamedNode(self.ns["cim"] + "SynchronousMachine.GeneratingUnit")
            thermal_gen_unit_type = NamedNode(self.ns["cim"] + "ThermalGeneratingUnit")
            name_pred = NamedNode(self.ns["cim"] + "IdentifiedObject.name")
            schedule_resource_pred = NamedNode(self.ns["SN"] + "GeneratingUnit.ScheduleResource")
            scheduler_resource_type = NamedNode(self.ns["SN"] + "ScheduleResource")
            market_code_pred = NamedNode(self.ns["SN"] + "ScheduleResource.marketCode")

            self.store.add(Quad(sync_machine, gen_unit_pred, generating_unit, ctx))
            self.store.add(Quad(gen_unit_pred, StandardNamespaces.rdf_type, thermal_gen_unit_type))
            self.store.add(Quad(generating_unit, name_pred, Literal("GeneratuinggUnit"), ctx))
            self.store.add(Quad(generating_unit, schedule_resource_pred, schedule_resource, ctx))
            self.store.add(Quad(schedule_resource, StandardNamespaces.rdf_type, scheduler_resource_type, ctx))
            self.store.add(Quad(schedule_resource, market_code_pred, Literal("market001"), ctx))

    def add_protective_action_equipment(self) -> None:
        sync_machine, _, _, _ = next(
            self.store.quads_for_pattern(
                None, StandardNamespaces.rdf_type, NamedNode(self.ns["cim"] + "SynchronousMachine"), None
            )
        )
        load, _, _, _ = next(
            self.store.quads_for_pattern(
                None, StandardNamespaces.rdf_type, NamedNode(self.ns["cim"] + "EnergyConsumer"), None
            )
        )
        eq_graph = next(self.eq_contexts())
        for protective_action in (
            ProtectiveActionEquipment(
                equipment,
                name,
                flow_shift=flow_shift,
                flow_shift_flip=flow_shift_flip,
                load_contribution=load_contribution,
                unit_contribution=False,
            )
            for (flow_shift, flow_shift_flip, load_contribution) in itertools.product([True, False], repeat=3)
            for equipment, name in ((sync_machine, "ras_sync_machine"), (load, "ras_load"))
        ):
            for quad in protective_action.to_quads(self.ns, eq_graph):
                self.store.add(quad)


def is_uuid(x: str) -> bool:
    return re.match("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", x) is not None


def generate_uuid(x: str) -> str:
    h = hashlib.md5(x.encode(), usedforsecurity=False)
    return str(uuid.UUID(hex=h.hexdigest()))


def to_boolean(x: str) -> bool:
    return x.lower() in {"true", "1"}
