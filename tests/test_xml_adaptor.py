from pathlib import Path

import pytest

from cimsparql.adaptions import XmlModelAdaptor, is_uuid


@pytest.fixture
def xml_adaptor():
    folder = Path(__file__).parent / "data" / "micro"
    return XmlModelAdaptor.from_folder(folder)


def test_namespaces(xml_adaptor: XmlModelAdaptor):
    ns = xml_adaptor.namespaces()
    assert {"xsd", "cim"}.issubset(set(ns.keys()))


def test_add_mrid(xml_adaptor: XmlModelAdaptor):
    query = "select * where {graph ?g {?s cim:IdentifiedObject.mRID ?mrid}}"
    assert len(list(xml_adaptor.select_query(query, prefixes=xml_adaptor.namespaces()))) == 0
    xml_adaptor.add_mrid()
    assert len(list(xml_adaptor.select_query(query, prefixes=xml_adaptor.namespaces()))) > 0


def test_tpsvssh_contexts(xml_adaptor: XmlModelAdaptor):
    assert len(set(xml_adaptor.tpsvssh_contexts())) == 6


@pytest.mark.parametrize(
    ("value", "result"),
    [
        ("17086487-56ba-4979-b8de-064025a6b4da", True),
        ("1708648756ba-4979-b8de-064025a6b4da", False),
    ],
)
def test_is_uuid(value: str, result: bool):
    assert is_uuid(value) is result


def test_add_internal_eq_link(xml_adaptor: XmlModelAdaptor):
    xml_adaptor.add_internal_eq_link("http://eq.com")
    query = f"select * where {{graph ?g {{?node <{XmlModelAdaptor.eq_predicate}> ?eq_uri}}}}"
    assert len(list(xml_adaptor.select_query(query))) == 1


def test_add_sv_injection(xml_adaptor: XmlModelAdaptor) -> None:
    query = "select * where {graph ?g {?s a cim:SvInjection}}"
    orig_number = len(list(xml_adaptor.select_query(query, prefixes=xml_adaptor.namespaces())))
    xml_adaptor.add_zero_sv_injection()
    assert len(list(xml_adaptor.select_query(query, prefixes=xml_adaptor.namespaces()))) > orig_number


def test_add_eic_code(xml_adaptor: XmlModelAdaptor) -> None:
    query = "select * where {graph ?g {?s a cim:Substation}}"
    num_substations = len(list(xml_adaptor.select_query(query, prefixes=xml_adaptor.namespaces())))
    assert num_substations > 0
    xml_adaptor.add_eic_code()
    query = "select * where {graph ?g {?s entsoeSecretariat:IdentifiedObject.energyIdentCodeEIC ?eic}}"
    assert len(list(xml_adaptor.select_query(query, prefixes=xml_adaptor.namespaces()))) == num_substations


def test_add_network_analysis_enable(xml_adaptor: XmlModelAdaptor) -> None:
    xml_adaptor.add_network_analysis_enable()
    query = """
        select * where {graph ?g {?terminal cim:Terminal.ConductingEquipment/SN:Equipment.networkAnalysisEnable true }}
        """
    assert len(list(xml_adaptor.select_query(query, prefixes=xml_adaptor.namespaces()))) > 0


def test_add_generating_unit(xml_adaptor: XmlModelAdaptor) -> None:
    xml_adaptor.add_generating_unit()
    query = "select * {graph ?g {?machine cim:SynchronousMachine.GeneratingUnit ?unit}}"
    assert len(list(xml_adaptor.select_query(query, prefixes=xml_adaptor.namespaces()))) > 0


def test_add_market_code_to_non_conform_load(xml_adaptor: XmlModelAdaptor) -> None:
    query = "select * {graph ?g {?group SN:NonConformLoadGroup.ScheduleResource/SN:ScheduleResource.marketCode ?code}}"
    assert len(list(xml_adaptor.select_query(query, prefixes=xml_adaptor.namespaces()))) == 0

    xml_adaptor.add_market_code_to_non_conform_load()
    assert len(list(xml_adaptor.select_query(query, prefixes=xml_adaptor.namespaces()))) == 6


def test_num_base_voltage_in_serialized_graph(xml_adaptor: XmlModelAdaptor) -> None:
    result = xml_adaptor.nq_bytes(set(xml_adaptor.eq_contexts())).decode()
    assert result.count("BaseVoltage.nominalVoltage") == 8


def test_add_ras(xml_adaptor: XmlModelAdaptor) -> None:
    xml_adaptor.add_protective_action_equipment()
    query = "select * {graph ?g {?s a ALG:ProtectiveActionEquipment}}"
    assert len(list(xml_adaptor.select_query(query, prefixes=xml_adaptor.namespaces()))) == 1
