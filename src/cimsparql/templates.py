"""Templates for miscellaneous queries."""

import pathlib
from string import Template


def _read_template(filename: pathlib.Path) -> Template:
    with filename.open() as file:
        return Template(file.read())


sparql_folder = pathlib.Path(__file__).parent / "sparql"

AC_LINE_QUERY = _read_template(sparql_folder / "ac_lines.sparql")
ADD_MRID_QUERY = _read_template(sparql_folder / "add_mrid.sparql")
ASSOCIATED_SWITCHES = _read_template(sparql_folder / "associated_switches.sparql")
BASE_VOLTAGE = _read_template(sparql_folder / "base_voltage.sparql")
BORDERS_QUERY = _read_template(sparql_folder / "borders.sparql")
BRANCH_NODE_WITHDRAW_QUERY = _read_template(sparql_folder / "branch_node_withdraw.sparql")
BUS_DATA_QUERY = _read_template(sparql_folder / "bus.sparql")
CONNECTIONS_QUERY = _read_template(sparql_folder / "connections.sparql")
CONNECTIVITY_NODES_QUERY = _read_template(sparql_folder / "connectivity_nodes.sparql")
CONVERTERS_QUERY = _read_template(sparql_folder / "converters.sparql")
COORDINATES_QUERY = _read_template(sparql_folder / "coordinates.sparql")
DC_ACTIVE_POWER_FLOW_QUERY = _read_template(sparql_folder / "dc_active_power_flow.sparql")
DISCONNECTED_QUERY = _read_template(sparql_folder / "disconnected.sparql")
EXCHANGE_QUERY = _read_template(sparql_folder / "exchange.sparql")
FULL_MODEL_QUERY = _read_template(sparql_folder / "full_model.sparql")
GEN_UNIT_MRID_AND_SYNC_MACHINE = _read_template(sparql_folder / "gen_unit_and_sync_machine_mapping.sparql")
HVDC = _read_template(sparql_folder / "hvdc.sparql")
HVDC_CONVERTER_BIDZONES = _read_template(sparql_folder / "converter_hvdc_bidzones.sparql")
LOADS_QUERY = _read_template(sparql_folder / "loads.sparql")
MARKET_DATES_QUERY = _read_template(sparql_folder / "market_dates.sparql")
PHASE_TAP_CHANGER = _read_template(sparql_folder / "phase_tap_changer.sparql")
POWER_FLOW_QUERY = _read_template(sparql_folder / "power_flow.sparql")
RAS_EQUIPMENT_QUERY = _read_template(sparql_folder / "ras_equipment.sparql")
REGIONS_QUERY = _read_template(sparql_folder / "regions.sparql")
SERIES_COMPENSATORS_QUERY = _read_template(sparql_folder / "series_compensators.sparql")
STATION_GROUP_CODE_NAME_QUERY = _read_template(sparql_folder / "station_group_code_and_names.sparql")
STATION_GROUP_FOR_POWER_UNIT_QUERY = _read_template(sparql_folder / "station_group_for_power_unit.sparql")
SUBSTATION_VOLTAGE_LEVEL_QUERY = _read_template(sparql_folder / "substation_voltage_level.sparql")
SV_BRANCH_QUERY = _read_template(sparql_folder / "sv_branch.sparql")
SV_INJECTION_QUERY = _read_template(sparql_folder / "sv_injection.sparql")
SV_POWER_DEVIATION_QUERY = _read_template(sparql_folder / "sv_power_deviation.sparql")
SWITCHES_QUERY = _read_template(sparql_folder / "switches.sparql")
SYNCHRONOUS_MACHINES_QUERY = _read_template(sparql_folder / "synchronous_machines.sparql")
TRANSFORMER_BRANCHES_QUERY = _read_template(sparql_folder / "transformer_branches.sparql")
TRANSFORMER_CENTER_NODES_QUERY = _read_template(sparql_folder / "transformer_center_nodes.sparql")
TRANSFORMERS_CONNECTED_TO_CONVERTER_QUERY = _read_template(sparql_folder / "transformers_connected_to_converter.sparql")
TRANSFORMERS_QUERY = _read_template(sparql_folder / "transformers.sparql")
TRANSFORMER_WINDINGS_QUERY = _read_template(sparql_folder / "transformer_windings.sparql")
TRANSFORMER_WINDING_ANGLE_QUERY = _read_template(sparql_folder / "transformer_winding_angle.sparql")
TYPE_MAPPER_QUERY = _read_template(sparql_folder / "type_mapper.sparql")
WINDING = _read_template(sparql_folder / "winding.sparql")
WINDING_LOSS_QUERY = _read_template(sparql_folder / "winding_loss.sparql")
WIND_GENERATING_UNITS_QUERY = _read_template(sparql_folder / "wind_generating_units.sparql")
