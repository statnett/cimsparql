import pathlib
import re
from string import Template
from typing import Dict


def drop_prefix(query: str) -> str:
    """Remove PREFIX from query.

    For sub-templates which should also be able to run seperately
    """
    return re.sub(r"^PREFIX .*\n?", "", query, flags=re.MULTILINE)


def _read_template(filename: pathlib.Path) -> Template:
    with open(filename, "r") as file:
        return Template(file.read())


def _ac_line_terminals(query: Template) -> Dict[str, str]:
    return {f"acline_terminal_{nr}": drop_prefix(query.safe_substitute(nr=nr)) for nr in [1, 2]}


sparql_folder = pathlib.Path(__file__).parent / "sparql"

ACLINE_TERMINAL_QUERY = _read_template(sparql_folder / "acline_terminal.sparql")
AC_LINE_QUERY = Template(
    _read_template(sparql_folder / "ac_lines.sparql").safe_substitute(
        _ac_line_terminals(ACLINE_TERMINAL_QUERY)
    )
)
ADD_MRID_QUERY = _read_template(sparql_folder / "add_mrid.sparql")
BORDERS_QUERY = _read_template(sparql_folder / "borders.sparql")
BRANCH_NODE_WITHDRAW_QUERY = _read_template(sparql_folder / "branch_node_withdraw.sparql")
BUS_DATA_QUERY = _read_template(sparql_folder / "bus.sparql")
CONNECTIONS_QUERY = _read_template(sparql_folder / "connections.sparql")
CONVERTERS_QUERY = _read_template(sparql_folder / "converters.sparql")
COORDINATES_QUERY = _read_template(sparql_folder / "coordinates.sparql")
DC_ACTIVE_POWER_FLOW_QUERY = _read_template(sparql_folder / "dc_active_power_flow.sparql")
DISCONNECTED_QUERY = _read_template(sparql_folder / "disconnected.sparql")
EXCHANGE_QUERY = _read_template(sparql_folder / "exchange.sparql")
FULL_MODEL_QUERY = _read_template(sparql_folder / "full_model.sparql")
LOADS_QUERY = _read_template(sparql_folder / "loads.sparql")
MARKET_DATES_QUERY = _read_template(sparql_folder / "market_dates.sparql")
POWER_FLOW_QUERY = _read_template(sparql_folder / "power_flow.sparql")
REGIONS_QUERY = _read_template(sparql_folder / "regions.sparql")
SERIES_COMPENSATORS_QUERY = _read_template(sparql_folder / "series_compensators.sparql")
STATION_GROUP_CODE_NAME_QUERY = _read_template(
    sparql_folder / "station_group_code_and_names.sparql"
)
SUBSTATION_VOLTAGE_LEVEL_QUERY = _read_template(sparql_folder / "substation_voltage_level.sparql")
SV_BRANCH_QUERY = _read_template(sparql_folder / "sv_branch.sparql")
SYNCHRONOUS_MACHINES_QUERY = _read_template(sparql_folder / "synchronous_machines.sparql")
THREE_WINDING_DUMMY_NODES_QUERY = _read_template(sparql_folder / "three_winding_dummy_nodes.sparql")
THREE_WINDING_LOSS_QUERY = _read_template(sparql_folder / "three_winding_loss.sparql")
THREE_WINDING_QUERY = _read_template(sparql_folder / "three_winding.sparql")
TRANSFORMERS_QUERY = _read_template(sparql_folder / "transformers.sparql")
TRANSFORMERS_CONNECTED_TO_CONVERTER_QUERY = _read_template(
    sparql_folder / "transformers_connected_to_converter.sparql"
)
TWO_WINDING_ANGLE_QUERY = _read_template(sparql_folder / "two_winding_transformer_angle.sparql")
TWO_WINDING_QUERY = _read_template(sparql_folder / "two_winding_transformer.sparql")
TYPE_MAPPER_QUERY = _read_template(sparql_folder / "type_mapper.sparql")
WIND_GENERATING_UNITS_QUERY = _read_template(sparql_folder / "wind_generating_units.sparql")
