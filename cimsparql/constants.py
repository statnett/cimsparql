from typing import List, Tuple

# Allowed
allowed_load_types: Tuple[str, ...] = ("ConformLoad", "NonConformLoad", "EnergyConsumer")

generating_types: Tuple[str, ...] = ("Hydro", "Thermal", "Wind")

# Available ratings
ratings: Tuple[str, ...] = ("Normal", "Warning", "Overload")

converter_types: Tuple[str, ...] = (
    "ALG:VoltageSourceConverter",
    "ALG:DCConverter",
    "cim:VsConverter",
    "cim:CsConverter",
    "cim:DCConvertUnit",
)

union_split: str = "}\nUNION\n{"
sequence_numbers: List[int] = [1, 2]

# Default variable names
con_mrid_str: str = "connectivity_mrid"
mrid_variable: str = "?mrid"

impedance_variables: Tuple[str, ...] = ("r", "x")
