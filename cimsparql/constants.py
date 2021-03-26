from typing import Iterable, List, Tuple

# Allowed
allowed_load_types: Tuple[str] = ("ConformLoad", "NonConformLoad", "EnergyConsumer")

generating_types: Iterable[str] = ("Hydro", "Thermal", "Wind")

# Available ratings
ratings: Tuple[str] = ("Normal", "Warning", "Overload")

union_split: str = "\n} UNION\n{"
sequence_numbers: List[int] = [1, 2]

# Default variable names
con_mrid_str: str = "connectivity_mrid"
mrid_variable: str = "?mrid"

impedance_variables: Iterable[str] = ("r", "x")
