from __future__ import annotations

import dataclasses
from contextlib import contextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pandas as pd

from cimsparql.graphdb import GraphDBClient, ServiceConfig
from cimsparql.templates import TYPE_MAPPER_QUERY

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

# Type-caster that can be used with pandas. It can be python types, numpy dtypes or string
# value. Examples: float, "int", int, "string"
if TYPE_CHECKING:
    TYPE_CASTER = Callable[[Any], Any] | str
    SPARQL_TYPE = str
    COL_NAME = str
    PREFIX = str
    URI = str

as_type_able = [int, float, "Int64", "Int32", "Int16"]


def to_timedelta(duration: str) -> pd.Timedelta:
    # Pandas only supports days, hours, minites, seconds
    # not year and month which can be part of
    # https://www.w3.org/TR/xmlschema11-2/#duration

    if "Y" in duration:
        raise ValueError("Cimsparql uses pandas to convert duration. Y not supported")

    return pd.to_timedelta(duration)


def str_preserve_none(x: Any) -> str | None:
    return None if pd.isna(x) else str(x)


XSD_TYPE_MAP = {
    # Primitive types (https://www.w3.org/TR/xmlschema11-2/#built-in-primitive-datatypes)
    "boolean": lambda x: x.lower() in {"true", "1"},
    "date": pd.to_datetime,
    "dateTime": pd.to_datetime,
    "decimal": Decimal,
    "double": float,
    "duration": to_timedelta,  # Require ISO 8601 format,
    "float": float,
    "integer": int,
    "time": pd.to_datetime,
}


CIM_TYPE_MAP = {
    "String": str_preserve_none,
    "Integer": int,
    "Boolean": bool,
    "Float": float,
    "Date": pd.to_datetime,
}

sparql_type_map = {"literal": str_preserve_none, "uri": str_preserve_none}


@contextmanager
def enforce_no_limit(client: GraphDBClient) -> Generator[GraphDBClient, None, None]:
    orig_cfg = client.service_cfg
    client.service_cfg = dataclasses.replace(orig_cfg, limit=None)
    client._update_sparql_parameters()

    try:
        yield client
    finally:
        client.service_cfg = orig_cfg
        client._update_sparql_parameters()


def build_type_map(prefixes: dict[PREFIX, URI]) -> dict[SPARQL_TYPE, TYPE_CASTER]:
    short_map = {"xsd": XSD_TYPE_MAP, "cim": CIM_TYPE_MAP}

    type_map = {}
    for namespace, prim_types in short_map.items():
        if namespace not in prefixes:
            continue
        prefix = prefixes[namespace]
        new_map = {f"{prefix}{dtype}": converter for dtype, converter in prim_types.items()}
        type_map.update(new_map)
    return type_map


class TypeMapper:
    def __init__(
        self,
        service_cfg: ServiceConfig | None = None,
        custom_additions: dict[str, Any] | None = None,
    ) -> None:
        self.client = GraphDBClient(service_cfg)

        self.query = TYPE_MAPPER_QUERY.substitute(self.client.prefixes)
        custom_additions = custom_additions or {}
        self.prim_type_map = build_type_map(self.client.prefixes)
        self.map = sparql_type_map | self.get_map() | self.prim_type_map | custom_additions

    def have_cim_version(self, cim: str) -> bool:
        return any(cim in val for val in self.map)

    def type_map(self, df: pd.DataFrame) -> dict[str, Any]:
        return {
            row.sparql_type: self.prim_type_map.get(row.range, str_preserve_none)
            for row in df.itertuples()
        }

    def get_map(self) -> dict[str, Any]:
        """Reads all metadata from the sparql backend & creates a sparql-type -> python type map

        Args:
            client: initialized SingleClientModel

        Returns:
            sparql-type -> python type map

        """

        with enforce_no_limit(self.client) as c:
            res = c.get_table(self.query)
            df = res[0]
        if df.empty:
            return {}
        return self.type_map(df)

    def build_type_caster(
        self, col_map: dict[COL_NAME, SPARQL_TYPE]
    ) -> dict[COL_NAME, TYPE_CASTER]:
        """
        Construct a direct mapping from column names to a type caster from the
        """
        return {col: self.map[dtype] for col, dtype in col_map.items() if dtype in self.map}

    def map_data_types(
        self, df: pd.DataFrame, col_map: dict[COL_NAME, SPARQL_TYPE]
    ) -> pd.DataFrame:
        """Maps the dtypes of a DataFrame to the python-corresponding types of the sparql-types from
        the source data

        Args:
            df: DataFrame with columns to be converted

        Returns:
            mapped DataFrame

        """
        if df.empty:
            return df
        type_caster = self.build_type_caster(col_map)
        df = map_base_types(df, type_caster)
        df = map_exceptions(df, type_caster)
        return df


def map_base_types(df: pd.DataFrame, type_map: dict[COL_NAME, TYPE_CASTER]) -> pd.DataFrame:
    """Maps the datatypes in type_map which can be used with the df.astype function

    Args:
        df:

    Returns:
        mapped DataFrame

    """
    as_type_able_columns = {c for c, datatype in type_map.items() if datatype in as_type_able}
    if not df.empty:
        df = df.astype({c: type_map[c] for c in as_type_able_columns})
    return df


def map_exceptions(df: pd.DataFrame, type_map: dict[COL_NAME, TYPE_CASTER]) -> pd.DataFrame:
    """Maps the functions/datatypes in type_map which cant be done with the df.astype function

    Args:
        df:

    Returns:
        mapped DataFrame

    """
    ex_columns = {c for c, datatype in type_map.items() if datatype not in as_type_able}
    for column in ex_columns:
        df[column] = df[column].apply(type_map[column])
    return df
