from __future__ import annotations

import warnings
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, Generator, Optional, Union

import pandas as pd

from cimsparql.graphdb import GraphDBClient, ServiceConfig
from cimsparql.templates import TYPE_MAPPER_QUERY

# Type-caster that can be used with pandas. It can be python types, numpy dtypes or string
# value. Examples: float, "int", int, "string"
TYPE_CASTER = Union[Callable[[Any], Any], str]
SPARQL_TYPE = str
COL_NAME = str
PREFIX = str
URI = str

as_type_able = [int, float, "string", "Int64", "Int32", "Int16"]


def to_timedelta(duration: str) -> datetime.timedelta:
    # Pandas only supports days, hours, minites, seconds
    # not year and month which can be part of
    # https://www.w3.org/TR/xmlschema11-2/#duration

    if "Y" in duration:
        raise ValueError("Cimsparql uses pandas to convert duration. Y not supported")

    return pd.to_timedelta(duration)


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
    "String": "string",
    "Integer": int,
    "Boolean": bool,
    "Float": float,
    "Date": pd.to_datetime,
}

sparql_type_map = {
    "literal": "string",
    "uri": "string",
}


@contextmanager
def enforce_no_limit(client: GraphDBClient) -> Generator[GraphDBClient, None, None]:
    orig_limit = client.service_cfg.limit
    client.service_cfg.limit = None
    try:
        yield client
    finally:
        client.service_cfg.limit = orig_limit


def build_type_map(prefixes: Dict[PREFIX, URI]) -> Dict[SPARQL_TYPE, TYPE_CASTER]:
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
        service_cfg: Optional[ServiceConfig] = None,
        custom_additions: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.client = GraphDBClient(service_cfg)

        self.query = TYPE_MAPPER_QUERY.substitute(self.client.prefixes)
        custom_additions = custom_additions or {}
        self.prim_type_map = build_type_map(self.client.prefixes)
        self.map = sparql_type_map | self.get_map() | self.prim_type_map | custom_additions

    def have_cim_version(self, cim) -> bool:
        return any(cim in val for val in self.map.keys())

    def type_map(self, df: pd.DataFrame) -> Dict[str, Any]:
        return {
            row.sparql_type: self.prim_type_map.get(row.range, "string") for row in df.itertuples()
        }

    def get_map(self) -> Dict[str, Any]:
        """Reads all metadata from the sparql backend & creates a sparql-type -> python type map

        Args:
            client: initialized CimModel

        Returns:
            sparql-type -> python type map

        """

        with enforce_no_limit(self.client) as c:
            res = c.get_table(self.query)
            df = res[0]
        if df.empty:
            return {}
        return self.type_map(df)

    def get_type(self, sparql_type: str, col: str = ""):
        """Gets the python type/function to apply on columns of the sparql_type

        Args:
            sparql_type:
            missing_return: returns the identity-function if python- type/function is not found,
                else returns None

            custom_maps: dictionary on the form {'sparql_data_type': function/datatype} overwrites
                the default types gained from the graphdb. Applies the function/datatype on all
                columns in the DataFrame that are of the sparql_data_type

        Returns:
            python datatype or function to apply on DataFrame columns

        """

        try:
            return self.map[sparql_type]
        except KeyError:
            warnings.warn(
                f"{col}:{sparql_type} not found in the sparql -> python type map. Using 'string'"
            )
        return "string"

    def build_type_caster(
        self, col_map: Dict[COL_NAME, SPARQL_TYPE]
    ) -> Dict[COL_NAME, TYPE_CASTER]:
        """
        Construct a direct mapping from column names to a type caster from the
        """
        return {col: self.get_type(dtype, col) for col, dtype in col_map.items()}

    def map_data_types(
        self, df: pd.DataFrame, col_map: Dict[COL_NAME, SPARQL_TYPE]
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


def map_base_types(df: pd.DataFrame, type_map: Dict[COL_NAME, TYPE_CASTER]) -> pd.DataFrame:
    """Maps the datatypes in type_map which can be used with the df.astype function

    Args:
        df:

    Returns:
        mapped DataFrame

    """
    as_type_able_columns = {c for c, datatype in type_map.items() if datatype in as_type_able}
    if not df.empty:
        df = df.astype({column: type_map[column] for column in as_type_able_columns})
    return df


def map_exceptions(df: pd.DataFrame, type_map: Dict[COL_NAME, TYPE_CASTER]) -> pd.DataFrame:
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
