from __future__ import annotations

import warnings
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

import pandas as pd

from cimsparql.query_support import combine_statements, unionize

if TYPE_CHECKING:
    from cimsparql.model import CimModel

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


def identity(x):
    return x


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


def build_type_map(prefixes: Dict[PREFIX, URI]) -> Dict[SPARQL_TYPE, TYPE_CASTER]:
    short_map = {"xsd": XSD_TYPE_MAP, "cim": CIM_TYPE_MAP}

    type_map = {}
    for namespace, prim_types in short_map.items():
        if namespace not in prefixes:
            continue
        prefix = prefixes[namespace]
        new_map = {f"{prefix}#{dtype}": converter for dtype, converter in prim_types.items()}
        type_map.update(new_map)
    return type_map


class TypeMapperQueries:
    def __init__(self, prefixes: Dict[PREFIX, URI]):
        self.prefixes = prefixes

    @property
    def type_queries(self) -> List[List[str]]:
        queries = [
            ["?sparql_type rdf:type rdfs:Datatype", "?sparql_type owl:equivalentClass ?range"],
            ["?sparql_type owl:equivalentClass ?range"],
        ]

        if "cims" in self.prefixes:
            queries += self.cims_queries
        return queries

    @property
    def cims_queries(self) -> List[List[str]]:
        """
        Return queries that require that cims namespace. We must distinguish between types
        that is of type 'Primitive' and types that are of type 'CIMdatatype'. In the latter case
        we must find the type of the 'value' attribute
        """
        return [
            [
                "?datatypevalue rdfs:domain ?sparql_type",
                "?datatypevalue cims:dataType ?range",
                '?range cims:stereotype "Primitive"',
            ],
            [
                "?datatypevalue rdfs:domain ?sparql_type",
                "?datatypevalue cims:dataType ?CIMdtype",
                '?CIMdtype cims:stereotype "CIMDatatype"',
                "?CIMdtypeValue rdfs:domain ?CIMdtype",
                '?CIMdtypeValue rdfs:label "value"@en',
                "?CIMdtypeValue cims:dataType ?range",
            ],
        ]

    @property
    def query(self) -> str:
        select_query = "SELECT ?sparql_type ?range"
        grouped = [combine_statements(*g, split=".\n") for g in self.type_queries]
        union = unionize(*grouped)
        return f"{select_query}\nWHERE\n{{\n{union}\n}}"


class TypeMapper:
    def __init__(self, client: CimModel, custom_additions: Optional[Dict[str, Any]] = None) -> None:
        self.queries = TypeMapperQueries(client.prefixes)
        self.prefixes = client.prefixes
        custom_additions = custom_additions or {}
        self.prim_type_map = build_type_map(self.prefixes)
        self.map = sparql_type_map | self.get_map(client) | self.prim_type_map | custom_additions

    def have_cim_version(self, cim) -> bool:
        return cim in (val.split("#")[0] for val in self.map.keys())

    def type_map(self, df: pd.DataFrame) -> Dict[str, Any]:
        return {
            row.sparql_type: self.prim_type_map.get(row.range, "string") for row in df.itertuples()
        }

    def get_map(self, client: CimModel) -> Dict[str, Any]:
        """Reads all metadata from the sparql backend & creates a sparql-type -> python type map

        Args:
            client: initialized CimModel

        Returns:
            sparql-type -> python type map

        """
        df = client.get_table(self.queries.query, map_data_types=False)
        if df.empty:
            return {}
        return self.type_map(df)

    def get_type(self, sparql_type: str, missing_return: str = "identity"):
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
            warnings.warn(f"{sparql_type} not found in the sparql -> python type map")
            if missing_return == "identity":
                return lambda x: x
            return None

    def build_type_caster(
        self, col_map: Dict[COL_NAME, SPARQL_TYPE]
    ) -> Dict[COL_NAME, TYPE_CASTER]:
        """
        Construct a direct mapping from column names to a type caster from the
        """
        return {col: self.get_type(dtype) for col, dtype in col_map.items()}

    def map_data_types(
        self, df: pd.DataFrame, col_map: Dict[COL_NAME, SPARQL_TYPE]
    ) -> pd.DataFrame:
        """Maps the dtypes of a DataFrame to the python-corresponding types of the sparql-types from the
        source data

        Args:
            df: DataFrame with columns to be converted

        Returns:
            mapped DataFrame

        """
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
