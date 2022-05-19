from __future__ import annotations

import re
import warnings
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

import pandas as pd

from cimsparql.query_support import combine_statements, unionize

if TYPE_CHECKING:
    from cimsparql.model import CimModel

TYPE_CASTER = Callable[[Any], Any]
SPARQL_TYPE = str
COL_NAME = str

as_type_able = [int, float, str, "Int64", "Int32", "Int16"]


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
    "String": str,
    "Integer": int,
    "Boolean": bool,
    "Float": float,
    "Date": pd.to_datetime,
}

uri_snmst = re.compile("[^\\#]*(.\\#\\_)")
sparql_type_map = {"literal": str, "uri": lambda x: uri_snmst.sub("", x) if x is not None else ""}


def build_type_map(prefixes: Dict[str, str]) -> Dict[SPARQL_TYPE, TYPE_CASTER]:
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
    @property
    def generals(self) -> List[List[str]]:
        """For sparql-types that are not sourced from objects of type rdf:property, sparql & type are
        required

        Sparql values should be like: http://iec.ch/TC57/2010/CIM-schema-cim15#PerCent this is how
        type or DataType usually looks like for each data point in the converted query result from
        SPARQLWrapper.

        type can be anything as long as it is represented in one of the type_maps
        (XSD_TYPE_MAP, CIM_TYPE_MAP etc.).
        """
        return [["?sparql_type rdf:type rdfs:Datatype", "?sparql_type owl:equivalentClass ?range"]]

    @property
    def prefix_general(self) -> List[str]:
        """Common query used as a base for all prefix_based queries."""
        return ["?sparql_type rdf:type rdf:Property", "?sparql_type rdfs:range ?range"]

    @property
    def prefix_based(self) -> Dict[str, List[str]]:
        """Each prefix can have different locations of where DataTypes are described.

        Based on a object of type rdf:property & its rdfs:range, one has edit the query such that
        one ends up with the DataType.

        """
        return {
            "https://www.w3.org/2001/XMLSchema": ["?range rdfs:label ?type"],
            "https://iec.ch/TC57/2010/CIM-schema-cim15": [
                "?range owl:equivalentClass ?class",
                "?class rdfs:label ?type",
            ],
        }

    @property
    def query(self) -> str:
        select_query = "SELECT ?sparql_type ?range"

        grouped_generals = [combine_statements(*g, split=" .\n") for g in self.generals]
        grouped_prefixes = [
            combine_statements(*v, f'FILTER (?prefix = "{k}")', split=" .\n")
            for k, v in self.prefix_based.items()
        ]
        grouped_prefix_general = combine_statements(*self.prefix_general, split=" .\n")
        unionized_generals = unionize(*grouped_generals)
        unionized_prefixes = unionize(*grouped_prefixes)

        full_prefixes = combine_statements(grouped_prefix_general, unionized_prefixes, group=True)
        full_union = unionize(unionized_generals, full_prefixes, group=False)
        return f"{select_query}\nWHERE\n{{\n{full_union}\n}}"


class TypeMapper:
    def __init__(self, client: CimModel, custom_additions: Optional[Dict[str, Any]] = None) -> None:
        self.queries = TypeMapperQueries()
        self.prefixes = client.prefixes
        custom_additions = custom_additions or {}
        self.prim_type_map = build_type_map(self.prefixes)
        self.map = {**sparql_type_map, **self.get_map(client), **custom_additions}

    def have_cim_version(self, cim) -> bool:
        return cim in (val.split("#")[0] for val in self.map.keys())

    def type_map(self, df: pd.DataFrame) -> Dict[str, Any]:
        return {row.sparql_type: self.prim_type_map.get(row.range, str) for row in df.itertuples()}

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
