from __future__ import annotations

import re
import warnings
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

import pandas as pd
from dateutil import parser

from cimsparql.constants import union_split
from cimsparql.query_support import combine_statements, group_query, select_statement

if TYPE_CHECKING:
    from cimsparql.model import Model


as_type_able = [int, float, str, "int16", "int32", "int64", "Int16", "Int32", "Int64"]

python_type_map: Dict[str, Callable] = {
    "string": str,
    "integer": int,
    "boolean": lambda x: x.lower() == "true",
    "decimal": float,
    "float": float,
    "dateTime": parser.parse,
}
uri_snmst = re.compile("([^\\#]*(.\\#\\_)|urn:uuid:)")
sparql_type_map = {"literal": str, "uri": lambda x: uri_snmst.sub("", x) if x is not None else ""}


class TypeMapperQueries:
    @property
    def generals(self) -> str:
        """For sparql-types that are not sourced from objects of type rdf:property, sparql & type are
        required

        Sparql values should be like: http://iec.ch/TC57/2010/CIM-schema-cim15#PerCent this is how
        type or DataType usually looks like for each data point in the converted query result from
        SPARQLWrapper.

        type can be anything as long as it is represented in the python_type_map.
        """
        return combine_statements(
            *[
                "?sparql_type rdf:type rdfs:Datatype",
                "?sparql_type owl:equivalentClass ?range",
                'bind(lcase(strafter(str(?range), "#")) as ?type)',
            ],
            split=" .\n",
        )

    @property
    def prefix_general(self) -> str:
        """Common query used as a base for all prefix_based queries."""
        equiv = "?range owl:equivalentClass ?equiv"
        return combine_statements(
            *[
                "?sparql_type rdf:type rdf:Property",
                "?sparql_type rdfs:range ?range",
                group_query([equiv], command="optional"),
                f"bind(if(exists {{{equiv}}}, ?equiv, ?range) as ?t)",
                'bind(lcase(strafter(str(?t), "#")) as ?type)',
            ],
            split=" .\n",
        )


class TypeMapper(TypeMapperQueries):
    def __init__(self, client: Model, custom: Optional[Dict[str, Callable]] = None) -> None:
        self.prefixes = client.prefixes
        custom = custom if custom is not None else {}
        self.map = {**sparql_type_map, **self.get_map(client), **custom}

    def have_cim_version(self, cim) -> bool:
        return cim in (val.split("#")[0] for val in self.map.keys())

    @staticmethod
    def type_map(df: pd.DataFrame) -> Dict[str, Callable]:
        return df.set_index("sparql_type")["type"].replace(python_type_map).to_dict()

    @property
    def query(self) -> Optional[str]:
        if "owl" not in self.prefixes:
            return
        variables = ["?sparql_type", "?type"]
        statements = [self.generals, self.prefix_general]
        full = combine_statements(*statements, split=union_split, group=True)
        return combine_statements(select_statement(variables), combine_statements(full, group=True))

    def get_map(self, client: Model) -> Dict[str, Callable]:
        """Reads all metadata from the sparql backend & creates a sparql-type -> python type map

        Args:
            client: initialized Model

        Returns:
            sparql-type -> python type map

        """
        if (query := self.query) is None:
            return {}
        df = client.get_table(query, map_data_types=False)

        if df.empty:
            return {}
        type_map = self.type_map(df[~df["type"].isna()])
        xsd_map = {
            f"{self.prefixes['xsd']}#{xsd_type}": xsd_callable
            for xsd_type, xsd_callable in python_type_map.items()
        }
        return {**type_map, **xsd_map}

    def get_type(
        self,
        sparql_type: str,
        missing_return: Optional[str] = "identity",
        custom_maps: Optional[Dict[str, Callable]] = None,
    ):
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
        type_map = {**self.map, **custom_maps} if custom_maps is not None else self.map
        try:
            return type_map[sparql_type]
        except KeyError:
            warnings.warn(f"{sparql_type} not found in the sparql -> python type map")
            if missing_return == "identity":
                return lambda x: x
            return None

    def convert_dict(
        self,
        d: Dict[str, Callable],
        drop_missing: bool = True,
        custom_maps: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Converts a col_name -> sparql_datatype map to a col_name -> python_type map

        Args:
            d: dictionary with {'column_name': 'sparql type/DataType'}
            drop_missing: drops columns where no corresponding python type could be found
            custom_maps: dictionary on the form {'sparql_data_type': function/datatype} overwrites
                the default types gained from the graphdb. Applies the function/datatype on all
                columns in the DataFrame that are of the sparql_data_type.

        Returns:
            col_name -> python_type/function map

        """
        missing_return = "None" if drop_missing else "identity"
        base = {
            column: self.get_type(data_type, missing_return, custom_maps)
            for column, data_type in d.items()
        }
        if drop_missing:
            return {key: value for key, value in base.items() if value is not None}
        return base

    @staticmethod
    def map_base_types(df: pd.DataFrame, type_map: Dict[str, Callable]) -> pd.DataFrame:
        """Maps the datatypes in type_map which can be used with the df.astype function

        Args:
            df:
            type_map: {'column_name': type/function} map of functions/types to apply on the columns

        Returns:
            mapped DataFrame

        """
        as_type_able_columns = {c for c, datatype in type_map.items() if datatype in as_type_able}
        if not df.empty:
            df = df.astype({column: type_map[column] for column in as_type_able_columns})
        return df

    @staticmethod
    def map_exceptions(df: pd.DataFrame, type_map: Dict[str, Callable]) -> pd.DataFrame:
        """Maps the functions/datatypes in type_map which cant be done with the df.astype function

        Args:
            df:
            type_map: {'column_name': type/function} map of functions/types to apply on the columns

        Returns:
            mapped DataFrame

        """
        for column in {c for c, datatype in type_map.items() if datatype not in as_type_able}:
            df[column] = df[column].apply(type_map[column])
        return df

    def map_data_types(
        self,
        df: pd.DataFrame,
        col_map: Dict[str, Callable],
        custom_maps: Optional[Dict] = None,
        columns: Optional[Dict] = None,
    ) -> pd.DataFrame:
        """Maps the dtypes of a DataFrame to the python-corresponding types of the sparql-types from the
        source data

        Args:
            df: DataFrame with columns to be converted
            data_row: a complete row with data from the source data of which the DataFrame is
                constructed from
            custom_maps: dictionary on the form {'sparql_data_type': function/datatype} overwrites
                the default types gained from the graphdb. Applies the function/datatype on all
                columns in the DataFrame that are of the sparql_data_type.
            columns: dictionary on the form {'DataFrame_column_name: function/datatype} overwrites
                the default types gained from the graphdb.  Applies the function/datatype on the
                column.

        Returns:
            mapped DataFrame

        """
        type_map = {**self.convert_dict(col_map, custom_maps=custom_maps), **columns}
        df = self.map_base_types(df, type_map)
        df = self.map_exceptions(df, type_map)
        return df
