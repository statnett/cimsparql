from __future__ import annotations
import datetime as dt
from cimsparql.queries import combine_statements, unionize
from typing import TYPE_CHECKING

import pandas as pd
import warnings

if TYPE_CHECKING:
    from cimsparql.graphdb import GraphDBClient

as_type_able = [int, float, str, "Int64", "Int32", "Int16"]

python_type_map = {
    "string": str,
    "integer": int,
    "boolean": lambda x: x.lower == "true",
    "float": float,
    "dateTime": dt.datetime,
}

sparql_type_map = {"literal": str, "uri": lambda x: x.split("_")[-1] if len(x) == 48 else x}

generals = [
    [
        "?sparql_type rdf:type rdfs:Datatype",
        "?sparql_type owl:equivalentClass ?range",
        'BIND(STRAFTER(str(?range), "#") as ?type)',
    ]
]

prefix_general = [
    "?sparql_type rdf:type rdf:Property",
    "?sparql_type rdfs:range ?range",
    'BIND(STRBEFORE(str(?range), "#") as ?prefix) .',
]

prefix_based = {
    "http://www.w3.org/2001/XMLSchema": ["?range rdfs:label ?type"],
    "http://iec.ch/TC57/2010/CIM-schema-cim15": [
        "?range owl:equivalentClass ?class",
        "?class rdfs:label ?type",
    ],
}


class TypeMapper:
    def __init__(self, client: GraphDBClient, custom_additions: dict = None):
        self.prefixes = client.prefix_dict
        custom_additions = custom_additions if custom_additions is not None else {}
        self.map = {**sparql_type_map, **self.get_map(client), **custom_additions}

    @property
    def _query(self):
        select_query = "SELECT ?sparql_type ?type ?prefix"

        grouped_generals = [combine_statements(*g, split=" .\n") for g in generals]
        grouped_prefixes = [
            combine_statements(*v, f'FILTER (?prefix = "{k}")', split=" .\n")
            for k, v in prefix_based.items()
        ]
        grouped_prefix_general = combine_statements(*prefix_general, split=" .\n")
        unionized_generals = unionize(*grouped_generals)
        unionized_prefixes = unionize(*grouped_prefixes)

        full_prefixes = combine_statements(grouped_prefix_general, unionized_prefixes, group=True)
        full_union = unionize(unionized_generals, full_prefixes, group=False)
        return f"{select_query}\nWHERE\n{{\n{full_union}\n}}"

    @staticmethod
    def type_map(df: pd.DataFrame) -> dict:
        df["type"] = df["type"].str.lower()
        d = df.set_index("sparql_type").to_dict("index")
        return {k: python_type_map.get(v.get("type", "String")) for k, v in d.items()}

    @staticmethod
    def prefix_map(df: pd.DataFrame) -> dict:
        df = df.loc[~df["prefix"].isna()].head()
        df["comb"] = df["prefix"] + "#" + df["type"]
        df = df.drop_duplicates("comb")
        d2 = df.set_index("comb").to_dict("index")
        return {k: python_type_map.get(v.get("type", "String")) for k, v in d2.items()}

    def get_map(self, client: GraphDBClient) -> dict:
        df = client.get_table(self._query, map_data_types=False)
        if df.empty:
            return {}
        type_map = self.type_map(df)
        prefix_map = self.prefix_map(df)
        return {**type_map, **prefix_map}

    def get_type(self, prefix, missing_return="identity", custom_maps: dict = None):
        map = {**self.map, **custom_maps} if custom_maps is not None else self.map
        try:
            return map[prefix]
        except KeyError:
            warnings.warn(f"{prefix} not found in the sparql -> python type map")
            if missing_return == "identity":
                return lambda x: x
            else:
                return None

    def convert_dict(self, d: dict, drop_missing: bool = True, custom_maps: dict = None) -> dict:
        missing_return = "None" if drop_missing else "identity"
        base = {
            column: self.get_type(data_type, missing_return, custom_maps)
            for column, data_type in d.items()
        }
        if drop_missing:
            return {k: v for k, v in base.items() if v is not None}
        return base

    @staticmethod
    def map_base_types(df: pd.DataFrame, type_map: dict) -> pd.DataFrame:
        as_type_able_columns = {c for c, datatype in type_map.items() if datatype in as_type_able}
        df = df.astype({column: type_map[column] for column in as_type_able_columns})
        return df

    @staticmethod
    def map_exceptions(df: pd.DataFrame, type_map: dict) -> pd.DataFrame:
        ex_columns = {c for c, datatype in type_map.items() if datatype not in as_type_able}
        for column in ex_columns:
            df[column] = df[column].apply(type_map[column])
        return df

    def map_data_types(
        self, df: pd.DataFrame, data_row: dict, custom_maps: dict = None, columns: dict = None
    ) -> pd.DataFrame:
        columns = {} if columns is None else columns
        col_map = {
            column: data.get("datatype", data.get("type", None))
            for column, data in data_row.items()
            if column not in columns.keys()
        }
        type_map = {**self.convert_dict(col_map, custom_maps=custom_maps), **columns}

        df = self.map_base_types(df, type_map)
        df = self.map_exceptions(df, type_map)
        return df
