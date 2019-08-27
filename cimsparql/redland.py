import RDF

import pandas as pd

from pathlib import PosixPath
from typing import Dict

name = {".n3": "ntriples", ".xml": "rdfxml"}


class Model:
    def __init__(
        self, fname: PosixPath, new: bool = False, hash_type: str = "bdb", base_uri: str = None
    ):
        option_string = ""
        if new:
            option_string += "new='yes',"
        option_string += f"hash-type='{hash_type}',dir='{fname.parent}'"

        storage = RDF.HashStorage(fname.stem, options=option_string)
        self._model = RDF.Model(storage)
        if new or self.empty:
            parser = RDF.Parser(name=name[fname.suffix])
            parser.parse_into_model(self._model, fname.as_uri(), base_uri)

    @property
    def empty(self) -> bool:
        return self.get_table("SELECT * \n WHERE { ?s ?p ?o } limit 1").empty

    def get_table(
        self, query: str, query_language: str = "sparql", index: str = None
    ) -> pd.DataFrame:
        rdf_query = RDF.Query(query, query_language=query_language)
        result = pd.DataFrame([res for res in rdf_query.execute(self._model)])

        # Set index column if required
        if index:
            result.set_index(index, inplace=True)

        return result


def get_table_and_convert(model: Model, query: str, columns: Dict = None) -> pd.DataFrame:
    result = model.get_table(query)
    if len(result) > 0 and columns:
        for column, column_type in columns.items():
            result[column] = result[column].apply(str).astype(column_type)
    return result
