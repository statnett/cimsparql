import RDF
import json

import pandas as pd

from pathlib import PosixPath
from typing import Dict, Tuple
from cimsparql.model import CimModel

name = {".n3": "ntriples", ".xml": "rdfxml"}


class Model(CimModel):
    def __init__(
        self,
        fname: PosixPath,
        new: bool = False,
        hash_type: str = "bdb",
        base_uri: str = "urn:snmst:",
        query_language: str = "sparql",
        mapper: CimModel = None,
    ):
        super().__init__(
            fname=fname,
            base_uri=base_uri,
            mapper=mapper,
            new=new,
            hash_type=hash_type,
            query_language=query_language,
        )

    def _load_from_source(
        self, query_language: str, new: bool, fname: PosixPath, hash_type: str, **kwargs
    ):
        self._query_language = query_language
        storage = RDF.HashStorage(fname.stem, self._option_string(new, hash_type, fname))
        self._model = RDF.Model(storage)

    def get_prefix_dict(self, new: bool, fname: PosixPath, base_uri: str, **kwargs):
        if new or self.empty:
            parser = RDF.Parser(name=name[fname.suffix])
            parser.parse_into_model(self._model, fname.as_uri(), base_uri)
            self.prefix_dict = {
                name: str(uri).rstrip("#") for name, uri in parser.namespaces_seen().items()
            }
            with open(str(fname.with_suffix(".ns")), "w") as fid:
                json.dump(self.prefix_dict, fid)
        else:
            # Read namespace from file
            with open(str(fname.with_suffix(".ns")), "r") as fid:
                self.prefix_dict = json.load(fid)

    def _option_string(
        self, new: bool, hash_type: str, fname: PosixPath, with_context: bool = False
    ):
        if new:
            option_string = "new='yes',"
        else:
            option_string = ""

        if with_context:
            option_string += "context='yes'"

        return option_string + f"hash-type='{hash_type}',dir='{fname.parent}'"

    def _get_table(
        self, query: str, index: str = None, limit: int = None
    ) -> Tuple[pd.DataFrame, Dict]:
        rdf_query = RDF.Query(self._query_str(query, limit), query_language=self._query_language)
        try:
            result = [res for res in rdf_query.execute(self._model)]
        except RDF.RedlandError:
            return pd.DataFrame([]), {}
        return pd.DataFrame(result), result[0]

    @staticmethod
    def _col_map(data_row, columns) -> Dict:
        raise NotImplementedError(
            f"cim xml file contains {data_row} and {columns}. This is not currently handled by "
            "cimsparql. Report this error to DataScience <datascience.drift@statnett.no>"
        )
        out = {}
        for r, node in data_row.items():
            if node.is_resource():
                out[r] = "uri"
            elif node.is_literal():
                pass
        return out, columns
