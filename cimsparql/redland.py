import json
from pathlib import PosixPath
from typing import Dict, Tuple

import pandas as pd

import RDF
from cimsparql.model import CimModel

name = {".n3": "ntriples", ".xml": "rdfxml"}


class Model(CimModel):
    def __init__(  # pylint: disable=too-many-arguments
        self,
        fname: PosixPath,
        new: bool = False,
        hash_type: str = "bdb",
        base_uri: str = "urn:snmst:",
        query_language: str = "sparql",
        mapper: CimModel = None,
        network_analysis: bool = True,
    ):
        super().__init__(
            fname=fname,
            base_uri=base_uri,
            mapper=mapper,
            new=new,
            hash_type=hash_type,
            query_language=query_language,
            network_analysis=network_analysis,
        )

    def _setup_client(
        self,
        query_language: str,
        new: bool,
        fname: PosixPath,
        hash_type: str,
        base_uri: str,
        **kwargs,
    ):
        self._query_language = query_language
        storage = RDF.HashStorage(fname.stem, self._option_string(new, hash_type, fname))
        self._model = RDF.Model(storage)

        if new or self.empty:
            parser = RDF.Parser(name=name[fname.suffix])
            parser.parse_into_model(self._model, fname.as_uri(), base_uri)
            self._prefixes = {
                name: str(uri).rstrip("#") for name, uri in parser.namespaces_seen().items()
            }
            with open(str(fname.with_suffix(".ns")), "w") as fid:
                json.dump(self.prefixes, fid)
        else:
            # Read namespace from file
            with open(str(fname.with_suffix(".ns")), "r") as fid:
                self._prefixes = json.load(fid)

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

    def _get_table(self, query: str, limit: int = None) -> Tuple[pd.DataFrame, Dict]:
        rdf_query = RDF.Query(
            self._query_with_header(query, limit), query_language=self._query_language
        )
        try:  # pylint: disable=unnecessary-comprehension
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
