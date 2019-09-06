import RDF
import json

import pandas as pd

from pathlib import PosixPath

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
    ):
        self._query_language = query_language
        storage = RDF.HashStorage(fname.stem, self._option_string(new, hash_type, fname))
        self._model = RDF.Model(storage)
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

        self.set_cim_version()

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

    def get_table(self, query: str, index: str = None, limit: int = None) -> pd.DataFrame:
        rdf_query = RDF.Query(self._query_str(query, limit), query_language=self._query_language)
        result = pd.DataFrame([res for res in rdf_query.execute(self._model)])

        # Set index column if required
        if len(result) > 0 and index:
            result.set_index(index, inplace=True)

        return result
