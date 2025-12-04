"""Implementation of an in-memory GraphDBClient using pyoxigraph as engine."""

import re
from pathlib import Path

import httpx
from pyoxigraph import NamedNode, QueryResultsFormat, QuerySolutions, RdfFormat, Store

from cimsparql.graphdb import GraphDBClient, default_namespaces
from cimsparql.sparql_result_json import SparqlResultJson


class InvalidResultTypeError(RuntimeError):
    def __init__(self, result_type: str) -> None:
        super().__init__(f"Only queries resulting a solution of type 'QuerySolutions' are supported. Got {result_type}")


class LocalClient(GraphDBClient):
    def __init__(self, store: Store | None = None, strip_service_specifier: bool = False) -> None:
        self.strip_service_specifier = strip_service_specifier
        self.store = store or Store()
        super().__init__()

    def exec_query(self, query: str) -> SparqlResultJson:
        if self.strip_service_specifier:
            query = re.sub("SERVICE[^{]+", "", query)

        result = self.store.query(query, prefixes=self.prefixes)
        if not isinstance(result, QuerySolutions):
            raise InvalidResultTypeError(str(type(result)))

        serialized_result = result.serialize(format=QueryResultsFormat.JSON)
        assert serialized_result
        return SparqlResultJson.model_validate_json(serialized_result)

    def get_prefixes(self, http_transport: httpx.BaseTransport | None = None) -> dict[str, str]:
        _ = http_transport
        if self._prefixes is None:
            self._prefixes = default_namespaces()
        return self._prefixes

    def delete_repo(self) -> None:
        raise NotImplementedError(
            "LocalClient supports only one repository. Create a new client instead of deleting the repo"
        )

    def upload_rdf(self, content: Path | bytes, rdf_format: str, params: dict[str, str] | None = None) -> None:
        data_format = oxigraph_format(rdf_format)
        params = params or {}
        named_graph = NamedNode(params["graph"]) if "graph" in params else None
        base_iri = params.get("base_iri")
        if isinstance(content, Path):
            self.store.load(path=content, format=data_format, to_graph=named_graph, base_iri=base_iri)
        else:
            self.store.load(content, format=data_format, to_graph=named_graph, base_iri=base_iri)

    def update_query(self, query: str) -> None:
        _ = query
        raise NotImplementedError("Update query is currently not implemented")

    def get_namespace(self, prefix: str) -> str:
        return self.prefixes[prefix]

    def set_namespace(self, prefix: str, value: str) -> None:
        if self._prefixes is None:
            self._prefixes = {}
        self._prefixes[prefix] = value


def oxigraph_format(fmt: str) -> RdfFormat:
    format_map = {
        "rdf/xml": RdfFormat.RDF_XML,
        "n-triples": RdfFormat.N_TRIPLES,
        "turtle": RdfFormat.TURTLE,
        "n3": RdfFormat.N3,
        "n-quads": RdfFormat.N_QUADS,
        "json-ld": RdfFormat.JSON_LD,
        "trig": RdfFormat.TRIG,
    }
    if fmt not in format_map:
        raise ValueError(f"format must be one of {', '.join(format_map.keys())}")
    return format_map[fmt]
