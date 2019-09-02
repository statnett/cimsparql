import pandas as pd

from cimsparql import queries, ssh_queries, tp_queries, sv_queries
from cimsparql.url import Prefix

from typing import Dict, List


class CimModel(Prefix):
    def _query_str(self, query: str, limit: int = None) -> str:
        q = f"{self.header_str()}\n\n{query}"
        if limit is not None:
            q += f" limit {limit}"
        return q

    def loads(self, conform: bool = True, region: str = "NO", limit: int = None) -> pd.DataFrame:
        query = queries.load_query(conform, region)
        return self.get_table(query, index="mrid", limit=limit)

    def synchronous_machines(self, region: str = "NO", limit: int = None) -> pd.DataFrame:
        query = queries.synchronous_machines_query(region)
        return self.get_table(query, index="mrid", limit=limit)

    def connections(self, rdf_type: str, region: str = "NO", limit: int = None):
        query = queries.connection_query(self._cim_version, rdf_type, region)
        return self.get_table(query, index="mrid", limit=limit)

    def ac_lines(self, region: str = "NO", limit: int = None) -> pd.DataFrame:
        query = queries.ac_line_query(cim_version=self._cim_version, region=region)
        columns = {var: float for var in ["x", "r", "un", "bch", "length"]}
        return self.get_table_and_convert(query, limit=limit, columns=columns)

    def transformers(self, region: str = "NO", limit: int = None) -> pd.DataFrame:
        query = queries.transformer_query(region)
        columns = {"endNumber": int, "x": float, "un": float}
        return self.get_table_and_convert(query, limit=limit, columns=columns)

    def ssh_disconnected(self, limit: int = None) -> pd.DataFrame:
        query = ssh_queries.disconnected()
        return self.get_table(query, limit=limit)

    def ssh_synchronous_machines(self, limit: int = None) -> pd.DataFrame:
        query = ssh_queries.synchronous_machines()
        columns = {"p": float, "q": float, "controlEnabled": bool}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def ssh_load(self, rdf_types: List[str] = None, limit: int = None) -> pd.DataFrame:
        if rdf_types is None:
            rdf_types = ["cim:ConformLoad", "cim:NonConformLoad"]
        query = ssh_queries.load(rdf_types)
        columns = {"p": float, "q": float}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def ssh_generating_unit(self, rdf_types: List[str] = None, limit: int = None) -> pd.DataFrame:
        if rdf_types is None:
            rdf_types = [f"cim:{unit}GeneratingUnit" for unit in ["Hydro", "Thermal", "Wind"]]
        query = ssh_queries.generating_unit(rdf_types)
        columns = {"normalPF": float}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def terminal(self, limit: int = None) -> pd.DataFrame:
        query = tp_queries.terminal(self._cim_version)
        columns = {"connected": bool}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def topological_node(self, limit: int = None) -> pd.DataFrame:
        query = tp_queries.topological_node()
        return self.get_table_and_convert(query, index="mrid", limit=limit)

    def powerflow(self, limit: int = None) -> pd.DataFrame:
        query = sv_queries.powerflow()
        columns = {"p": float, "q": float}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def voltage(self, limit: int = None) -> pd.DataFrame:
        query = sv_queries.voltage()
        columns = {"v": float, "angle": float}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def tapstep(self, limit: int = None) -> pd.DataFrame:
        query = sv_queries.tapstep()
        columns = {"position": float}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    @property
    def empty(self) -> bool:
        return self.get_table("SELECT * \n WHERE { ?s ?p ?o } limit 1").empty

    def get_table_and_convert(
        self, query: str, index: str = None, limit: int = None, columns: Dict = None
    ) -> pd.DataFrame:
        result = self.get_table(query, index=index, limit=limit)
        if len(result) > 0 and columns:
            for column, column_type in columns.items():
                result[column] = result[column].apply(str).astype(column_type)
        return result
