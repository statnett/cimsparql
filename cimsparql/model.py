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

    def bus_data(self, region: str = "NO", limit: int = None) -> pd.DataFrame:
        query = queries.bus_data(region)
        columns = {var: str for var in ["mrid", "name"]}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def loads(
        self,
        load_type: List[str],
        load_vars: List[str] = ["p", "q"],
        region: str = "NO",
        limit: int = None,
        connectivity: str = None,
    ) -> pd.DataFrame:
        query = queries.load_query(load_type, load_vars, region, connectivity)
        columns = {var: float for var in load_vars}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def synchronous_machines(
        self,
        synchronous_vars: List[str] = ["sn", "p", "q"],
        region: str = "NO",
        limit: int = None,
        connectivity: str = None,
    ) -> pd.DataFrame:
        query = queries.synchronous_machines_query(synchronous_vars, region, connectivity)
        columns = {var: float for var in synchronous_vars}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def connections(
        self, rdf_type: str, region: str = "NO", limit: int = None, connectivity: str = None
    ):
        query = queries.connection_query(self._cim_version, rdf_type, region, connectivity)
        columns = {var: str for var in ["mrid", "t_mrid_1", "t_mrid_2"]}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def ac_lines(
        self, region: str = "NO", limit: int = None, connectivity: str = None
    ) -> pd.DataFrame:
        query = queries.ac_line_query(self._cim_version, region, connectivity)
        columns = {var: float for var in ["x", "r", "un", "bch", "length"]}
        columns["mrid"] = columns["t_mrid_1"] = columns["t_mrid_2"] = str
        return self.get_table_and_convert(query, limit=limit, columns=columns)

    def transformers(
        self, region: str = "NO", limit: int = None, connectivity: str = None
    ) -> pd.DataFrame:
        query = queries.transformer_query(region, connectivity)
        columns = {"endNumber": int, "x": float, "un": float, "t_mrid": str, "mrid": str}
        return self.get_table_and_convert(query, limit=limit, columns=columns)

    def ssh_disconnected(self, limit: int = None) -> pd.DataFrame:
        query = ssh_queries.disconnected()
        return self.get_table(query, limit=limit)

    def ssh_synchronous_machines(self, limit: int = None) -> pd.DataFrame:
        query = ssh_queries.synchronous_machines()
        columns = {"p": float, "q": float, "controlEnabled": bool, "mrid": str}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def ssh_load(self, rdf_types: List[str] = None, limit: int = None) -> pd.DataFrame:
        if rdf_types is None:
            rdf_types = ["cim:ConformLoad", "cim:NonConformLoad"]
        query = ssh_queries.load(rdf_types)
        columns = {"p": float, "q": float, "mrid": str}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def ssh_generating_unit(self, rdf_types: List[str] = None, limit: int = None) -> pd.DataFrame:
        if rdf_types is None:
            rdf_types = [f"cim:{unit}GeneratingUnit" for unit in ["Hydro", "Thermal", "Wind"]]
        query = ssh_queries.generating_unit(rdf_types)
        columns = {"normalPF": float}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def terminal(self, limit: int = None) -> pd.DataFrame:
        query = tp_queries.terminal(self._cim_version)
        columns = {"connected": bool, "mrid": str, "tp_node": str}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def topological_node(self, limit: int = None) -> pd.DataFrame:
        query = tp_queries.topological_node()
        columns = {"mrid": str, "name": str, "ConnectivityNodeContainer": str, "BaseVoltage": float}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def powerflow(self, power: List[str] = ["p", "q"], limit: int = None) -> pd.DataFrame:
        query = sv_queries.powerflow(power)
        columns = {x: float for x in power}
        columns["mrid"] = str
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def voltage(self, voltage_vars: List[str] = ["v", "angle"], limit: int = None) -> pd.DataFrame:
        query = sv_queries.voltage(voltage_vars)
        columns = {x: float for x in voltage_vars}
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
                if column == index:
                    result.reset_index(inplace=True)

                result[column] = result[column].apply(str)

                if not isinstance(column_type, str):
                    try:
                        result[column] = result[column].apply(column_type)
                    except ValueError:
                        if (result[column] == "None").all():
                            result[column] = None

        if len(result) > 0 and isinstance(columns, dict) and index in columns:
            result.set_index(index, inplace=True)
        return result
