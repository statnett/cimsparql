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
        return self.get_table_and_convert(query, index="mrid", limit=limit)

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

    def wind_generating_units(self, limit: int = None):
        query = queries.wind_generating_unit_query()
        float_list = ["maxP", "allocationMax", "allocationWeight", "minP"]
        columns = {var: float for var in float_list}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def synchronous_machines(
        self,
        synchronous_vars: List[str] = ["sn", "p", "q"],
        region: str = "NO",
        limit: int = None,
        connectivity: str = None,
    ) -> pd.DataFrame:
        query = queries.synchronous_machines_query(synchronous_vars, region, connectivity)

        float_list = ["maxP", "minP", "allocationMax", "allocationWeight"]
        float_list += synchronous_vars

        columns = {var: float for var in synchronous_vars + float_list}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def connections(
        self,
        rdf_types: List[str] = ["cim:Breaker", "cim:Disconnector"],
        region: str = "NO",
        limit: int = None,
        connectivity: bool = True,
    ):
        query = queries.connection_query(self._cim_version, rdf_types, region, connectivity)
        return self.get_table_and_convert(query, index="mrid", limit=limit)

    def ac_lines(
        self,
        region: str = "NO",
        limit: int = None,
        connectivity: str = None,
        rates: List[str] = ["Normal", "Warning", "Overload"],
    ) -> pd.DataFrame:
        query = queries.ac_line_query(self._cim_version, region, connectivity, rates)
        columns = {
            var: float
            for var in ["x", "r", "un", "bch", "length"] + [f"rate{rate}" for rate in rates]
        }
        return self.get_table_and_convert(query, limit=limit, columns=columns)

    def transformers(
        self,
        region: str = "NO",
        limit: int = None,
        connectivity: str = None,
        rates: List[str] = ["Normal", "Warning", "Overload"],
    ) -> pd.DataFrame:
        query = queries.transformer_query(region, connectivity, rates)
        columns = {var: float for var in ["x", "un"] + [f"rate{rate}" for rate in rates]}
        columns["endNumber"] = int
        return self.get_table_and_convert(query, limit=limit, columns=columns)

    def disconnected(self, index: str = None, limit: int = None) -> pd.DataFrame:
        query = ssh_queries.disconnected(self._cim_version)
        return self.get_table(query, index=index, limit=limit)

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
        columns = {"BaseVoltage": float}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def powerflow(self, power: List[str] = ["p", "q"], limit: int = None) -> pd.DataFrame:
        query = sv_queries.powerflow(power)
        columns = {x: float for x in power}
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

    @staticmethod
    def _assign_column_types(result, columns):
        for column, column_type in columns.items():
            if column_type is str:
                continue
            elif column_type is bool:
                result.loc[:, column] = result.loc[:, column].str.contains("True|true")
            else:
                result.loc[result[column] == "None", column] = ""
                try:
                    result.loc[:, column] = pd.to_numeric(result[column]).astype(column_type)
                except ValueError:
                    raise

    def get_table_and_convert(
        self, query: str, index: str = None, limit: int = None, columns: Dict = None
    ) -> pd.DataFrame:
        result = self.get_table(query, index=index, limit=limit)
        if len(result) > 0:
            if columns is None:
                columns = {}
            reset_index = index is not None
            if reset_index:
                result.reset_index(inplace=True)
            result = result.astype(str)
            self._assign_column_types(result, columns)
            if reset_index:
                result.set_index(index, inplace=True)
        return result
