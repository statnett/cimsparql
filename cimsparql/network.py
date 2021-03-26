import copy
from typing import List, Set

import networkx as nx
import numpy as np
import pandas as pd


class Islands(nx.Graph):
    def __init__(self, connections: pd.DataFrame) -> None:
        super().__init__()
        self.add_edges_from(connections.to_numpy())
        self._groups = list(nx.connected_components(self))

    def reference_nodes(self, columns: List[str] = None) -> pd.DataFrame:
        columns = ["mrid", "ref_node"] if columns is None else columns
        keys = []
        values = []
        for group in self.groups():
            ref = list(group)[0]
            keys += list(group)
            values += [ref] * len(group)
        return pd.DataFrame(np.array([keys, values]).transpose(), columns=columns).set_index("mrid")

    def groups(self) -> List[Set]:
        return copy.deepcopy(self._groups)
