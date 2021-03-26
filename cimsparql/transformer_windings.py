from typing import List, Tuple

import numpy as np
import pandas as pd

from cimsparql.constants import con_mrid_str, ratings


def winding_from_three_tx(three_tx: pd.DataFrame, i: int) -> pd.DataFrame:
    columns = [col for col in three_tx.columns if col.endswith(f"_{i}") or col == "mrid"]
    winding = three_tx[columns]
    t_mrid = f"t_mrid_{i}"
    rename_columns = {
        column: "_".join(column.split("_")[:-1])
        for column in columns
        if column not in [t_mrid, "mrid"]
    }
    rename_columns[t_mrid] = "t_mrid_1"
    return winding.rename(columns=rename_columns)


def winding_list(three_tx: pd.DataFrame) -> List[pd.DataFrame]:
    return [winding_from_three_tx(three_tx, i) for i in [1, 2, 3]]


def three_tx_to_windings(three_tx: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    three_tx.reset_index(inplace=True)
    three_tx.rename(columns={"index": "mrid"}, inplace=True)
    windings = pd.concat(winding_list(three_tx), ignore_index=True)
    windings["b"] = np.divide(1.0, windings["x"])
    windings["ckt"] = windings["w_mrid"]
    windings["t_mrid_2"] = windings["mrid"]
    windings["bidzone_1"] = windings["bidzone_2"] = windings["bidzone"]
    return windings[cols]


def windings_set_end(windings: pd.DataFrame, i: int, cols: List[str]):
    columns = {f"{var}": f"{var}_{i}" for var in cols}
    return windings[windings["endNumber"] == i][["mrid"] + cols].rename(columns=columns)


def windings_to_tx(
    windings: pd.DataFrame, phase_tap_changers: pd.DataFrame
) -> Tuple[pd.DataFrame, ...]:
    """Split windings two-windings and three-windings

    Will also update provided phase_tap_changers dataframe with columne winding end one of
    transformer if not empty.

    Args:
        winding: All transformerends (windings)
        phase_tap_changers:

    Returns: Both two-winding and three-winding transformers (as three two-winding transformers).
    """
    possible_columns = ["name", "x", "un", "t_mrid", "w_mrid", "bidzone", con_mrid_str]
    rate_columns = [f"rate{rate}" for rate in ratings]
    cols = [col for col in possible_columns + rate_columns if col in windings.columns]

    # Three winding includes endNumber == 3
    three_winding_mrid = windings[windings["endNumber"] == 3]["mrid"]
    three_windings = windings.loc[windings["mrid"].isin(three_winding_mrid)]
    wd = [windings_set_end(three_windings, i, cols).set_index("mrid") for i in range(1, 4)]
    three_tx = pd.concat(wd, axis=1, sort=False)

    # While two winding transformers don't. Combine first and second winding.
    two_tx_group = windings[~windings["mrid"].isin(three_winding_mrid)].groupby("endNumber")
    two_tx = two_tx_group.get_group(1).set_index("mrid")
    two_tx_2 = two_tx_group.get_group(2).set_index("w_mrid")
    for col in ["t_mrid", "bidzone"]:
        two_tx.loc[two_tx_2["mrid"], f"{col}_2"] = two_tx_2[col].values
    two_tx.loc[two_tx_2["mrid"], "x"] += two_tx_2["x"].values

    if not phase_tap_changers.empty:
        phase_tap_changers["w_mrid_1"] = two_tx.loc[
            two_tx_2.loc[phase_tap_changers.index, "mrid"], "w_mrid"
        ].values

    two_tx = two_tx.reset_index().rename(
        columns={"w_mrid": "ckt", "t_mrid": "t_mrid_1", "bidzone": "bidzone_1"}
    )
    return two_tx, three_tx
