"""Provide value mappers."""

import pandas as pd


class ValueMapper:
    """Base value mapper."""

    def map(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


class MridMapper(ValueMapper):
    """Mrid mapper.

    Replaces all matches of uri_regex with an empty string in columns which constains mrid in the name
    """

    def __init__(self, uri_regex: str = "^([^#_|^#]+)(#_|#)") -> None:
        self.uri_regex = uri_regex

    def map(self, df: pd.DataFrame) -> pd.DataFrame:  # A003
        for col in filter(lambda col: "mrid" in col, df.columns):
            df[col] = df[col].str.replace(self.uri_regex, "", regex=True)
        return df
