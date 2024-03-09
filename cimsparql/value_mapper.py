import pandas as pd


class ValueMapper:
    def map(self, df: pd.DataFrame) -> pd.DataFrame:  # A003
        return df


class MridMapper(ValueMapper):
    """
    Mapper which replaces all matches of uri_regex with an empty string
    in columns which constains mrid in the name
    """

    def __init__(self, uri_regex: str = "^([^#_|^#]+)(#_|#)") -> None:
        self.uri_regex = uri_regex

    def map(self, df: pd.DataFrame) -> pd.DataFrame:  # A003
        for col in filter(lambda col: "mrid" in col, df.columns):
            df[col] = df[col].str.replace(self.uri_regex, "", regex=True)
        return df
