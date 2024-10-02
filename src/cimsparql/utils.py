import re


def query_name(query: str) -> str:
    """
    Extract the name of the query provided that the first line starts with # Name: <name>.
    If no match is found, an empty string is returned
    """
    m = re.search("^# Name: ([a-zA-Z0-9 ]+)", query)
    return m.group(1) if m else ""
