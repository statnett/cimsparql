from cimsparql import query_support as qs


def test_region_query_empty():
    regions = qs.region_query(
        region=None, sub_region=False, container="", sub_geographical_region="?subgeoreg"
    )
    assert not regions


def test_region_query():
    regions = qs.region_query(
        region="NO", sub_region=False, container="", sub_geographical_region="?subgeoreg"
    )
    assert len(regions) == 3


def test_default_terminal_where_query():
    assert len(qs.terminal_where_query()) == 3


def test_terminal_where_query_no_var():
    assert len(qs.terminal_where_query(var=None)) == 2


def test_terminal_where_query_no_var_with_sequence():
    assert len(qs.terminal_where_query(cim_version=15, var=None, with_sequence_number=1)) == 3
