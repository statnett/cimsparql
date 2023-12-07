from cimsparql.sparql_result_json import SparqlResultHead, SparqlResultValue


def test_populate_sparql_result_head_by_name():
    sparql_head_1 = SparqlResultHead(variables=["a", "b"], links=["l1", "l2"])

    sparql_head_2 = SparqlResultHead(vars=["a", "b"], links=["l1", "l2"])
    assert sparql_head_1 == sparql_head_2


def test_populate_sparql_result_value_by_name():
    sparql_value_1 = SparqlResultValue(value_type="literal", value="value")

    sparql_value_2 = SparqlResultValue(type="literal", value="value")
    assert sparql_value_1 == sparql_value_2
