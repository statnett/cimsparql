# CIMSPARQL Query CIM data using sparql

This Python package provides functionality for reading/parsing cim
data from either xml files (requires installation of Redland) or
GraphDB into Python memory as pandas dataframes.

The package provides a set of predefined functions/queries to load CIM
data such generator or branch data, though the user can easiliy extend
or define their own queries.

# Installing

The module is available through artifactory using `pip install
cimsparql` (see [Pip with
artifactory](https://wiki.statnett.no/display/DATASCIENCE/Setting+up+certificates+and+artifactory)
) or using git clone and updating `$PYTHONPATH`.

# Usage


## Load data using predefined functions/queries
```python
>>> from cimsparql.graphdb import GraphDBClient
>>> gdbc = GraphDBClient()
>>> ac_lines = gdbc.ac_lines(limit=3)
>>> print(ac_lines[['name', 'x', 'r', 'bch']])
         name       x       r       bch
0  <branch 1>  1.9900  0.8800  0.000010
1  <branch 2>  1.9900  0.8800  0.000010
2  <branch 3>  0.3514  0.1733  0.000198
```

In the example above the client will query the default server
(https://graphdb.statnett.no/repositories/SNMST-Master1Repo-VERSION-LATEST)
for AC line values. To see the actual sparql used do the following:
```python
>>> from cimsparql.queries import ac_line_query
>>> cim_version = 15
>>> print(ac_line_query(cim_version))
```

Other predefined queries can be found in `cimsparql.queries`,
`cimsparql.ssh_queries`, `cimsparql.sv_queries` or
`cimsparql.tp_queries`. See also other functions of `GraphDBClient`.

## Load data using user spesified queries


```python
>>> query = 'SELECT ?mrid where { ?mrid rdf:type cim:ACLineSegment } limit 2'
>>> gdbc = GraphDBClient()
>>> query_result = gdbc.get_table(query)
>>> print(query_result)
```

## Prefix and namespace

Available namespace for current graphdb client (`gdbc` in the examples
above), which can be used in queries (such as `rdf` and `cim`) can by found by

```python
>>> gdbc = GraphDBClient()
>>> print(gdbc.ns())
{'wgs': 'http://www.w3.org/2003/01/geo/wgs84_pos#',
 'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
 'owl': 'http://www.w3.org/2002/07/owl#',
 'cim': 'http://iec.ch/TC57/2010/CIM-schema-cim15#',
 'gn': 'http://www.geonames.org/ontology#',
 'xsd': 'http://www.w3.org/2001/XMLSchema#',
 'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
 'SN': 'http://www.statnett.no/CIM-schema-cim15-extension#',
 'ALG': 'http://www.alstom.com/grid/CIM-schema-cim15-extension#'}
```
