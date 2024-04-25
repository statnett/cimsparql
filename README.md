[![PyPI version](https://img.shields.io/pypi/v/cimsparql)](https://pypi.org/project/cimsparql/)
[![Python Versions](https://img.shields.io/pypi/pyversions/cimsparql)](https://pypi.org/project/cimsparql/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![](https://github.com/statnett/data_cache/workflows/Tests/badge.svg)](https://github.com/statnett/cimsparql/actions?query=workflow%3ATests)
[![codecov](https://codecov.io/gh/statnett/cimsparql/branch/master/graph/badge.svg)](https://codecov.io/gh/statnett/cimsparql)

# CIMSPARQL Query CIM data using sparql

This Python package provides functionality for reading cim data from
tripple stores such as GraphDB, BlazeGraph or Rdf4j into Python memory
as pandas dataframes.

The package provides a set of predefined functions/queries to load CIM
data such as generator, demand or branch data, though the user can
easiliy define their own queries.

## Usage

### Load data using predefined functions/queries

```python
>>> from cimsparql.graphdb import ServiceConfig
>>> from cimsparql.model import get_single_client_model
>>> model = get_single_client_model(ServiceConfig(limit=3))
>>> ac_lines = model.ac_lines()
>>> print(ac_lines[['name', 'x', 'r', 'bch']])
         name       x       r       bch
0  <branch 1>  1.9900  0.8800  0.000010
1  <branch 2>  1.9900  0.8800  0.000010
2  <branch 3>  0.3514  0.1733  0.000198
```

In the example above the client will query repo "<repo>" in the default server
[GraphDB](https://graphdb.ontotext.com) for AC line values.

### Inspect/view predefined queries

See the sparql templates folder (`cimsparql/sparql`) to the query used.

### Load data using user specified queries

```python
>>> from string import Template
>>> query = 'PREFIX cim:<${cim}>\nPREFIX rdf: <${rdf}>\nSELECT ?mrid where {?mrid rdf:type cim:ACLineSegment}'
>>> query_result = model.get_table_and_convert(model.template_to_query(Template(query)))
>>> print(query_result)
```

### Prefix and namespace

Available namespace for current graphdb client (`gdbc` in the examples above),
which can be used in queries (such as `rdf` and `cim`) can by found by

```python
>>> print(model.prefixes)
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

### Running Tests Against Docker Databases

Tests can be run agains RDF4J and/or BlazeGraph databases if a container with the correct images are available.

```
docker pull eclipse/rdf4j-workbench
docker pull openkbs/blazegraph
```

Launch one or both containers and specify the following environment variables

```
RDF4J_URL = "localhost:8080/rdf4j-server"
BLAZEGRAPH_URL = "localhost:9999/blazegraph/namespace
```
**Note 1**: The port numbers may differ depending on your local Docker configurations.
**Note 2**: You don't *have* to install RDF4J or BlazeGraph. Tests requiring these will be skipped in case
they are not available. They will in any case be run in the CI pipeline on GitHub (where both always are available).

### Ontology (for developers)

Ontologies for the CIM model can be found at (ENTSOE's webpages)[https://www.entsoe.eu/digital/common-information-model/cim-for-grid-models-exchange/].
For convenience and testing purposes the ontology are located under `tests/data/ontology`. CIM models used for testing purposes in Cimsparql should
be stored in N-quads format. In case you have a model in XML format it can be converted to N-quads by launching a DB (for example RDF4J) and upload
all the XML files and the ontology.

Execute

```sparql
PREFIX cims: <http://iec.ch/TC57/1999/rdf-schema-extensions-19990926#>

DELETE {?s ?p ?o}
INSERT {?s ?p ?o_cast} WHERE {
  ?s ?p ?o .
  ?p cims:dataType ?_dtype .
  ?_dtype cims:stereotype ?stereotype .
  BIND(IF(?stereotype = "Primitive",
    URI(concat("http://www.w3.org/2001/XMLSchema#", lcase(strafter(str(?_dtype), "#")))),
    ?_dtype) as ?dtype)
  BIND(STRDT(?o, ?dtype) as ?o_cast)
}
```
and export as N-quads.

### Test models

1. *micro_t1_nl*: `MicroGrid/Type1_T1/CGMES_v2.4.15_MicroGridTestConfiguration_T1_NL_Complete_v2`


### Rest APIs

CimSparql mainly uses `SparqlWrapper` to communicate with the databases. However, there are certain operations which are performed
directly via REST calls. Since there are small differences between different APIs you may have to specify which API you are using.
This can be done when initializing the `ServiceCfg` class or by specifying the `SPARQL_REST_API` environment variable. Currently,
`RDF4J` and `blazegraph` is supported (if not given `RDF4J` is default).

```bash
export SPARQL_REST_API=RDF4J  # To use RDF4J
export SPARQL_REST_API=BLAZEGRAPH  # To use BlazeGraph
```
