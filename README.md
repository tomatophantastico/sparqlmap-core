# SparqlMap - core library
[![Build Status](https://travis-ci.org/tomatophantastico/sparqlmap-core.svg?branch=develop)](https://travis-ci.org/tomatophantastico/sparqlmap-core) 

SparqlMap - A SPARQL to SQL rewriter based on [R2RML](http://www.w3.org/TR/r2rml/) specification.

It can be used in allows both extracting RDF from an relational database and rewrite SPARQL queries into SQL.

## Usage

This module contains the core functionality of SparqlMap.

It serves as the foundation for the [SparqlMap client](http://github.com/tomatophantastico/sparqlmap). 





## DB Support


### HSQL
There is no support for binary data types (xsd:binary) in HSQL due to problems with the query engine.
