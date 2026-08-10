# SPARQL-to-SHACL Pattern Extractor & Generator

TODO:
* Make it work with TPF server (using my code), and RDF dumps

**A simple Ruby tool that discovers data patterns from one or more SPARQL endpoints and generates basic [SHACL](https://www.w3.org/TR/shacl/) shapes.**

This script performs lightweight schema mining by:

1. Discovering classes (`rdf:type`) present in the endpoint
2. Collecting outgoing and incoming properties for each class
3. Capturing explicit `rdf:type` values when they differ from the queried class
4. Generating one SHACL `.ttl` file per endpoint containing `sh:NodeShape`s with `sh:targetClass` and very basic property constraints

