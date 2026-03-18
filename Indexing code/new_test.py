from indexer import Engine



engine = Engine()

# SPARQL (default)
engine.extract_patterns(["http://localhost:9999/blazegraph/namespace/caresm/sparql"])
engine.shacl_generator(engine.endpoint_patterns, "shacl_output")

# TPF
engine.extract_patterns(["http://localhost:3000/caresm-sparql"], mode="tpf")
engine.shacl_generator(engine.endpoint_patterns, "shacl_output")

# # RDF dump
# engine.extract_patterns(["data.ttl"], mode="dump")