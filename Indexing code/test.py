# ------------------------------------------------------
# Import the engine
# ------------------------------------------------------
from indexer import Engine


# ------------------------------------------------------
# 1. Create the engine
# ------------------------------------------------------
engine = Engine()


# endpoints = [
#     "http://acb8computer:7200/repositories/SP2Bench",
#     "http://acb8computer:7200/repositories/watdiv",    # tarda mucho
#     "http://acb8computer:7200/repositories/berlin",
#     "http://acb8computer:7200/repositories/caresm-test",
#     "http://acb8computer:7200/repositories/largebench-chebi",  # tarda mucho
#     "http://acb8computer:7200/repositories/largebench-drugbank",
#     "http://acb8computer:7200/repositories/largebench-dbpedia",  # tarda mucho
#     "http://acb8computer:7200/repositories/largebench-geonames",
#     "http://acb8computer:7200/repositories/largebench-kegg",
#     "http://acb8computer:7200/repositories/largebench-linkedmdb",
#     "http://acb8computer:7200/repositories/largebench-newyorktimes"
# ]

endpoints = [
    "http://localhost:9999/blazegraph/namespace/caresm/sparql"
]


# ------------------------------------------------------
# 2. Extract patterns
# ------------------------------------------------------
print("Extracting SPO patterns…")

rdf_index = engine.extract_patterns(endpoints)


# ------------------------------------------------------
# 3. Generate SHACL files (one per endpoint)
# ------------------------------------------------------
print("Generating SHACL files…")

engine.shacl_generator(rdf_index, "shacl_output")


print("Done! SHACL files written to ./shacl_output/")