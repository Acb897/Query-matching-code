from rdflib import Graph
from collections import defaultdict

class RDFDumpAdapter:

    RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

    def __init__(self, file_path):

        self.graph = Graph()
        self.graph.parse(file_path)

        self.type_index = defaultdict(set)
        self.spo_index = defaultdict(list)
        self.pos_index = defaultdict(list)

        for s,p,o in self.graph:

            s = str(s); p = str(p); o = str(o)

            self.spo_index[s].append((p,o))
            self.pos_index[o].append((s,p))

            if p == self.RDF_TYPE:
                self.type_index[s].add(o)

    # ------------------------------------------

    def exploratory_types(self):
        all_types = set()
        for t in self.type_index.values():
            all_types.update(t)
        return list(all_types)

    # ------------------------------------------

    def outgoing_patterns(self, type_):

        results = []

        for s, types in self.type_index.items():

            if type_ not in types:
                continue

            for p,o in self.spo_index[s]:

                obj_types = self.type_index.get(o, [""])

                for ot in obj_types:
                    results.append({
                        "predicate": {"value": p},
                        "object_type": {"value": ot},
                        "g": {"value": "urn:default-graph"},
                        "subject_type": {"value": type_}
                    })

        return results

    # ------------------------------------------

    def incoming_patterns(self, type_):

        results = []

        for s,p in self.pos_index[type_]:

            subject_types = self.type_index.get(s, [])

            for st in subject_types:
                results.append({
                    "predicate": {"value": p},
                    "subject_type": {"value": st},
                    "g": {"value": "urn:default-graph"}
                })

        return results