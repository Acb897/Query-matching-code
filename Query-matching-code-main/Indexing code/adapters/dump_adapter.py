from rdflib import Graph, URIRef
from collections import defaultdict


class RDFDumpAdapter:

    RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

    def __init__(self, file_path):

        self.graph = Graph()
        self.graph.parse(file_path)

        # type_index: entity IRI -> set of class IRIs (rdf:type objects
        # that are themselves URIRefs; blank-node classes are excluded,
        # see note below)
        self.type_index = defaultdict(set)
        self.spo_index = defaultdict(list)
        self.pos_index = defaultdict(list)

        for s, p, o in self.graph:

            # Decide URI-vs-blank-node/literal from the actual rdflib term
            # *before* stringifying, so a blank node is never mistaken for
            # a resolvable IRI just because it stringifies without an
            # "http" prefix check. Non-URIRef subjects/objects are still
            # indexed for SPO/POS traversal (a blank-node subject can
            # still carry outgoing data properties), but are never
            # eligible to populate type_index as a *class*.
            s_is_uri = isinstance(s, URIRef)
            o_is_uri = isinstance(o, URIRef)

            s_str = str(s)
            p_str = str(p)
            o_str = str(o)

            self.spo_index[s_str].append((p_str, o_str))
            self.pos_index[o_str].append((s_str, p_str))

            if p_str == self.RDF_TYPE and o_is_uri:
                self.type_index[s_str].add(o_str)

    # ------------------------------------------

    def exploratory_types(self):
        # Already URI-filtered at index-build time (see __init__): only
        # rdf:type objects that were URIRef instances were admitted into
        # type_index, so nothing further to check here. Returned as plain
        # class-IRI strings, matching the SPARQLAdapter contract used by
        # Engine.process_type.
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

            for p, o in self.spo_index[s]:

                obj_types = self.type_index.get(o) or {None}

                for ot in obj_types:
                    results.append({
                        "predicate": {"type": "uri", "value": p},
                        "object_type": (
                            {"type": "uri", "value": ot}
                            if ot is not None else {}
                        ),
                        "g": {"type": "uri", "value": "urn:default-graph"},
                        "subject_type": {"type": "uri", "value": type_},
                    })

        return results

    # ------------------------------------------

    def incoming_patterns(self, type_):

        results = []

        for s, p in self.pos_index[type_]:

            subject_types = self.type_index.get(s) or set()

            for st in subject_types:
                results.append({
                    "predicate": {"type": "uri", "value": p},
                    "subject_type": {"type": "uri", "value": st},
                    "g": {"type": "uri", "value": "urn:default-graph"},
                })

        return results
