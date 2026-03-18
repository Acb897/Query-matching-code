from TPF import run_query_strict

class TPFAdapter:

    def __init__(self, endpoint):
        self.endpoint = endpoint
        
    @staticmethod
    def normalize_iri(value: str) -> str:
        value = value.strip()
        if value.startswith("<") and value.endswith(">"):
            return value[1:-1]
        return value

    def exploratory_types(self):

        query = """
        SELECT DISTINCT ?type WHERE {
            ?s a ?type .
        }
        """

        repo = run_query_strict(query, [self.endpoint])

        return list(set(
            self.normalize_iri(str(o)) for s,p,o in repo
            if str(p).endswith("rdf-syntax-ns#type")
        ))

    # ------------------------------------------

    def outgoing_patterns(self, type_):
        type_ = self.normalize_iri(type_)
        query = f"""
        SELECT ?predicate ?object WHERE {{
            ?s a <{type_}> .
            ?s ?predicate ?object .
        }}
        """

        repo = run_query_strict(query, [self.endpoint])

        results = []

        for s,p,o in repo:
            results.append({
                "predicate": {"value": str(p)},
                "object_type": {"value": ""},
                "g": {"value": "urn:default-graph"},
                "subject_type": {"value": type_}
            })

        return results

    # ------------------------------------------

    def incoming_patterns(self, type_):
        type_ = self.normalize_iri(type_)

        query = f"""
        SELECT ?subject ?predicate WHERE {{
            ?subject ?predicate ?o .
            ?o a <{type_}> .
        }}
        """

        repo = run_query_strict(query, [self.endpoint])

        results = []

        for s,p,o in repo:
            results.append({
                "predicate": {"value": str(p)},
                "subject_type": {"value": ""},
                "g": {"value": "urn:default-graph"}
            })

        return results
    
    