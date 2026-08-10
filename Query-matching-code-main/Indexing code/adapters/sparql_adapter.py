class SPARQLAdapter:

    def __init__(self, endpoint, engine):
        self.endpoint = endpoint
        self.engine = engine

    def exploratory_types(self):

        results = self.engine.query_endpoint(self.endpoint, "exploratory")

        # Only accept classes bound as a URI/IRI term. A blank-node class
        # (binding["type"] == "bnode") is excluded because it has no
        # stable, cross-repository identity to match against a query's
        # typed variables -- see add_triple_pattern in indexer.py.
        return list({
            r["type"]["value"]
            for r in results
            if r.get("type") and r["type"].get("type") == "uri"
        })

    def outgoing_patterns(self, type_):
        return self.engine.query_endpoint(self.endpoint, "fixed_subject", type_)

    def incoming_patterns(self, type_):
        return self.engine.query_endpoint(self.endpoint, "fixed_object", type_)
