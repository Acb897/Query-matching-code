class SPARQLAdapter:

    def __init__(self, endpoint, engine):
        self.endpoint = endpoint
        self.engine = engine

    def exploratory_types(self):

        results = self.engine.query_endpoint(self.endpoint, "exploratory")

        return list({
            r.get("type", {}).get("value")
            for r in results if r.get("type")
        })

    def outgoing_patterns(self, type_):
        return self.engine.query_endpoint(self.endpoint, "fixed_subject", type_)

    def incoming_patterns(self, type_):
        return self.engine.query_endpoint(self.endpoint, "fixed_object", type_)