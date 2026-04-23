from TPF import run_query_strict

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

# Metadata predicates that should never appear as shape properties
METADATA_PREFIXES = (
    "http://www.w3.org/ns/hydra/core#",
    "http://rdfs.org/ns/void#",
    "http://www.w3.org/ns/sparql-service-description#",
)

def _is_metadata_predicate(p: str) -> bool:
    return any(p.startswith(ns) for ns in METADATA_PREFIXES)

def _is_metadata_class(c: str) -> bool:
    return any(c.startswith(ns) for ns in METADATA_PREFIXES)


class TPFAdapter:
    def __init__(self, endpoint):
        self.endpoint = endpoint

    @staticmethod
    def normalize_iri(value: str) -> str:
        value = value.strip()
        if value.startswith("<") and value.endswith(">"):
            return value[1:-1]
        return value

    # ------------------------------------------
    # Shared helper: build a type-index from raw triples
    # ------------------------------------------
    @staticmethod
    def _build_indices(repo):
        """
        Given a list of (s, p, o) string triples, return:
          - type_of:  { entity_iri -> set of class IRIs }
          - data:     [ (s, p, o) ] non-rdf:type triples only
        """
        type_of = {}     # entity → {class, ...}
        data = []

        for s, p, o in repo:
            s, p, o = str(s), str(p), str(o)
            if p == RDF_TYPE:
                type_of.setdefault(s, set()).add(o)
            else:
                if not _is_metadata_predicate(p):
                    data.append((s, p, o))

        return type_of, data

    # ------------------------------------------
    def exploratory_types(self):
        query = "SELECT DISTINCT ?type WHERE { ?s a ?type . }"
        repo = run_query_strict(query, [self.endpoint])

        def is_valid_class(iri):
            iri = str(iri).strip()
            return (
                iri.startswith("http")
                and iri != ""
                and "<>" not in iri
                and " " not in iri
                and not _is_metadata_class(iri)
            )

        return list(set(
            self.normalize_iri(str(o))
            for s, p, o in repo
            if str(p) == RDF_TYPE and is_valid_class(o)
        ))

    # ------------------------------------------
    def outgoing_patterns(self, type_):
        """
        For every triple   ?s  ?predicate  ?object
        where ?s is of type <type_>, also look up ?object's type
        so the Engine can write sh:class.
        """
        type_ = self.normalize_iri(type_)
        query = f"""
        SELECT ?subject ?predicate ?object WHERE {{
            ?subject a <{type_}> .
            ?subject ?predicate ?object .
            OPTIONAL {{ ?object a ?objectType . }}
        }}
        """
        repo = run_query_strict(query, [self.endpoint])
        type_of, data = self._build_indices(repo)

        # Identify which subjects are actually of type_
        instances = {
            s for s, types in type_of.items()
            if type_ in types
        }

        results = []
        seen = set()

        for s, p, o in data:
            if s not in instances:
                continue

            # Resolve the object's classes (may be empty → one result with "")
            obj_classes = type_of.get(o, {""})
            if not obj_classes:
                obj_classes = {""}

            for obj_class in obj_classes:
                # Skip metadata classes leaking through
                if obj_class and _is_metadata_class(obj_class):
                    continue

                key = (p, obj_class)
                if key in seen:
                    continue
                seen.add(key)

                results.append({
                    "predicate":    {"value": p},
                    "object_type":  {"value": obj_class},
                    "g":            {"value": "urn:default-graph"},
                    "subject_type": {"value": type_},
                })

        return results

    # ------------------------------------------
    def incoming_patterns(self, type_):
        """
        For every triple   ?subject  ?predicate  ?o
        where ?o is of type <type_>, also look up ?subject's type.
        """
        type_ = self.normalize_iri(type_)
        query = f"""
        SELECT ?subject ?predicate ?object WHERE {{
            ?subject ?predicate ?object .
            ?object a <{type_}> .
            OPTIONAL {{ ?subject a ?subjectType . }}
        }}
        """
        repo = run_query_strict(query, [self.endpoint])
        type_of, data = self._build_indices(repo)

        # Identify which objects are actually of type_
        targets = {
            s for s, types in type_of.items()
            if type_ in types
        }

        results = []
        seen = set()

        for s, p, o in data:
            if o not in targets:
                continue

            # Resolve the subject's classes
            subj_classes = type_of.get(s, {""})
            if not subj_classes:
                subj_classes = {""}

            for subj_class in subj_classes:
                if subj_class and _is_metadata_class(subj_class):
                    continue

                # Skip patterns where we cannot determine the subject type
                # (avoids the empty <Shape> problem)
                if not subj_class or not subj_class.startswith("http"):
                    continue

                key = (subj_class, p)
                if key in seen:
                    continue
                seen.add(key)

                results.append({
                    "predicate":    {"value": p},
                    "subject_type": {"value": subj_class},
                    "g":            {"value": "urn:default-graph"},
                })

        return results