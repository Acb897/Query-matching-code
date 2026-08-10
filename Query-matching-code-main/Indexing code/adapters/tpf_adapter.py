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


def _term_value(term):
    """Plain string value of a run_query_strict term dict, or the term
    itself if it is already a plain string (defensive fallback)."""
    if isinstance(term, dict):
        return term.get("value", "")
    return str(term) if term is not None else ""


def _is_uri_term(term) -> bool:
    """
    True only if `term` is a run_query_strict binding for a URI/IRI
    (term["type"] == "uri"). Blank nodes and literals are rejected here
    based on the actual SPARQL-JSON binding type, not on a string-prefix
    guess -- see TPF.execute_sparql_query(include_types=True).
    """
    return isinstance(term, dict) and term.get("type") == "uri"


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
    # Shared helper: build a type-index from raw (typed) triples
    # ------------------------------------------
    @staticmethod
    def _build_indices(repo):
        """
        Given a list of (s, p, o) run_query_strict term dicts, return:
          - type_of:  { entity_iri -> set of class IRIs }
                      (only rdf:type objects that are URI terms are
                      admitted as classes; blank-node/literal "types"
                      are dropped)
          - data:     [ (s, p, o) ] non-rdf:type triples only, as plain
                      string values
        """
        type_of = {}     # entity → {class, ...}
        data = []

        for s, p, o in repo:
            s_val = _term_value(s)
            p_val = _term_value(p)
            o_val = _term_value(o)

            if p_val == RDF_TYPE:
                if _is_uri_term(o):
                    type_of.setdefault(s_val, set()).add(o_val)
                # else: blank-node/literal rdf:type object -- not a
                # resolvable class, dropped rather than indexed.
            else:
                if not _is_metadata_predicate(p_val):
                    data.append((s_val, p_val, o_val))

        return type_of, data

    # ------------------------------------------
    def exploratory_types(self):
        query = "SELECT DISTINCT ?type WHERE { ?s a ?type . }"
        repo = run_query_strict(query, [self.endpoint])

        def is_valid_class(term):
            if not _is_uri_term(term):
                return False
            iri = _term_value(term).strip()
            return (
                iri != ""
                and "<>" not in iri
                and " " not in iri
                and not _is_metadata_class(iri)
            )

        return list(set(
            self.normalize_iri(_term_value(o))
            for s, p, o in repo
            if _term_value(p) == RDF_TYPE and is_valid_class(o)
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

            # Resolve the object's classes (may be empty → one result with None)
            obj_classes = type_of.get(o) or {None}

            for obj_class in obj_classes:
                # Skip metadata classes leaking through
                if obj_class and _is_metadata_class(obj_class):
                    continue

                key = (p, obj_class)
                if key in seen:
                    continue
                seen.add(key)

                results.append({
                    "predicate":    {"type": "uri", "value": p},
                    "object_type":  (
                        {"type": "uri", "value": obj_class}
                        if obj_class is not None else {}
                    ),
                    "g":            {"type": "uri", "value": "urn:default-graph"},
                    "subject_type": {"type": "uri", "value": type_},
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

            # Resolve the subject's classes (already URI-filtered by
            # _build_indices; an unresolved/blank-node subject type is
            # simply absent from type_of and therefore skipped below)
            subj_classes = type_of.get(s) or set()

            for subj_class in subj_classes:
                if _is_metadata_class(subj_class):
                    continue

                key = (subj_class, p)
                if key in seen:
                    continue
                seen.add(key)

                results.append({
                    "predicate":    {"type": "uri", "value": p},
                    "subject_type": {"type": "uri", "value": subj_class},
                    "g":            {"type": "uri", "value": "urn:default-graph"},
                })

        return results
