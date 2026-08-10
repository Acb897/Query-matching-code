import hashlib
import os
import re
from urllib.parse import urlparse
from SPARQLWrapper import SPARQLWrapper, JSON
from adapters.factory import AdapterFactory

# ==============================
# SPO Pattern Container
# ==============================
class SPO:

    def __init__(self, params=None):
        params = params or {}
        self.SPO_Subject = params.get("SPO_Subject", "")
        self.SPO_Predicate = params.get("SPO_Predicate", "")
        self.SPO_Object = params.get("SPO_Object", "")
        self.SPO_Graph = params.get("SPO_Graph", "urn:default-graph")


# ==============================
# Engine
# ==============================
class Engine:

    RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

    def __init__(self):
        self.hashed_patterns = set()
        self.patterns = {}
        self.endpoint_graph_mode = {}
        self.endpoint_patterns = {}

    # --------------------------------------------------
    # Detect if endpoint contains named graphs, the default graph, or both
    # --------------------------------------------------
    def detect_named_graphs(self, endpoint_URL):
        """
        Determines self.endpoint_graph_mode[endpoint_URL] as one of:
          "default" -- data found only in the default graph
          "named"   -- data found only inside named graphs
          "mixed"   -- data found in both
          "none"    -- neither ASK succeeded (empty or unreachable
                       endpoint); treated the same as "default" by
                       build_query, which is the cheapest/safest query
                       shape to fall back to.

        Two independent ASKs are required: a single
        `ASK { GRAPH ?g { ?s ?p ?o } }` (the previous approach) only
        tells you whether *any* named graph exists, and says nothing
        about whether the default graph is also populated. Treating
        every endpoint as either purely "named" or purely "default" from
        one boolean silently drops the default-graph portion of a mixed
        endpoint (or vice-versa).
        """

        print(f"\n[Engine] Detecting graph mode for {endpoint_URL}...")

        def ask(query):
            sparql = SPARQLWrapper(endpoint_URL)
            sparql.setMethod("POST")
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            return sparql.query().convert()["boolean"]

        try:
            has_default = ask("ASK { ?s ?p ?o . }")
        except Exception:
            has_default = None

        try:
            has_named = ask("ASK { GRAPH ?g { ?s ?p ?o . } }")
        except Exception:
            has_named = None

        if has_default and has_named:
            mode = "mixed"
        elif has_named:
            mode = "named"
        elif has_default:
            mode = "default"
        elif has_default is None and has_named is None:
            # Both ASKs failed outright (e.g. connection error) --
            # fall back to the cheapest query shape rather than guessing.
            mode = "default"
        else:
            # Both ASKs succeeded but the endpoint is empty.
            mode = "none"

        self.endpoint_graph_mode[endpoint_URL] = mode
        print(f" → Graph mode detected: {mode}")

    # --------------------------------------------------
    # Deduplication
    # --------------------------------------------------
    def in_database(self, s, p, o, g):

        digest = hashlib.sha256(f"{s}|{p}|{o}|{g}".encode()).hexdigest()

        if digest in self.hashed_patterns:
            return True

        self.hashed_patterns.add(digest)
        return False

    # --------------------------------------------------
    # Add pattern
    # --------------------------------------------------
    def add_triple_pattern(self, type_, s, p, o, g):
        """
        Record one structural pattern for class `type_`.

        Callers are responsible for resolving `s` and `o` to a class IRI
        *only* when the underlying RDF term is a URI/IRI (never a blank
        node or literal) -- see adapters.*. This method no longer makes
        that decision itself via a string prefix, since a class IRI is
        not required to use the http(s) scheme (e.g. urn:, doi:, ark:).
        A blank-node or otherwise unresolved class is expected to arrive
        here as an empty string and is rejected below on that basis.
        """

        s = str(s).strip()
        p = str(p).strip()
        o = str(o).strip()
        g = str(g).strip() if str(g).strip() else "urn:default-graph"

        if not p:
            return

        if not s:
            return
        if p == self.RDF_TYPE and not o:
            return

        if p == self.RDF_TYPE and o == type_:
            return

        if type_ not in self.patterns:
            self.patterns[type_] = []

        self.patterns[type_].append(
            SPO({
                "SPO_Subject": s,
                "SPO_Predicate": p,
                "SPO_Object": o,
                "SPO_Graph": g
            })
        )

    # --------------------------------------------------
    # Graph scoping helper
    # --------------------------------------------------
    def _graph_scope(self, graph_mode, content, graph_var, bind_graph_var=False):
        """
        Wrap `content` (a graph-pattern fragment with no GRAPH clause of
        its own) so that it matches regardless of which graph it is
        actually stored in, according to `graph_mode`:

          "default"        -> content must match in the default graph
          "named"          -> content must match inside GRAPH ?<graph_var>
          "mixed" / "none" -> content may match in EITHER the default
                               graph OR any named graph -- evaluated
                               independently for this fragment only.

        Each call site supplies its own `graph_var`. This is the key
        difference from the previous implementation, which wrapped an
        entire BGP in a single `GRAPH ?g { ... }` block: every triple
        pattern was then forced to bind the SAME named graph, so a
        subject typed in one named graph but linked to another resource
        by a predicate asserted in a *different* named graph was
        invisible (the join on ?g could never succeed). Giving the
        subject-type clause, the data-triple clause, and the object-type
        clause each their own graph variable lets them resolve in
        different graphs independently, so such cross-graph structural
        relationships are still discovered.

        `bind_graph_var`: only the data-triple clause's graph variable
        (?g) is actually SELECTed and used for deduplication in
        in_database(); the type-assertion clauses' graph variables are
        throwaway and left unbound in the default-graph branch. Set
        True only for the clause whose graph_var is projected.
        """
        default_branch = content
        if bind_graph_var:
            default_branch = (
                f'{content}\nBIND(IRI("urn:default-graph") AS ?{graph_var})'
            )
        named_branch = f"GRAPH ?{graph_var} {{ {content} }}"

        if graph_mode == "default":
            return default_branch
        elif graph_mode == "named":
            return named_branch
        else:  # "mixed" or "none" -- check both, independently
            return f"{{ {default_branch} }} UNION {{ {named_branch} }}"

    # --------------------------------------------------
    # Query Builder
    # --------------------------------------------------
    def build_query(self, endpoint_URL, mode, type_=None):

        graph_mode = self.endpoint_graph_mode.get(endpoint_URL, "mixed")

        if mode == "exploratory":

            scoped = self._graph_scope(
                graph_mode, "?subject a ?type .", "g", bind_graph_var=True
            )

            return f"""
            SELECT DISTINCT ?type ?g
            WHERE {{
              {scoped}
            }}
            """

        elif mode == "fixed_subject":

            # NOTE: `?subject a ?subject_type` was previously joined in here
            # but is never consumed downstream (process_type only reads
            # object_type for the outgoing direction) -- it only inflated
            # the result set by the number of rdf:type assertions on each
            # subject. Removed, and DISTINCT added, so the transferred
            # volume tracks the number of distinct (predicate, object
            # class) pairs -- i.e. P+(c) as defined -- rather than the
            # number of triples times the subject's type multiplicity.
            type_clause = self._graph_scope(
                graph_mode, f"?subject a <{type_}> .", "tg"
            )
            data_clause = self._graph_scope(
                graph_mode, "?subject ?predicate ?object .", "g",
                bind_graph_var=True,
            )
            object_type_clause = self._graph_scope(
                graph_mode, "?object a ?object_type .", "og"
            )

            return f"""
            SELECT DISTINCT ?predicate ?object_type ?g
            WHERE {{
              {type_clause}
              {data_clause}
              OPTIONAL {{ {object_type_clause} }}
            }}
            """

        elif mode == "fixed_object":

            object_type_clause = self._graph_scope(
                graph_mode, f"?object a <{type_}> .", "tg"
            )
            data_clause = self._graph_scope(
                graph_mode, "?subject ?predicate ?object .", "g",
                bind_graph_var=True,
            )
            subject_type_clause = self._graph_scope(
                graph_mode, "?subject a ?subject_type .", "sg"
            )

            return f"""
            SELECT DISTINCT ?subject_type ?predicate ?g
            WHERE {{
              {object_type_clause}
              {data_clause}
              OPTIONAL {{ {subject_type_clause} }}
            }}
            """

    # --------------------------------------------------
    # Execute query
    # --------------------------------------------------
    def query_endpoint(self, endpoint_URL, mode, type_=None):

        print(f" [Engine] Executing {mode} query for {type_ if type_ else 'N/A'}...")

        sparql = SPARQLWrapper(endpoint_URL)
        sparql.setMethod("POST")
        sparql.setReturnFormat(JSON)

        query = self.build_query(endpoint_URL, mode, type_)
        sparql.setQuery(query)

        try:
            results = sparql.query().convert()

            bindings = results["results"]["bindings"]
            count = len(bindings)

            print(f" → {count} row(s) received.")

            return bindings

        except Exception as e:

            print(f" [Engine] SPARQL error on {endpoint_URL}: {e}")
            return []

    # --------------------------------------------------
    # Extract patterns
    # --------------------------------------------------
    def extract_patterns(self, sources, mode="sparql"):

        self.endpoint_patterns = {}
        print(f"\n[Engine] Starting pattern extraction")
        print(f"          → Mode: {mode.upper()}")
        print(f"          → Sources: {len(sources)} endpoint(s)")
        for src in sources:
            print(f"            - {src}")

        for source in sources:

            adapter = AdapterFactory.create(source, mode, self)

            # ⚠️ pass engine only for SPARQL
            if mode == "sparql":
                adapter.engine = self

            self.patterns = {}
            self.hashed_patterns = set()

            print("\n[Engine] Phase 1: exploratory scan...")
            types = adapter.exploratory_types()

            print(f" → Detected {len(types)} classes.")

            print("[Engine] Phase 2: expansion...")

            from concurrent.futures import ThreadPoolExecutor

            def _is_uri_binding(binding):
                """
                True only if `binding` is a SPARQL-JSON binding dict for a
                URI/IRI term (binding["type"] == "uri"). Blank nodes
                ("bnode") and literals ("literal"/"typed-literal") are
                rejected here so that a class value is never accepted on
                the basis of its string prefix (see add_triple_pattern).
                Adapters that do not speak SPARQL-JSON natively (dump,
                TPF) are expected to populate this same "type" key
                themselves -- see adapters/dump_adapter.py.
                """
                return bool(binding) and binding.get("type") == "uri"

            def process_type(type_):

                # outgoing
                for sol in adapter.outgoing_patterns(type_):

                    g = sol.get("g", {}).get("value", "urn:default-graph")
                    p = sol.get("predicate", {}).get("value", "")
                    o_binding = sol.get("object_type", {})
                    o = o_binding.get("value", "") if _is_uri_binding(o_binding) else ""

                    if not self.in_database(type_, p, o, g):
                        self.add_triple_pattern(type_, type_, p, o, g)

                # incoming
                for sol in adapter.incoming_patterns(type_):

                    s_binding = sol.get("subject_type", {})
                    s = s_binding.get("value", "") if _is_uri_binding(s_binding) else ""
                    p = sol.get("predicate", {}).get("value", "")
                    g = sol.get("g", {}).get("value", "urn:default-graph")

                    if not s:
                        # Subject type unresolved or not a URI (blank node,
                        # literal-typed anomaly) -- an incoming relationship
                        # cannot be attached to an unnamed neighbour class,
                        # so it is dropped rather than silently mis-typed.
                        continue

                    if not self.in_database(s, p, type_, g):
                        self.add_triple_pattern(type_, s, p, type_, g)

            with ThreadPoolExecutor(max_workers=6) as pool:
                pool.map(process_type, types)

            self.endpoint_patterns[source] = self.patterns

        return self.endpoint_patterns

    # --------------------------------------------------
    # SHACL Generator
    # --------------------------------------------------
    def shacl_generator(self, patterns_hash, output_dir):

        print(f"\n[Engine] Generating SHACL files in {output_dir}...")

        os.makedirs(output_dir, exist_ok=True)

        for url, patterns in patterns_hash.items():

            print(f" [Engine] Building SHACL for {url}...")

            shacl = []

            shacl.append("""@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
@prefix dct:  <http://purl.org/dc/terms/> .
""")

            grouped = {}

            for _, values in patterns.items():
                for pattern in values:
                    grouped.setdefault(pattern.SPO_Subject, []).append(pattern)

            for subject, lst in grouped.items():

                shacl.append(f"<{subject}Shape>\n")
                shacl.append("  a sh:NodeShape ;\n")
                shacl.append(f"  sh:targetClass <{subject}> ;\n")
                shacl.append(f"  dct:source <{url}> ;\n")

                grouped_props = {}

                for pat in lst:
                    key = (pat.SPO_Predicate, pat.SPO_Object.strip())
                    grouped_props.setdefault(key, []).append(pat)

                items = list(grouped_props.items())

                for idx, ((predicate, object_str), _) in enumerate(items):

                    if predicate == self.RDF_TYPE and not object_str:
                        continue

                    shacl.append("  sh:property [\n")

                    if predicate == self.RDF_TYPE and object_str:

                        shacl.append("    sh:path rdf:type ;\n")
                        shacl.append(f"    sh:hasValue <{object_str}> ;\n")

                    else:

                        shacl.append(f"    sh:path <{predicate}> ;\n")

                        if object_str and object_str != "urn:default-graph":
                            shacl.append(f"    sh:class <{object_str}> ;\n")

                    end = "." if idx == len(items) - 1 else ";"

                    shacl.append(f"  ]{end}\n")

                shacl.append("\n")

            uri = urlparse(url)

            host = re.sub(r"[^a-zA-Z0-9]", "_", uri.hostname or "unknown")
            path = re.sub(r"[^a-zA-Z0-9]", "_", uri.path if uri.path else "root")

            base = re.sub(r"_+", "_", f"{host}{path}").strip("_")

            filename = f"{base}.ttl"

            output_path = os.path.join(output_dir, filename)

            if os.path.exists(output_path):

                short = hashlib.sha256(url.encode()).hexdigest()[:6]

                filename = f"{base}_{short}.ttl"
                output_path = os.path.join(output_dir, filename)

            print(f" → Writing {output_path}")

            with open(output_path, "w", encoding="utf-8") as f:
                f.write("".join(shacl))

        return True