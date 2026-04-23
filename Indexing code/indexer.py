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
    # Detect if endpoint contains named graphs
    # --------------------------------------------------
    def detect_named_graphs(self, endpoint_URL):

        print(f"\n[Engine] Detecting named graph support for {endpoint_URL}...")

        sparql = SPARQLWrapper(endpoint_URL)
        sparql.setMethod("POST")

        ask_query = """
        ASK {
          GRAPH ?g {
            ?s ?p ?o .
          }
        }
        """

        sparql.setQuery(ask_query)
        sparql.setReturnFormat(JSON)

        try:
            result = sparql.query().convert()
            value = result["boolean"]

            self.endpoint_graph_mode[endpoint_URL] = "named" if value else "default"

            print(f" → Graph mode detected: {self.endpoint_graph_mode[endpoint_URL]}")

        except Exception:
            self.endpoint_graph_mode[endpoint_URL] = "default"
            print(" → ASK failed; defaulting to :default")

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

        s = str(s).strip()
        p = str(p).strip()
        o = str(o).strip()
        g = str(g).strip() if str(g).strip() else "urn:default-graph"

        if not p:
            return

        if not s or not s.startswith("http"):
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
    # Query Builder
    # --------------------------------------------------
    def build_query(self, endpoint_URL, mode, type_=None):

        graph_mode = self.endpoint_graph_mode.get(endpoint_URL)

        if graph_mode == "named":
            graph_pattern = "GRAPH ?g { %CONTENT% }"
        else:
            graph_pattern = '%CONTENT%\nBIND(IRI("urn:default-graph") AS ?g)'

        if mode == "exploratory":

            content = "?subject a ?type ."

            return f"""
            SELECT ?type ?g
            WHERE {{
              {graph_pattern.replace("%CONTENT%", content)}
            }}
            """

        elif mode == "fixed_subject":

            content = f"""
            ?subject a <{type_}> .
            ?subject a ?subject_type .
            ?subject ?predicate ?object .
            OPTIONAL {{ ?object a ?object_type . }}
            """

            return f"""
            SELECT ?subject_type ?predicate ?object_type ?g
            WHERE {{
              {graph_pattern.replace("%CONTENT%", content)}
            }}
            """

        elif mode == "fixed_object":

            content = f"""
            ?object a <{type_}> .
            ?subject ?predicate ?object .
            OPTIONAL {{ ?subject a ?subject_type . }}
            """

            return f"""
            SELECT ?subject_type ?predicate ?g
            WHERE {{
              {graph_pattern.replace("%CONTENT%", content)}
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

            def process_type(type_):

                # outgoing
                for sol in adapter.outgoing_patterns(type_):

                    g = sol.get("g", {}).get("value", "urn:default-graph")
                    p = sol.get("predicate", {}).get("value", "")
                    o = sol.get("object_type", {}).get("value", "")

                    if not self.in_database(type_, p, o, g):
                        self.add_triple_pattern(type_, type_, p, o, g)

                # incoming
                for sol in adapter.incoming_patterns(type_):

                    s = sol.get("subject_type", {}).get("value", "")
                    p = sol.get("predicate", {}).get("value", "")
                    g = sol.get("g", {}).get("value", "urn:default-graph")

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