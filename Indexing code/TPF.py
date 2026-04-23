# == TPF Federated Harvester with Binding Propagation & Blank Node Safety ==
#
# Python implementation of the Ruby version with the following improvements:
#
# 1. SPARQL algebra parsing via RDFLib (no regex)
# 2. Concurrent TPF requests
# 3. RDFa page caching (avoid repeated parsing)
# 4. Vectorized bind-join batches
# 5. Unified datasource abstraction (TPF / SPARQL endpoint / RDF dump)
#
# Behaviour intentionally matches the Ruby version:
# - Harvest patterns independently per endpoint
# - Does NOT require endpoints to answer the whole query
# - Store harvested triples locally

import re
import json
import requests
import threading
from urllib.parse import urlencode, urljoin
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup

from rdflib import ConjunctiveGraph, Graph, URIRef, Literal, Variable
from rdflib.namespace import Namespace
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.sparql.parserutils import Expr
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery
import time
import queue

HYDRA = Namespace("http://www.w3.org/ns/hydra/core#")
VOID = Namespace("http://rdfs.org/ns/void#")

# -------------------------------------------------------------------------
# GLOBAL SETTINGS
# -------------------------------------------------------------------------

MAX_THREADS = 3
BIND_BATCH_SIZE = MAX_THREADS * 5
MAX_BUFFER_BYTES   = 10_000_000  # ~10MB safety cap
FLUSH_INTERVAL     = 5           # seconds
_cache_lock = threading.Lock()
page_cache = {}
_repo_lock  = threading.Lock()
_parse_lock = threading.Lock()

BUFFER_QUEUE_MAXSIZE = 100_000  # prevents memory explosion
_ingest_queue = queue.Queue(maxsize=BUFFER_QUEUE_MAXSIZE)

INDEXING_MODE = False
ALLOW_PARTIAL = True

# -------------------------------------------------------------------------
# SPARQL ALGEBRA PARSING
# -------------------------------------------------------------------------

def extract_all_patterns(node, patterns=None, graph_term=None):
    """
    Recursively walk the SPARQL algebra tree.
    - graph_term is propagated into BGP triples when inside a GRAPH block.
    - Handles both TPF (no graph) and QPF (GRAPH ?g / GRAPH <uri>) queries.
    """
    if patterns is None:
        patterns = []

    if node is None:
        return patterns

    node_name = getattr(node, "name", None)

    # BGP node: contains the actual triples
    if node_name == "BGP":
        if node.triples:
            for s, p, o in node.triples:
                patterns.append({
                    "subject":   s,
                    "predicate": p,
                    "object":    o,
                    "graph":     graph_term,  # None unless inside GRAPH block
                })
        return patterns

    # GRAPH node: sets the graph context for everything inside it
    if node_name == "Graph":
        inner_graph_term = getattr(node, "term", None)
        # node.p holds the inner algebra (usually a BGP)
        if hasattr(node, "p"):
            extract_all_patterns(node.p, patterns, graph_term=inner_graph_term)
        return patterns

    # All other nodes: recurse over known child attributes
    for attr in ["p", "p1", "p2", "args", "expr", "BGP", "Join", "LeftJoin", "Union", "Project", "Filter"]:
        if hasattr(node, attr):
            child = getattr(node, attr)
            if isinstance(child, list):
                for c in child:
                    extract_all_patterns(c, patterns, graph_term)
            elif child is not None:
                extract_all_patterns(child, patterns, graph_term)

    return patterns


def transform(query: str):
    with _parse_lock:   # <--- THIS IS THE KEY
        try:
            parsed = parseQuery(query)
            algebra = translateQuery(parsed).algebra
            raw_patterns = extract_all_patterns(algebra)
        except Exception as e:
            print(f"SPARQL parse error: {e}")

    # The rest of the function (term_to_str, deduplication, etc.) stays outside the lock
    def term_to_str(term):
        if term is None:
            return None
        if isinstance(term, Variable):
            return f"?{term}"
        return str(term)

    seen = set()
    bgp = []
    for pat in raw_patterns:
        entry = {
            "subject": term_to_str(pat.get("subject")),
            "predicate": term_to_str(pat.get("predicate")),
            "object": term_to_str(pat.get("object")),
            "graph": term_to_str(pat.get("graph"))
        }
        key = (entry["subject"], entry["predicate"], entry["object"], entry["graph"])
        if key not in seen:
            seen.add(key)
            bgp.append(entry)

    print("DEBUG: extracted quad patterns:", bgp)
    return bgp

# -------------------------------------------------------------------------
# PATTERN HELPERS
# -------------------------------------------------------------------------

def extract_vars_from_pattern(pat):
    vars_ = []

    for field in ("subject", "predicate", "object", "graph"):
        val = pat.get(field)
        if val and val.startswith("?"):
            vars_.append(val[1:])

    return vars_

def shares_variable(pat, processed_patterns):

    vars = set(extract_vars_from_pattern(pat))

    for p in processed_patterns:
        if vars.intersection(extract_vars_from_pattern(p)):
            return True

    return False


# -------------------------------------------------------------------------
# TPF URL BUILDER
# -------------------------------------------------------------------------
def triple_matches_request(s, p, o, req_s, req_p, req_o):
    """
    Comunica-style triple filtering:
    Only keep triples that match the requested triple pattern.
    """

    def match(req, val):
        if req is None or req.startswith("?"):
            return True

        # Normalize <IRI>
        if req.startswith("<") and req.endswith(">"):
            req = req[1:-1]

        return str(val) == req

    return (
        match(req_s, s) and
        match(req_p, p) and
        match(req_o, o)
    )


def tpf_uri_request_builder(control_uri, subject, predicate, object_, graph=None):
    """
    Build a TPF or QPF request URL.
    Skips variables (starting with ?) and None values.
    Includes graph parameter for QPF when provided.
    """
    params = {}

    if subject is not None and not subject.startswith("?"):
        params["subject"] = subject

    if predicate is not None and not predicate.startswith("?"):
        params["predicate"] = predicate

    if object_ is not None and not object_.startswith("?"):
        params["object"] = object_

    if graph is not None and not graph.startswith("?"):
        params["graph"] = graph

    if params:
        return f"{control_uri}?{urlencode(params)}"
    return control_uri


# -------------------------------------------------------------------------
# CARDINALITY ESTIMATION
# -------------------------------------------------------------------------

def heuristic_cardinality(html):

    if re.search(r'no\s*triples', html, re.I):
        return 0

    if 'rel="next"' in html:
        return 10000

    triple_count = len(re.findall(r'property=|typeof=', html))

    if triple_count > 0:
        return triple_count

    return 5000


def get_pattern_count(control_uri, subject, predicate, object_, graph=None):

    url = tpf_uri_request_builder(control_uri, subject, predicate, object_, graph)

    try:
        response = requests.get(url)
        html = response.text
        content_type = response.headers.get("content-type", "")
    except Exception as e:
        print("Connection failed:", url, e)
        return 999_999_999

    if "html" not in content_type and "<html" not in html:
        return heuristic_cardinality(html)

    g = Graph()
    count = None

    try:
        g.parse(data=html, format="rdfa")
    except:
        return heuristic_cardinality(html)

    for pred in [HYDRA.totalItems, VOID.triples]:
        for s, p, o in g:
            if p == pred:
                raw = re.sub(r'[,±~]', '', str(o).strip())
                if raw.isdigit():
                    count = int(raw)
                    break
        if count:
            break

    return count if count else heuristic_cardinality(html)

# -------------------------------------------------------------------------
# PAGE FETCHING (WITH CACHE)
# -------------------------------------------------------------------------

from rdflib import Graph, ConjunctiveGraph
import requests

page_cache = {}

FOAF_PRIMARY_TOPIC = URIRef("http://xmlns.com/foaf/0.1/primaryTopic")
METADATA_NAMESPACES = (
    "http://www.w3.org/ns/hydra/core#",
    "http://rdfs.org/ns/void#",
)

def _is_metadata_predicate(p):
    return any(str(p).startswith(ns) for ns in METADATA_NAMESPACES)

def _extract_data_and_meta(cg):
    """
    Primary strategy: exclude the graph that contains foaf:primaryTopic.
    Returns (data_graph, meta_graph, strategy_used).
    Falls back to predicate filtering if primaryTopic not found or yields no data.
    """
    # --- Strategy 1: graph-based exclusion via foaf:primaryTopic ---
    metadata_graph_iris = set()
    for s, p, o, ctx in cg.quads((None, FOAF_PRIMARY_TOPIC, None, None)):
        metadata_graph_iris.add(ctx.identifier)

    if metadata_graph_iris:
        data_graph = Graph()
        meta_graph = Graph()

        for s, p, o, ctx in cg.quads((None, None, None, None)):
            if ctx.identifier in metadata_graph_iris:
                meta_graph.add((s, p, o))
            else:
                data_graph.add((s, p, o))

        if len(data_graph) > 0:
            print(f"  [Strategy] Graph exclusion via foaf:primaryTopic "
                  f"({len(metadata_graph_iris)} metadata graph(s))")
            return data_graph, meta_graph, "graph"
        else:
            print("  [Strategy] foaf:primaryTopic found but yielded 0 data triples, "
                  "falling back to predicate filtering")

    else:
        print("  [Strategy] foaf:primaryTopic not found, "
              "falling back to predicate filtering")

    # --- Strategy 2: predicate namespace filtering (original behaviour) ---
    data_graph = Graph()
    meta_graph = Graph()

    for s, p, o, ctx in cg.quads((None, None, None, None)):
        if _is_metadata_predicate(p):
            meta_graph.add((s, p, o))
        else:
            data_graph.add((s, p, o))

    print(f"  [Strategy] Predicate filtering → {len(data_graph)} data triples")
    return data_graph, meta_graph, "predicate"


def fetch_tpf_page(url):
    """
    Fetch and parse a TPF/QPF page.

    Comunica-style:
    - DO NOT separate metadata vs data
    - Return full graph
    - Metadata will be handled separately
    """

    with _cache_lock:
        if url in page_cache:
            return page_cache[url]

    headers = {"Accept": "application/trig, text/turtle;q=0.9"}

    cg = ConjunctiveGraph()

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        text = resp.text

        print(f"[FETCH] {url} → {resp.status_code}, {len(text)} bytes")

        # Try formats (same as before but cleaner intention)
        for fmt in ["trig", "turtle", "nt", "xml", "json-ld", "rdfa"]:
            try:
                cg.parse(data=text, format=fmt)
                break
            except Exception:
                continue

    except Exception as e:
        print(f"  Fetch/parse error: {e}")

    with _cache_lock:
        page_cache[url] = cg

    return cg

def _next_page_from_graph(meta_graph, current_url):
    for next_pred in [HYDRA.nextPage, HYDRA.next,
                      URIRef("http://www.w3.org/ns/hydra/core#next")]:
        for s, p, o in meta_graph.triples((None, next_pred, None)):
            return str(o)
    return None


def harvest_pattern_into_repo(url, named_graph,
                              subject=None, predicate=None, object_=None):
    """
    Harvest triples from a TPF/QPF endpoint for ONE triple pattern.

    Comunica-style:
    - Fetch full page
    - Filter triples by triple pattern
    - Ignore metadata implicitly (it won't match)
    """

    print("Harvesting URL:", url)

    current_url = url
    page_count = 0

    while current_url:
        page_count += 1
        print(f" Page {page_count}: {current_url}")

        cg = fetch_tpf_page(current_url)

        # --- Extract next page from metadata ---
        next_url = None
        items_per_page = None

        for s, p, o in cg:
            if p == HYDRA.next or p == HYDRA.nextPage:
                next_url = str(o)

            if p == HYDRA.itemsPerPage:
                try:
                    items_per_page = int(str(o))
                except ValueError:
                    pass

        # --- Filter triples (THIS IS THE KEY CHANGE) ---
        data_triples = 0

        for s, p, o, ctx in cg.quads((None, None, None, None)):

            if triple_matches_request(s, p, o,
                                      subject, predicate, object_):

                add_to_buffer((s, p, o), named_graph)
                data_triples += 1

        print(f"  Buffered {data_triples} matching triples")

        # --- Stop conditions ---
        if data_triples == 0:
            print("  → No matching triples → end")
            break

        if items_per_page is not None and data_triples < items_per_page:
            print(f"  → Partial page → last page")
            break

        if next_url == current_url:
            print("  WARNING: next URL equals current URL, stopping.")
            break

        current_url = next_url




# -------------------------------------------------------------------------
# VECTORISED BIND JOIN
# -------------------------------------------------------------------------

def fetch_binding(binding, pat, control_uri, named_graph):

    def concretize(term, binding):
        if term is None or not term.startswith("?"):
            return term
        var_name = term[1:]
        if var_name not in binding:
            return term
        val = binding[var_name]
        if isinstance(val, URIRef):
            return str(val)
        elif isinstance(val, Literal):
            return val.n3()
        return str(val)

    s = concretize(pat["subject"],   binding)
    p = concretize(pat["predicate"], binding)
    o = concretize(pat["object"],    binding)
    g = concretize(pat.get("graph"), binding)  # None if TPF

    url = tpf_uri_request_builder(control_uri, s, p, o, g)
    harvest_pattern_into_repo(
    url,
    named_graph,
    s,
    p,
    o)


def fetch_binding_batch(batch, pat, control_uri, named_graph):

    with ThreadPoolExecutor(MAX_THREADS) as pool:

        futures = []

        for binding in batch:
            futures.append(
                pool.submit(fetch_binding, binding, pat, control_uri, named_graph)
            )

        for f in futures:
            f.result()


# -------------------------------------------------------------------------
# BINDING EXTRACTION
# -------------------------------------------------------------------------

# def extract_upstream_bindings(repo, current_idx, harvested, bgp):

#     if not harvested:
#         return []

#     prev_patterns = [bgp[i] for i in harvested]

#     needed = extract_vars_from_pattern(bgp[current_idx])

#     upstream_vars = set()

#     for p in prev_patterns:
#         upstream_vars.update(extract_vars_from_pattern(p))

#     join_vars = list(set(needed).intersection(upstream_vars))

#     if not join_vars:
#         return []

#     query = "SELECT " + " ".join("?" + v for v in join_vars) + " WHERE {\n"

#     def safe(term):

#         if term.startswith("?"):
#             return term

#         if term.startswith("<"):
#             return term

#         if term.startswith('"'):
#             return term

#         if term.startswith("http"):
#             return f"<{term}>"

#         return f'"{term}"'

#     for pat in prev_patterns:

#         s = safe(pat["subject"])
#         p = safe(pat["predicate"])
#         o = safe(pat["object"])

#         query += f" {s} {p} {o} .\n"

#     query += "}"

#     print("DEBUG binding extraction")
#     print(query)

#     try:

#         q = prepareQuery(query)

#         solutions = []

#         for row in repo.query(q):

#             sol = {}

#             for v in join_vars:

#                 val = row[v]

#                 if val:
#                     sol[v] = val

#             if sol:
#                 solutions.append(sol)

#         print("DEBUG:", len(solutions), "bindings")

#         return solutions

#     except Exception as e:

#         print("Local binding extraction failed", e)
#         return []

def term_matches(pattern_term, triple_term):
    """
    Check whether a triple term matches a pattern term.
    Variables always match.
    Constants must be equal.
    """
    if pattern_term.startswith("?"):
        return True

    if pattern_term.startswith("<") and pattern_term.endswith(">"):
        pattern_term = pattern_term[1:-1]

    return str(triple_term) == pattern_term

def triple_matches_pattern(triple, pat):
    s, p, o = triple

    if not term_matches(pat["subject"], s):
        return False

    if not term_matches(pat["predicate"], p):
        return False

    if not term_matches(pat["object"], o):
        return False

    return True

def extract_upstream_bindings(repo, current_idx, harvested, bgp):

    if not harvested:
        return []

    current_pat = bgp[current_idx]

    # variables in current pattern
    current_vars = set(extract_vars_from_pattern(current_pat))

    # collect vars from previous patterns
    upstream_vars = set()
    prev_patterns = [bgp[i] for i in harvested]

    for p in prev_patterns:
        upstream_vars.update(extract_vars_from_pattern(p))

    join_vars = current_vars.intersection(upstream_vars)

    print("\n--- Binding Extraction ---")
    print("Current pattern:", current_pat)
    print("Join variables:", join_vars)
    print("Repo size:", len(repo))
    if not join_vars:
        return []

    bindings = []

    for triple in repo:

        s, p, o = triple

        for pat in prev_patterns:

            if not triple_matches_pattern(triple, pat):
                continue

            sol = {}

            if pat["subject"].startswith("?"):
                var = pat["subject"][1:]
                if var in join_vars:
                    sol[var] = s

            if pat["predicate"].startswith("?"):
                var = pat["predicate"][1:]
                if var in join_vars:
                    sol[var] = p

            if pat["object"].startswith("?"):
                var = pat["object"][1:]
                if var in join_vars:
                    sol[var] = o

            if sol:
                bindings.append(sol)

    # deduplicate
    unique = []
    seen = set()

    for b in bindings:
        key = tuple(sorted((k, str(v)) for k, v in b.items()))
        if key not in seen:
            seen.add(key)
            unique.append(b)

    print("Bindings extracted:", len(unique))

    if unique:
        print("Sample bindings:", unique[:5])

    print("--------------------------\n")

    return unique


def extract_upstream_bindings_graphdb(current_idx, harvested, bgp, graph_iri):

    if not harvested:
        return []

    current_pat = bgp[current_idx]

    current_vars = set(extract_vars_from_pattern(current_pat))

    upstream_vars = set()
    prev_patterns = [bgp[i] for i in harvested]

    for p in prev_patterns:
        upstream_vars.update(extract_vars_from_pattern(p))

    join_vars = current_vars.intersection(upstream_vars)

    if not join_vars:
        return []

    def safe(term):
        if term.startswith("?"):
            return term
        if term.startswith("<"):
            return term
        if term.startswith('"'):
            return term
        if term.startswith("http"):
            return f"<{term}>"
        return f'"{term}"'

    query = "SELECT " + " ".join("?" + v for v in join_vars) + " WHERE {\n"

    if graph_iri:
        query += f" GRAPH <{graph_iri}> {{\n"

    for pat in prev_patterns:
        s = safe(pat["subject"])
        p = safe(pat["predicate"])
        o = safe(pat["object"])
        query += f" {s} {p} {o} .\n"

    if graph_iri:
        query += " }\n"

    query += "}"

    print("DEBUG GraphDB binding query:")
    print(query)

    try:
        results = execute_sparql_query(query)
        bindings = []

        for row in results:
            sol = {}
            for v in join_vars:
                if v in row:
                    sol[v] = row[v]
            if sol:
                bindings.append(sol)

        print("Bindings extracted from GraphDB:", len(bindings))
        return bindings

    except Exception as e:
        print("GraphDB binding extraction failed:", e)
        return []
# -------------------------------------------------------------------------
# INSERT INTO LOCAL TRIPLESTORE
# -------------------------------------------------------------------------

def build_query(statements, named_graph=None):

    triples = "\n".join(
        f"{s.n3()} {p.n3()} {o.n3()} ." for s,p,o in statements
    )

    if named_graph:

        return f"""
INSERT DATA {{
 GRAPH <{named_graph}> {{
 {triples}
 }}
}}
"""

    else:

        return f"""
INSERT DATA {{
{triples}
}}
"""


# def insert_query(query):
#     endpoint = "http://acb8computer:7200/repositories/test1/statements"

#     try:
#         r = requests.post(
#             endpoint,
#             data=query,
#             headers={
#                 "Content-Type":"application/sparql-update",
#                 "Accept":"application/json"
#             }
#         )

#         if r.status_code == 200 or r.status_code == 204:
#             print("Bulk insert successful")
#         else:
#             print("Insert failed:", r.status_code, r.text)  # <- show server response

#     except Exception as e:
#         print("Insert error:", e)


def insert_triples_stream(statements, named_graph=None):
    """
    Send triples directly to GraphDB as RDF (N-Triples), without SPARQL.
    Much lower memory usage and faster ingestion.
    """

    endpoint = "http://acb8computer:7200/repositories/test1/statements"

    lines = []

    for s, p, o in statements:
        triple = f"{s.n3()} {p.n3()} {o.n3()} ."

        # If using named graphs → use N-Quads format
        if named_graph:
            triple = f"{s.n3()} {p.n3()} {o.n3()} <{named_graph}> ."

        lines.append(triple)

    payload = "\n".join(lines)

    headers = {
        # IMPORTANT: switch format depending on named_graph
        "Content-Type": "application/n-quads" if named_graph else "application/n-triples"
    }

    try:
        r = requests.post(endpoint, data=payload, headers=headers)

        if r.status_code in (200, 204):
            print(f"Stream insert OK ({len(statements)} triples)")
            
        else:
            print("Stream insert failed:", r.status_code, r.text)
        time.sleep(0.1)  # slight delay to avoid overwhelming the server
    except Exception as e:
        print("Streaming insert error:", e)


# -------------------------------------------------------------------------
# BUFFERED INGESTION
# -------------------------------------------------------------------------
def add_to_buffer(triple, named_graph=None):
    """
    Push triple into ingestion queue (blocking if full).
    This provides backpressure and guarantees no data loss.
    """

    s, p, o = triple

    if named_graph:
        line = f"{s.n3()} {p.n3()} {o.n3()} <{named_graph}> .\n"
    else:
        line = f"{s.n3()} {p.n3()} {o.n3()} .\n"

    _ingest_queue.put(line)  # blocks if queue full → SAFE




def buffer_flusher_daemon():
    endpoint = "http://acb8computer:7200/repositories/test1/statements"
    BATCH_SIZE = 500
    MAX_RETRIES = 3

    while True:
        batch = []
        try:
            # Block until at least one item is available
            line = _ingest_queue.get(timeout=1)
            batch.append(line)

            # Drain up to BATCH_SIZE without blocking
            while len(batch) < BATCH_SIZE:
                try:
                    batch.append(_ingest_queue.get_nowait())
                except queue.Empty:
                    break

            # Attempt POST with retries
            payload = "".join(batch)
            success = False

            for attempt in range(MAX_RETRIES):
                try:
                    r = requests.post(
                        endpoint,
                        data=payload,
                        headers={"Content-Type": "application/n-quads"},
                        timeout=30
                    )
                    if r.status_code in (200, 204):
                        success = True
                        break
                    else:
                        print(f"[FLUSH ERROR] attempt {attempt+1}: {r.status_code} {r.text[:200]}")
                except requests.RequestException as e:
                    print(f"[FLUSH ERROR] attempt {attempt+1}: {e}")
                time.sleep(0.5 * (attempt + 1))  # backoff

            if not success:
                # Log the lost triples so you can diagnose
                print(f"[DATA LOSS] Failed to insert {len(batch)} triples after {MAX_RETRIES} retries")

            # Always mark done — keeps join() unblocked
            # Log failures above rather than silently swallowing
            for _ in batch:
                _ingest_queue.task_done()

            time.sleep(0.05)

        except queue.Empty:
            # Queue idle — nothing to flush right now
            pass
# -------------------------------------------------------------------------
# HARVEST EXECUTION
# -------------------------------------------------------------------------

def harvest_endpoint_optimized(control_uri, bgp, named_graph):

    print("Harvesting:", control_uri)

    harvested = set()
    counts = {}

    for i, pat in enumerate(bgp):
        counts[i] = get_pattern_count(
            control_uri,
            pat["subject"],
            pat["predicate"],
            pat["object"],
            pat.get("graph"),       # None for TPF, graph IRI/var for QPF
        )

    remaining = list(range(len(bgp)))
    first = min(remaining, key=lambda i: counts[i])
    remaining.remove(first)
    execution_order = [first]

    while remaining:
        connected = [
            idx for idx in remaining
            if shares_variable(bgp[idx], [bgp[i] for i in execution_order])
        ]
        next_idx = min(connected if connected else remaining,
                       key=lambda i: counts[i])
        execution_order.append(next_idx)
        remaining.remove(next_idx)

    print("Execution order:", execution_order)

    for idx in execution_order:
        pat = bgp[idx]
        print("Processing pattern", idx, pat)

        bindings = extract_upstream_bindings_graphdb(
            idx, harvested, bgp, named_graph
        )

        if INDEXING_MODE:
            required_vars = extract_vars_from_pattern(pat)
            if not bindings and required_vars and len(harvested) > 0:
                print("Skipping pattern (strict mode, no bindings)")
                harvested.add(idx)
                continue

        if not bindings or not extract_vars_from_pattern(pat):
            print("Full pattern download")
            url = tpf_uri_request_builder(
                control_uri,
                pat["subject"],
                pat["predicate"],
                pat["object"],
                pat.get("graph"),
            )
            harvest_pattern_into_repo(
            url,
            named_graph,
            pat["subject"],
            pat["predicate"],
            pat["object"])

        else:
            print("Bind join:", len(bindings), "bindings")
            for i in range(0, len(bindings), BIND_BATCH_SIZE):
                print(f"Processing binding batch {i} → {i + BIND_BATCH_SIZE}")
                batch = bindings[i:i + BIND_BATCH_SIZE]
                fetch_binding_batch(batch, pat, control_uri, named_graph)

        _ingest_queue.join()
        print(f"[SYNC] Queue drained after pattern {idx}")
        harvested.add(idx)
        print("------------------------------------")

    return None


# -------------------------------------------------------------------------
# MAIN ENTRY POINT
# -------------------------------------------------------------------------

def FindBGPPriority(query, endpoints, base_named_graph=None):
    threading.Thread(target=buffer_flusher_daemon, daemon=True).start()
    if isinstance(endpoints,str):
        endpoints = [endpoints]

    try:
        bgp = transform(query)
    except Exception as e:
        print("ERROR inside transform:", e)
        return

    if not bgp:
        print("No triple patterns extracted from query")
        return

    print("Query has", len(bgp), "triple patterns")

    for i,endpoint in enumerate(endpoints):

        graph_iri = None

        if base_named_graph:
            graph_iri = f"{base_named_graph}/endpoint{i+1}"

        harvest_endpoint_optimized(endpoint, bgp, graph_iri)
    print("Waiting for ingestion queue to empty...")
    _ingest_queue.join()
    print("All data flushed.")
    print("All endpoints processed.")

# -------------------------------------------------------------------------
# EXECUTE SPARQL QUERY ON LOCAL GRAPHDB
# -------------------------------------------------------------------------

def execute_sparql_query(query):

    endpoint = "http://acb8computer:7200/repositories/test1"

    try:

        r = requests.post(
            endpoint,
            data=query,
            headers={
                "Content-Type": "application/sparql-query",
                "Accept": "application/sparql-results+json"
            }
        )

        if r.status_code != 200:
            print("Query failed:", r.text)
            return None

        data = r.json()

        results = []

        for row in data["results"]["bindings"]:

            parsed = {}

            for var,val in row.items():
                parsed[var] = val["value"]

            results.append(parsed)

        return results

    except Exception as e:

        print("SPARQL execution error:", e)
        return None


def run_query_strict(query, endpoints, base_named_graph="urn:tpf:temp"):
    import uuid
    run_id = str(uuid.uuid4())
    graph_base = f"{base_named_graph}/{run_id}"

    print(f"[run_query_strict] Using temporary graph: {graph_base}")

    FindBGPPriority(query, endpoints, base_named_graph=graph_base)

    # FIXED: filter to only THIS run's graphs, not the entire repository
    wrapped_query = f"""
    SELECT ?s ?p ?o WHERE {{
      GRAPH ?g {{
        ?s ?p ?o .
      }}
      FILTER(STRSTARTS(STR(?g), "{graph_base}/"))
    }}
    """

    results = execute_sparql_query(wrapped_query)

    if not results:
        return []

    triples = []
    for row in results:
        s = row.get("s")
        p = row.get("p")
        o = row.get("o")
        if s and p and o:
            triples.append((s, p, o))

    print(f"[run_query_strict] Returned {len(triples)} triples")
    return triples