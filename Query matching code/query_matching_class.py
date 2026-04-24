"""
query_matching_class.py

Query → SHACL matching (partial responsiveness by triple-pattern overlap)

Pre-flight: query MUST contain at least one rdf:type triple pattern.
Matching rules (any one hit = endpoint is responsive):
  1. Exact:            query (A, P, B)   in SHACL exact (A, P, B)
  2. SHACL wildcard:   query (A, P, *)   where SHACL has (A, P, ANY)
  3. Query B wildcard: query (A, P, ANY) where SHACL has exact (A, P, B)
  4. Subject ANY:      query (ANY, P, *) where P exists anywhere in SHACL
"""

import os
import glob
import logging
from typing import List, Tuple, Set, Dict

from rdflib import Graph, URIRef, Literal, BNode, Namespace
from rdflib.term import Variable
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery

logger = logging.getLogger(__name__)

RDF_TYPE_STR = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
ANY = "ANY"  # Wildcard sentinel (mirrors Ruby :ANY)

SH = Namespace("http://www.w3.org/ns/shacl#")

# (subject_type_or_ANY, predicate_str, object_type_or_ANY)
APBPattern = Tuple[str, str, str]

# ---------------------------------------------------------------------------
# 1) Algebra walker — collect every triple pattern in the algebra tree
# ---------------------------------------------------------------------------

def collect_triple_patterns(node, acc=None, visited=None):
    """
    Recursively descend a rdflib SPARQL algebra tree and collect all
    (subject, predicate, object) triples from BGP nodes.

    Handles:
      - BGP nodes         → extract .triples directly
      - list / tuple      → iterate elements
      - other CompValues  → recurse into .values()
      - RDF terms / scalars → ignored (leaves)
    A visited set (by id) prevents infinite loops on cycles.
    """
    if acc is None:
        acc = []
    if visited is None:
        visited = set()

    if node is None:
        return acc

    # Leaves — nothing to descend into
    if isinstance(node, (str, int, float, bool, bytes,
                          URIRef, Literal, BNode, Variable)):
        return acc

    # Lists / tuples — recurse into each element
    if isinstance(node, (list, tuple)):
        for item in node:
            collect_triple_patterns(item, acc, visited)
        return acc

    # Cycle guard
    nid = id(node)
    if nid in visited:
        return acc
    visited.add(nid)

    # BGP: the triple patterns live in node['triples']
    if getattr(node, 'name', None) == 'BGP':
        for triple in (node.get('triples') or []):
            if isinstance(triple, (list, tuple)) and len(triple) == 3:
                acc.append(tuple(triple))
        return acc

    # All other algebra nodes (Join, LeftJoin, Filter, Project, etc.)
    # — recurse into every value they hold
    if hasattr(node, 'values'):
        for value in node.values():
            collect_triple_patterns(value, acc, visited)

    return acc


# ---------------------------------------------------------------------------
# 2) Build (A, P, B) patterns from a SPARQL query string
# ---------------------------------------------------------------------------

def _is_var(term) -> bool:
    return isinstance(term, Variable)

def _is_uri(term) -> bool:
    return isinstance(term, URIRef)


def extract_APB_from_query(query_string: str, debug: bool = True) -> List[APBPattern]:
    """
    Parse query → collect triple patterns → derive (A, P, B).

    A/B are the concrete rdf:type IRI of the subject/object variable
    (from ?var rdf:type <IRI> patterns in the same query), or ANY when
    no type constraint is present.
    Patterns with a variable predicate are skipped.
    rdf:type triples are skipped (they supply type info, not data).
    """
    parsed   = parseQuery(query_string)
    query_obj = translateQuery(parsed)
    algebra  = query_obj.algebra

    raw = collect_triple_patterns(algebra)

    # Deduplicate by string representation
    seen: Set[tuple] = set()
    unique_raw = []
    for triple in raw:
        key = (str(triple[0]), str(triple[1]), str(triple[2]))
        if key not in seen:
            seen.add(key)
            unique_raw.append(triple)
    raw = unique_raw

    if debug:
        print(f"  [DEBUG] raw patterns collected from algebra: {len(raw)}")
        for s, p, o in raw:
            print(f"    S={s} P={p} O={o}")

    # variable name (str) → set of concrete type IRIs
    var_types: Dict[str, Set[str]] = {}
    for s, p, o in raw:
        if _is_var(p):
            continue
        if str(p) != RDF_TYPE_STR:
            continue
        if _is_var(s) and _is_uri(o):
            var_types.setdefault(str(s), set()).add(str(o))

    if debug:
        print(f"  [DEBUG] var_types: "
              f"{[f'{k} => {list(v)}' for k, v in var_types.items()]}")

    result: Set[APBPattern] = set()
    for s, p, o in raw:
        if _is_var(p):
            continue
        if str(p) == RDF_TYPE_STR:
            continue
        pred = str(p)

        types_a = list(var_types.get(str(s), set())) if _is_var(s) else []
        if not types_a:
            types_a = [ANY]

        types_b = list(var_types.get(str(o), set())) if _is_var(o) else []
        if not types_b:
            types_b = [ANY]

        for a in types_a:
            for b in types_b:
                result.add((a, pred, b))

    result_list = list(result)
    if debug:
        print(f"  [DEBUG] APB patterns from query: {len(result_list)}")
        for apb in result_list:
            print(f"    {apb}")

    return result_list


def query_has_type_pattern(query_string: str, debug: bool = True) -> bool:
    """
    Returns True if the query contains at least one rdf:type triple pattern
    (predicate == rdf:type; subject/object may be anything).
    """
    parsed    = parseQuery(query_string)
    query_obj = translateQuery(parsed)
    algebra   = query_obj.algebra
    patterns  = collect_triple_patterns(algebra)
    found = any(
        not _is_var(p) and str(p) == RDF_TYPE_STR
        for _, p, _ in patterns
    )
    if debug:
        print(f"  [DEBUG] query_has_type_pattern? → {found}")
    return found


# ---------------------------------------------------------------------------
# 3) Build (A, P, B) patterns from a SHACL shapes graph
# ---------------------------------------------------------------------------

def extract_APB_from_shacl(shapes_graph: Graph) -> List[APBPattern]:
    """
    Extract (A, P, B) patterns from a loaded SHACL shapes graph:
      - Exact    (A, P, B)   from sh:targetClass A + sh:path P + sh:class B
      - Wildcard (A, P, ANY) from sh:targetClass A + sh:path P (no sh:class)
    """
    result: Set[APBPattern] = set()

    for shape_node, _, target_class in shapes_graph.triples(
            (None, SH.targetClass, None)):
        a = str(target_class)

        for _, _, prop_node in shapes_graph.triples(
                (shape_node, SH.property, None)):
            p_vals = [str(o) for _, _, o in
                      shapes_graph.triples((prop_node, SH.path, None))]
            b_vals = [str(o) for _, _, o in
                      shapes_graph.triples((prop_node, SH['class'], None))]

            if not p_vals:
                continue

            if not b_vals:
                for p in p_vals:
                    result.add((a, p, ANY))
            else:
                for p in p_vals:
                    for b in b_vals:
                        result.add((a, p, b))

    return list(result)


# ---------------------------------------------------------------------------
# 4) Main validator
# ---------------------------------------------------------------------------

def shacl_validator(query_string: str, shacl_dir: str,
                    debug: bool = True) -> List[str]:
    """
    Determine which SHACL endpoint files are responsive to the given query.

    Pre-flight: query must contain at least one rdf:type triple pattern.

    Matching rules (any hit = endpoint marked responsive):
      1. Exact:            query (A, P, B)   in SHACL exact (A, P, B)
      2. SHACL wildcard:   query (A, P, *)   where SHACL has (A, P, ANY)
      3. Query B wildcard: query (A, P, ANY) where SHACL has exact (A, P, B)
      4. Subject ANY:      query (ANY, P, *) where P exists in any SHACL path

    Returns a list of endpoint identifiers (TTL filenames without extension).
    """
    if debug:
        print("\n[shacl_validator] Pre-flight check...")
    if not query_has_type_pattern(query_string, debug=debug):
        if debug:
            print("  → Query has no rdf:type pattern — skipping.")
        return []

    if debug:
        print("[shacl_validator] Extracting APB from query...")
    query_patterns = extract_APB_from_query(query_string, debug=debug)

    if not query_patterns:
        if debug:
            print("  → No matchable (A, P, B) patterns in query — skipping.")
        return []

    query_pattern_set = set(query_patterns)

    shacl_files = glob.glob(os.path.join(shacl_dir, "*.ttl"))
    if debug:
        print(f"[shacl_validator] Found {len(shacl_files)} SHACL file(s)")

    responsive: List[str] = []

    for shacl_file in shacl_files:
        try:
            shapes_graph = Graph()
            shapes_graph.parse(shacl_file, format="turtle")

            shacl_patterns = extract_APB_from_shacl(shapes_graph)

            shacl_exact     = {pat for pat in shacl_patterns if pat[2] != ANY}
            shacl_wildcards = {(a, p) for a, p, b in shacl_patterns if b == ANY}
            shacl_all_paths = {
                str(o)
                for _, _, o in shapes_graph.triples((None, SH.path, None))
            }

            # Rule 1: exact match (A, P, B) — both ends concrete
            exact_overlap = [
                (a, p, b) for a, p, b in query_patterns
                if a != ANY and b != ANY and (a, p, b) in shacl_exact
            ]
            # Rule 2: SHACL has (A, P, ANY) — matches any query (A, P, *)
            wildcard_shacl = [
                (a, p, b) for a, p, b in query_patterns
                if a != ANY and (a, p) in shacl_wildcards
            ]
            # Rule 3: query has (A, P, ANY) — matches SHACL exact (A, P, B)
            wildcard_query_b = [
                (a, p, b) for a, p, b in shacl_exact
                if (a, p, ANY) in query_pattern_set
            ]
            # Rule 4: query has (ANY, P, *) — matches if P exists in SHACL
            wildcard_any_subj = [
                (a, p, b) for a, p, b in query_patterns
                if a == ANY and p in shacl_all_paths
            ]

            hit = bool(exact_overlap or wildcard_shacl
                       or wildcard_query_b or wildcard_any_subj)

            if debug:
                fname = os.path.basename(shacl_file)
                print(f"\n=== {fname} ===")
                print(f"  SHACL patterns: {len(shacl_patterns)} "
                      f"| paths: {len(shacl_all_paths)}")
                print(f"  Rule1 exact: {len(exact_overlap)}  "
                      f"Rule2 shacl-wc: {len(wildcard_shacl)}  "
                      f"Rule3 query-wc: {len(wildcard_query_b)}  "
                      f"Rule4 any-subj: {len(wildcard_any_subj)}")
                print(f"  → {'RESPONSIVE' if hit else 'not responsive'}")

            if hit:
                responsive.append(os.path.splitext(
                    os.path.basename(shacl_file))[0])

        except Exception as e:
            logger.warning(f"Error processing {shacl_file}: {e}")
            continue

    return responsive