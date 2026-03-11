# query_matching_class.rb
#
# Query → SHACL matching (partial responsiveness by triple-pattern overlap)
#
# This module:
#   1) Parses a SPARQL query and generates a mock RDF graph (fake_data.ttl)
#      that contains ONE triple per WHERE-pattern (including OPTIONAL parts),
#      replacing variables with fresh URIs and preserving IRIs.
#   2) Extracts triple-patterns from the fake RDF as (A, P, B) where:
#        - A is the rdf:type of the subject (or :ANY if not present in the query)
#        - P is the predicate IRI
#        - B is the rdf:type of the object (or :ANY if not present in the query)
#   3) Extracts triple-patterns from each SHACL file as:
#        - Exact (A, P, B) when a property has sh:path and sh:class
#        - Wildcard (A, P, :ANY) when a property has sh:path but NO sh:class
#   4) Marks an endpoint (SHACL file) as RESPONSIVE if there is ANY overlap:
#        - Query (A, P, B) matches SHACL exact (A, P, B)
#        - Query (A, P, B) matches SHACL wildcard (A, P, :ANY)
#        - Query (A, P, :ANY) matches SHACL exact (A, P, B)
#        - Query (:ANY, P, *) matches SHACL if ANY property path P exists
#
# NOTE: This is SOURCE SELECTION (partial responsiveness), not full SHACL validation.
#       We do NOT run SHACL conformance checks here, we only overlap structures.

require 'linkeddata'
require 'shacl'
require 'sparql'
require 'sparql/algebra'
require 'uri'
require 'set'

# -------------------------------------------------------------------
# 1) Fake RDF generator from a SPARQL query (structural mock graph)
# -------------------------------------------------------------------

# Generates mock RDF data to be validated against SHACL shapes.
#
# - Walks the SPARQL algebra tree to extract every triple pattern (including OPTIONAL parts).
# - Replaces variables with fresh fakedata URIs.
# - Preserves IRIs as-is; replaces literals with a neutral placeholder literal.
#
# @param query [String] the full SPARQL query string.
# @param output_document [String] path to write the generated Turtle/N-Triples.
# @return [void] writes the fake data file.
def fake_data_generator(query, output_document)
  algebra  = SPARQL.parse(query)                         # SPARQL::Algebra::Operator tree
  patterns = extract_triple_patterns_from_algebra(algebra)

  # De-duplicate identical patterns by their S/P/O string forms
  patterns = patterns.uniq { |pat| [pat.subject, pat.predicate, pat.object].map(&:to_s).join(' ') }

  # Map of variable symbol => fake URI
  variables = {}

  # Pre-assign fake URIs for each variable across the whole query
  patterns.each do |pat|
    pat.unbound_variables.each do |var_sym, _|
      variables[var_sym] ||= RDF::URI("http://fakedata.org/" + Array.new(12) { ('a'..'z').to_a.sample }.join)
    end
  end

  File.open(output_document, "w") do |file|
    patterns.each do |triple|
      s = triple.subject.variable?   ? variables[triple.subject.to_sym]   : triple.subject
      p = triple.predicate.variable? ? variables[triple.predicate.to_sym] : triple.predicate
      o =
        if triple.object.variable?
          variables[triple.object.to_sym]
        else
          triple.object.literal? ? RDF::Literal.new("FAKE_LITERAL") : triple.object
        end

      stmt = RDF::Statement.new(s, p, o)
      file.puts stmt.to_ntriples
      # Uncomment to debug each emitted triple:
      # puts "FAKE: #{stmt.to_ntriples.strip}"
    end
  end

  puts "fake_data_generator: wrote #{patterns.length} triple(s) to #{output_document}"
end

# ---- helpers ----

# Recursively walk the SPARQL algebra operator tree and collect all RDF::Query::Pattern
# from basic graph patterns (BGPs). Handles OPTIONAL (LeftJoin), ORDER, PREFIX, UNION, etc.
#
# @param node [Object] a SPARQL::Algebra::Operator, RDF::Query, RDF::Query::Pattern, or other
# @param acc  [Array<RDF::Query::Pattern>] accumulator
# @return [Array<RDF::Query::Pattern>]
def extract_triple_patterns_from_algebra(node, acc = [])
  return acc if node.nil?

  case node
  when RDF::Query
    node.patterns.each { |pat| acc << pat }
  when RDF::Query::Pattern
    acc << node
  else
    # Some nodes expose patterns directly
    if node.respond_to?(:patterns)
      node.patterns.each { |pat| acc << pat }
    end
    # Most SPARQL algebra operators expose operands (Prefix, Project, Order, LeftJoin, Union, BGP, etc.)
    if node.respond_to?(:operands)
      node.operands.each { |op| extract_triple_patterns_from_algebra(op, acc) }
    end
  end

  acc
end

# -------------------------------------------------------------------
# 2) Build triple-patterns (A,P,B) from the fake RDF graph
# -------------------------------------------------------------------

# From the fake graph, build all (A,P,B) where there exists s P o.
#
# IMPORTANT:
#   If the subject 's' has NO rdf:type triple in the fake data,
#     we set A = :ANY (subject-open).
#   If the object 'o' has NO rdf:type triple in the fake data,
#     we set B = :ANY (object-open).
#
# This reflects real queries that do not constrain types for one or both ends.
def extract_APB_patterns_from_fake(graph)
  rdf_type = RDF::URI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

  # Map node -> set of types
  types = Hash.new { |h,k| h[k] = Set.new }
  graph.query([nil, rdf_type, nil]).each { |st| types[st.subject] << st.object.to_s }

  patterns = Set.new

  graph.each_statement do |st|
    next if st.predicate == rdf_type

    subj_types = types[st.subject].to_a
    obj_types  = types[st.object].to_a

    # Subject wildcard when query doesn't constrain subject type
    subj_types = [:ANY] if subj_types.empty?
    # Object wildcard when query doesn't constrain object type
    obj_types  = [:ANY] if obj_types.empty?

    subj_types.each do |a|
      obj_types.each do |b|
        patterns << [a, st.predicate.to_s, b]
      end
    end
  end

  patterns.to_a
end

# -------------------------------------------------------------------
# 3) Build triple-patterns from SHACL shapes
# -------------------------------------------------------------------

# From SHACL shapes, build patterns:
#   - Exact (A,P,B): sh:NodeShape with sh:targetClass A and sh:property [ sh:path P ; sh:class B ].
#   - Wildcard (A,P,:ANY): sh:NodeShape with sh:targetClass A and sh:property [ sh:path P ] but NO sh:class.
#
# Notes:
#   - If multiple sh:path values exist (unusual), all are considered.
#   - If a property lacks sh:class entirely, we emit (A,P,:ANY) (open range).
def extract_APB_patterns_from_shacl(shapes_graph)
  ns_sh = "http://www.w3.org/ns/shacl#"
  sh_targetClass = RDF::URI("#{ns_sh}targetClass")
  sh_property    = RDF::URI("#{ns_sh}property")
  sh_path        = RDF::URI("#{ns_sh}path")
  sh_class       = RDF::URI("#{ns_sh}class")

  patterns = Set.new

  # For each NodeShape with targetClass A, iterate its sh:property blank nodes
  # and collect P and (optionally) B
  shapes_graph.query([nil, sh_targetClass, nil]).each do |tc|
    shape_node = tc.subject
    a = tc.object.to_s

    # properties linked from this shape
    shapes_graph.query([shape_node, sh_property, nil]).each do |prop_stmt|
      prop_bnode = prop_stmt.object

      # get sh:path and sh:class from the property node
      p_vals = shapes_graph.query([prop_bnode, sh_path, nil]).map { |s| s.object.to_s }
      b_vals = shapes_graph.query([prop_bnode, sh_class, nil]).map { |s| s.object.to_s }

      next if p_vals.empty? # malformed property without path

      if b_vals.empty?
        # Open range: (A,P,ANY)
        p_vals.each do |p|
          patterns << [a, p, :ANY]
        end
      else
        # Exact patterns: (A,P,B)
        p_vals.each do |p|
          b_vals.each do |b|
            patterns << [a, p, b]
          end
        end
      end
    end
  end

  patterns.to_a
end

# -------------------------------------------------------------------
# 4) Partial responsiveness validator by triple-pattern overlap
# -------------------------------------------------------------------

# Validates partial responsiveness of SHACL files to a query by matching triple-patterns.
#
# Rule:
#   An endpoint (SHACL file) is responsive if there exists at least one triple pattern (A, P, B)
#   such that:
#     - the fake data graph contributes (A,P,B_query) where A or B may be :ANY (wildcards), and
#     - the SHACL graph contains:
#         * exact (A,P,B_query), or
#         * wildcard (A,P,ANY), or
#         * (when A == :ANY) any property path P in any shape
#
# We do NOT require full SHACL validation; this is source-selection by pattern overlap.
#
# @param rdf_graph [String] path to fake RDF turtle (or N-Triples) file.
# @param shacl_dir [String] directory containing *.ttl files (one per endpoint).
# @return [Array<String>] list of endpoint identifiers (filenames sans .ttl) that are responsive.
def shacl_validator(rdf_graph, shacl_dir)
  fake_graph = RDF::Graph.load(rdf_graph)

  files = Dir.glob(File.join(shacl_dir, "*.ttl"))
  puts "shacl_validator: found #{files.length} SHACL file(s) in #{shacl_dir}"

  # --- Build (A,P,B_query) set from fake graph ---
  query_patterns = extract_APB_patterns_from_fake(fake_graph)
  puts "shacl_validator: query triple-patterns (A,P,B/ANY) = #{query_patterns.length}"
  if query_patterns.empty?
    warn "No (A,P,B/ANY) patterns could be extracted from fake data – ensure your query includes relevant patterns."
  end

  responsive = []

  files.each do |shacl_file|
    begin
      puts "\n=== Testing SHACL file: #{shacl_file} ==="
      shapes_graph = RDF::Graph.load(shacl_file)

      # Patterns from SHACL (exact and wildcard)
      shacl_patterns = extract_APB_patterns_from_shacl(shapes_graph)
      puts "  SHACL triple-patterns (A,P,B/ANY): #{shacl_patterns.length}"

      # Split into exact and wildcard (A,P,ANY)
      shacl_exact     = Set.new(shacl_patterns.select { |(_,_,b)| b != :ANY })
      shacl_wildcards = Set.new(shacl_patterns.select { |(_,_,b)| b == :ANY }.map { |a,p,_| [a,p] })

      # Collect ALL property paths present in SHACL (path-only index) for subject-ANY matches
      ns_sh = "http://www.w3.org/ns/shacl#"
      sh_path = RDF::URI("#{ns_sh}path")
      shacl_all_paths = Set.new(
        shapes_graph.query([nil, sh_path, nil]).map { |s| s.object.to_s }
      )

      # 1) Exact matches: (A,P,B) present in SHACL exact (requires A != :ANY, B != :ANY)
      exact_overlap = query_patterns.select { |a,p,b| a != :ANY && b != :ANY && shacl_exact.include?([a,p,b]) }

      # 2) SHACL wildcard: SHACL (A,P,ANY) matches query (A,P,B)  (A fixed, B any)
      wildcard_overlap_from_shacl = query_patterns.select { |a,p,_b| a != :ANY && shacl_wildcards.include?([a,p]) }

      # 3) Query wildcard on B: query (A,P,ANY) matches SHACL exact (A,P,B)
      wildcard_overlap_from_query_B = shacl_exact.select do |a,p,_b|
        query_patterns.include?([a,p,:ANY])
      end

      # 4) Subject ANY: query (:ANY,P,*) matches if SHACL has ANY property path P
      wildcard_overlap_from_query_A = query_patterns.select do |a,p,_b|
        a == :ANY && shacl_all_paths.include?(p)
      end

      puts "  Matched exact patterns:                #{exact_overlap.length}"
      puts "  Matched via SHACL wildcard (A,P,ANY):  #{wildcard_overlap_from_shacl.length}"
      puts "  Matched via query wildcard (A,P,ANY):  #{wildcard_overlap_from_query_B.length}"
      puts "  Matched via subject ANY (:ANY,P,*):    #{wildcard_overlap_from_query_A.length}"

      if exact_overlap.any? || wildcard_overlap_from_shacl.any? || wildcard_overlap_from_query_B.any? || wildcard_overlap_from_query_A.any?
        endpoint_id = File.basename(shacl_file, ".ttl")
        responsive << endpoint_id
      end

    rescue => ex
      warn "Error processing #{shacl_file}: #{ex}"
      next
    end
  end

  responsive
end