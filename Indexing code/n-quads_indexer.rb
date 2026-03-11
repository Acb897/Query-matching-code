require "sparql/client"
require "digest"
require "set"
require "fileutils"
require "uri"
STDOUT.sync = true

# ==============================
# SPO Pattern Container
# ==============================
class SPO
  attr_accessor :SPO_Subject
  attr_accessor :SPO_Predicate
  attr_accessor :SPO_Object
  attr_accessor :SPO_Graph

  def initialize(params = {})
    @SPO_Subject   = params.fetch(:SPO_Subject,   "")
    @SPO_Predicate = params.fetch(:SPO_Predicate, "")
    @SPO_Object    = params.fetch(:SPO_Object,    "")
    @SPO_Graph     = params.fetch(:SPO_Graph,     "urn:default-graph")
  end
end

# ==============================
# Engine
# ==============================
class Engine
  RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type".freeze

  def initialize
    @hashed_patterns       = Set.new
    @patterns              = {}
    @endpoint_graph_mode   = {}
  end

  # --------------------------------------------------
  # Detect if endpoint contains named graphs
  # --------------------------------------------------
  def detect_named_graphs(endpoint_URL)
    puts "\n[Engine] Detecting named graph support for #{endpoint_URL}..."
    sparql = SPARQL::Client.new(endpoint_URL, method: :post)
    ask_query = <<~SPARQL
      ASK {
        GRAPH ?g {
          ?s ?p ?o .
        }
      }
    SPARQL

    begin
      result = sparql.ask(ask_query)
      @endpoint_graph_mode[endpoint_URL] = result ? :named : :default
      puts " → Graph mode detected: #{@endpoint_graph_mode[endpoint_URL]}"
    rescue
      @endpoint_graph_mode[endpoint_URL] = :default
      puts " → ASK failed; defaulting to :default"
    end
  end

  # --------------------------------------------------
  # Deduplication
  # --------------------------------------------------
  def in_database?(s, p, o, g)
    digest = Digest::SHA256.hexdigest("#{s}|#{p}|#{o}|#{g}")
    return true if @hashed_patterns.include?(digest)
    @hashed_patterns.add(digest)
    false
  end

  # --------------------------------------------------
  # Add pattern (with basic sanity filtering)
  # --------------------------------------------------
  def add_triple_pattern(type, s, p, o, g)
    s = s.to_s.strip
    p = p.to_s.strip
    o = o.to_s.strip
    g = g.to_s.strip.empty? ? "urn:default-graph" : g.to_s.strip

    return if p.empty?
    return if p == RDF_TYPE && o.empty?
    return if p == RDF_TYPE && o == type   # avoid redundant self-type

    @patterns[type] ||= []
    @patterns[type] << SPO.new(
      SPO_Subject:   s,
      SPO_Predicate: p,
      SPO_Object:    o,
      SPO_Graph:     g
    )
  end

  # --------------------------------------------------
  # Query Builder – now includes rdf:type capture
  # --------------------------------------------------
  def build_query(endpoint_URL, mode, type = nil)
    graph_mode = @endpoint_graph_mode[endpoint_URL]
    graph_pattern =
      if graph_mode == :named
        "GRAPH ?g { %CONTENT% }"
      else
        "%CONTENT%\nBIND(IRI(\"urn:default-graph\") AS ?g)"
      end

    case mode
    when :exploratory
      content = "?subject a ?type ."
      <<~SPARQL
        SELECT ?type ?g
        WHERE {
          #{graph_pattern.gsub("%CONTENT%", content)}
        }
      SPARQL

    when :fixed_subject
      content = <<~SPARQL
        ?subject a <#{type}> .
        ?subject a ?subject_type .                  # capture explicit rdf:type
        ?subject ?predicate ?object .
        OPTIONAL { ?object a ?object_type . }
      SPARQL
      <<~SPARQL
        SELECT ?subject_type ?predicate ?object_type ?g
        WHERE {
          #{graph_pattern.gsub("%CONTENT%", content)}
        }
      SPARQL

    when :fixed_object
      content = <<~SPARQL
        ?object a <#{type}> .
        ?subject ?predicate ?object .
        OPTIONAL { ?subject a ?subject_type . }     # capture incoming type
      SPARQL
      <<~SPARQL
        SELECT ?subject_type ?predicate ?g
        WHERE {
          #{graph_pattern.gsub("%CONTENT%", content)}
        }
      SPARQL
    end
  end

  # --------------------------------------------------
  # Execute query
  # --------------------------------------------------
  def query_endpoint(endpoint_URL, mode, type = nil)
    puts " [Engine] Executing #{mode} query for #{type || 'N/A'}..."
    sparql = SPARQL::Client.new(endpoint_URL, method: :post, headers: {
      "Accept" => "application/sparql-results+json"
    })
    query = build_query(endpoint_URL, mode, type)

    begin
      results = sparql.query(query)

      # Live feedback
      count = 0
      results.each { |_r| count += 1 } if results.respond_to?(:each)
      puts " → #{count} row(s) received."

      # Re-issue if needed (non-rewindable enumerator)
      if count > 0 && !results.respond_to?(:rewind)
        results = sparql.query(query)
      end

      results
    rescue StandardError => e
      warn " [Engine] SPARQL error on #{endpoint_URL}: #{e.message}"
      []
    end
  end

  # --------------------------------------------------
  # Extract patterns – now captures rdf:type
  # --------------------------------------------------
  def extract_patterns(endpoint_URLs)
    @endpoint_patterns = {}
    endpoint_URLs.each do |endpoint_URL|
      puts "\n=== Extracting patterns from #{endpoint_URL} ==="
      detect_named_graphs(endpoint_URL)

      @patterns = {}
      @hashed_patterns = Set.new
      types = []

      puts " [Engine] Phase 1: exploratory type scan..."
      results = query_endpoint(endpoint_URL, :exploratory)
      results.each do |solution|
        t = solution[:type]&.to_s
        next if t.nil? || t.empty?
        next if t =~ /openlink|w3\.org/
        types << t unless types.include?(t)
      end
      puts " → Detected #{types.size} classes."

      puts " [Engine] Phase 2: type expansion..."
      types.each do |type|
        # Outgoing properties + own rdf:type
        query_endpoint(endpoint_URL, :fixed_subject, type).each do |sol|
          g = sol[:g]&.to_s || "urn:default-graph"

          # 1. Add explicit rdf:type triple (if present and useful)
          subj_type = sol[:subject_type]&.to_s&.strip || ""
          if !subj_type.empty? && subj_type != type
            next if in_database?(type, RDF_TYPE, subj_type, g)
            add_triple_pattern(type, type, RDF_TYPE, subj_type, g)
          end

          # 2. Add normal outgoing property
          p = sol[:predicate]&.to_s&.strip || ""
          o = sol[:object_type]&.to_s&.strip || ""   # ← fixed here
          next if p.empty?
          next if in_database?(type, p, o, g)
          add_triple_pattern(type, type, p, o, g)
        end

        # Incoming properties
        query_endpoint(endpoint_URL, :fixed_object, type).each do |sol|
          s = sol[:subject_type]&.to_s&.strip || ""
          p = sol[:predicate]&.to_s&.strip || ""
          g = sol[:g]&.to_s || "urn:default-graph"
          next if s.empty? || p.empty?
          next if in_database?(s, p, type, g)
          add_triple_pattern(type, s, p, type, g)
        end
      end

      puts " → Total patterns extracted: #{@patterns.values.flatten.size}"
      @endpoint_patterns[endpoint_URL] = @patterns
    end

    @endpoint_patterns
  end

  # --------------------------------------------------
  # SHACL Generator – with suppression of empty rdf:type
  # --------------------------------------------------
  def shacl_generator(patterns_hash, output_dir)
    puts "\n[Engine] Generating SHACL files in #{output_dir}..."
    FileUtils.mkdir_p(output_dir) unless Dir.exist?(output_dir)

    patterns_hash.each do |url, patterns|
      puts " [Engine] Building SHACL for #{url}..."
      shacl = []

      shacl << <<~PREFIXES
        @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix sh:   <http://www.w3.org/ns/shacl#> .
        @prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
        @prefix dct:  <http://purl.org/dc/terms/> .
      PREFIXES

      grouped = {}
      patterns.each do |_t, values|
        values.each do |pattern|
          grouped[pattern.SPO_Subject] ||= []
          grouped[pattern.SPO_Subject] << pattern
        end
      end

      grouped.each do |subject, list|
        shacl << "<#{subject}Shape>\n"
        shacl << "  a sh:NodeShape ;\n"
        shacl << "  sh:targetClass <#{subject}> ;\n"
        shacl << "  dct:source <#{url}> ;\n"

        # Group by (predicate, object) to avoid duplicates
        grouped_props = list.group_by { |pat| [pat.SPO_Predicate, pat.SPO_Object.to_s.strip] }

        grouped_props.each_with_index do |((predicate, object_str), _patterns), idx|
          # QUICK SUPPRESSION: skip useless rdf:type with empty object
          next if predicate == RDF_TYPE && object_str.empty?

          shacl << "  sh:property [\n"

          if predicate == RDF_TYPE && !object_str.empty?
            # Special handling for real rdf:type
            shacl << "    sh:path rdf:type ;\n"
            shacl << "    sh:hasValue <#{object_str}> ;\n"
            # You can uncomment these if you want stricter cardinality:
            # shacl << "    sh:minCount 1 ;\n"
            # shacl << "    sh:maxCount 1 ;\n"
          else
            shacl << "    sh:path <#{predicate}> ;\n"
            if !object_str.empty? && object_str != "urn:default-graph"
              shacl << "    sh:class <#{object_str}> ;\n"
            end
          end

          shacl << "  ]#{ idx == grouped_props.size - 1 ? " ." : " ;" }\n"
        end

        shacl << "\n"
      end

      uri   = URI.parse(url)
      host  = (uri.host || "unknown").gsub(/[^a-zA-Z0-9]/, "_")
      path  = (uri.path.empty? ? "root" : uri.path).gsub(/[^a-zA-Z0-9]/, "_")
      base  = "#{host}#{path}".gsub(/_+/, "_").sub(/^_/, "").sub(/_$/, "")
      filename = "#{base}.ttl"

      output_path = File.join(output_dir, filename)
      if File.exist?(output_path)
        short = Digest::SHA256.hexdigest(url)[0..5]
        filename = "#{base}_#{short}.ttl"
        output_path = File.join(output_dir, filename)
      end

      puts " → Writing #{output_path}"
      File.write(output_path, shacl.join)
    end

    true
  end
end