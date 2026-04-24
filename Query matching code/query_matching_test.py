import os
import glob
import traceback
from datetime import datetime
from query_matching_class import shacl_validator

def run_tests_on_all_queries(input_dir="./queries", shacl_dir="shacl_output", output_file="test_results.txt"):
    with open(output_file, "w", encoding="utf-8") as report:
        report.write("SPARQL Query Batch Test Report\n")
        report.write("=================================\n")
        report.write(f"Started: {datetime.now()}\n")
        report.write(f"Input directory: {os.path.abspath(input_dir)}\n")
        report.write(f"SHACL directory: {os.path.abspath(shacl_dir)}\n\n")

        # Find all .sparql files recursively
        sparql_files = glob.glob(os.path.join(input_dir, "**", "*.sparql"), recursive=True)

        if not sparql_files:
            report.write(f"No .sparql files found in {input_dir}\n")
            print("→ No query files found.")
            return

        print(f"Found {len(sparql_files)} SPARQL query files. Processing...")

        sparql_files.sort()

        for i, query_path in enumerate(sparql_files):
            print(f"  [{i+1}/{len(sparql_files)}] {query_path}")

            report.write("┌──────────────────────────────────────────────\n")
            report.write(f"Query {i+1}: {query_path}\n")
            report.write("───────────────────────────────────────────────┘\n")

            try:
                with open(query_path, "r", encoding="utf-8") as f:
                    query_content = f.read()

                # # 1. Generate fake data
                # base_name = os.path.basename(query_path).replace(".sparql", "")
                # fake_ttl_path = f"fake_data_{base_name}.ttl"

                # fake_data_generator(query_content, fake_ttl_path)

                # report.write(f"  • Fake data generated → {fake_ttl_path}\n")

                # 2. Run SHACL validation / endpoint matching
                matching_endpoints = shacl_validator(query_content, shacl_dir)

                report.write(f"  • Matching endpoints ({len(matching_endpoints)}):\n")

                if not matching_endpoints:
                    report.write("    (none)\n")
                else:
                    for ep in matching_endpoints:
                        report.write(f"      - {ep}\n")

                # Optional: clean up generated fake file
                # if os.path.exists(fake_ttl_path):
                #     os.remove(fake_ttl_path)

            except UnicodeDecodeError as e:
                report.write("  ERROR: Invalid encoding in query file\n")
                report.write(f"  → {str(e)}\n")

            except Exception as e:
                report.write(f"  ERROR: {type(e).__name__}\n")
                report.write(f"  → {str(e)}\n")
                report.write("  Backtrace (first 5 lines):\n")

                tb_lines = traceback.format_exc().splitlines()
                for line in tb_lines[:5]:
                    report.write(f"    {line}\n")

            report.write("\n")

        report.write("=================================\n")
        report.write(f"Finished: {datetime.now()}\n")
        report.write(f"Processed {len(sparql_files)} queries.\n")

    print(f"\nDone. Results written to: {os.path.abspath(output_file)}")


run_tests_on_all_queries("./Query-matching-code/Query matching code/queries", "./shacl_output", "batch_test_results_new.txt")