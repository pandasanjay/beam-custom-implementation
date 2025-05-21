\
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.enrichment import Enrichment
from apache_beam.transforms.enrichment_handlers.bigquery_enrichment_handlers_storage_read_api import BigQueryStorageEnrichmentHandler

# --- Configuration ---
# Replace with your Google Cloud project ID that has BigQuery API enabled
# and permissions to read the public dataset.
# For public datasets, the billing project is usually your own project.
GCP_PROJECT_ID = "your-gcp-project-id"  # <--- IMPORTANT: REPLACE THIS!
PUBLIC_BQ_TABLE = "bigquery-public-data.usa_names.usa_1910_current"

def run_pipeline():
    options = PipelineOptions()

    # --- 1. Create main input PCollection ---
    main_input_data = [
        beam.Row(name="Mary", source_year=1980),
        beam.Row(name="John", source_year=1990),
        beam.Row(name="Patricia", source_year=1975),
        beam.Row(name="Michael", source_year=2000),
        beam.Row(name="NonExistentName", source_year=2020), # To test no match
    ]

    # --- 2. Configure BigQueryStorageEnrichmentHandler ---
    # We will join on the 'name' field from our main_input_data
    # with the 'name' field in the BigQuery table.
    # We want to select 'name', 'number', and 'state' from the BQ table.
    # The 'name' field from BQ will be aliased to 'bq_name' to avoid collision
    # if we didn't want to overwrite the original 'name'.
    # Here, we'll let it be, and the Enrichment transform handles merging.

    bq_enrichment_handler = BigQueryStorageEnrichmentHandler(
        project=GCP_PROJECT_ID,
        table_name=PUBLIC_BQ_TABLE,
        fields=["name"],  # Field(s) from main input to use for the JOIN condition
                          # This will be used in the WHERE clause: BQ_TABLE.name = main_input_data.name
        row_restriction_template="name = '{name}'", # Template for the WHERE clause
                                                    # {name} will be replaced by the value from main_input_data.name
        column_names=["name as bq_name", "number", "state"], # Columns to select from BQ.
                                                             # 'name as bq_name' renames 'name' from BQ to 'bq_name'
                                                             # in the enriched data.
        # Optional: for better performance with many small lookups
        # min_batch_size=10,
        # max_batch_size=100,
        # max_parallel_streams=5, # Control concurrency for reading BQ streams
    )

    with beam.Pipeline(options=options) as p:
        main_pcoll = p | "CreateMainInput" >> beam.Create(main_input_data)

        # --- 3. Apply the Enrichment transform ---
        enriched_pcoll = main_pcoll | "EnrichData" >> Enrichment(
            source_handler=bq_enrichment_handler,
            # field_mapping default behavior merges fields.
            # If 'bq_name' was just 'name', it would overwrite the original 'name'.
            # Since we aliased it to 'bq_name', it will be added as a new field.
            # The original 'name' and 'source_year' will be preserved.
        )

        # --- 4. Print results ---
        enriched_pcoll | "PrintResults" >> beam.Map(print)

if __name__ == "__main__":
    # Note: You might need to set up Application Default Credentials (ADC)
    # or provide service account credentials for this to run.
    # e.g., by running `gcloud auth application-default login`
    # or setting the GOOGLE_APPLICATION_CREDENTIALS environment variable.
    print(f"Running BigQuery Enrichment pipeline. Ensure GCP_PROJECT_ID is set correctly.")
    print(f"Using GCP Project: {GCP_PROJECT_ID}")
    print(f"Enriching against BQ Table: {PUBLIC_BQ_TABLE}")

    if GCP_PROJECT_ID == "your-gcp-project-id":
        print("\\nWARNING: GCP_PROJECT_ID is not set. Please replace 'your-gcp-project-id' in the script.")
        print("The pipeline will likely fail without a valid project ID for billing and API access.\\n")

    run_pipeline()

    print("\\nPipeline execution finished (or started if running with a runner like Dataflow).")
    print("If you see no output, check for errors and ensure your Beam runner executes the pipeline (e.g., DirectRunner).")
    print("Expected output will be rows like: ")
    print("Row(name='Mary', source_year=1980, bq_name='Mary', number=SOME_NUMBER, state='SOME_STATE')")
    print("... (multiple rows, potentially multiple matches per input if not careful with row_restriction_template)")
    print("Row(name='NonExistentName', source_year=2020) # This row will not be enriched if no match")

