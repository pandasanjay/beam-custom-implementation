# filepath: /home/sanjay-dev/Workspace/beam-custom-implementation/examples/custom_ptranform_read_from_bq_using_storage_read.py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import Row as BeamRow

# Adjust the import path based on your project structure
# Assuming the script is run from the root of the beam-custom-implementation directory
# or that the apache_beam module within your project is in the PYTHONPATH
from custom_beam.transforms.bigquery_storage_read_api import BatchedParallelBigQueryEnrichmentTransform

# --- Configuration ---
# Replace with your Google Cloud project ID that will be used for billing
# and API quotas, even when accessing public datasets.
GCP_PROJECT_ID_FOR_BILLING = "your-gcp-project-for-billing"  # <--- IMPORTANT: REPLACE THIS!

def run_pipeline():
    """Runs the test pipeline."""

    pipeline_options = PipelineOptions()
    # If your GCP project for billing is not set via environment variable (GOOGLE_CLOUD_PROJECT)
    # or gcloud config, you might need to set it explicitly in PipelineOptions:
    # from apache_beam.options.pipeline_options import GoogleCloudOptions
    # gcp_options = pipeline_options.view_as(GoogleCloudOptions)
    # gcp_options.project = GCP_PROJECT_ID_FOR_BILLING

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Sample input data - list of dictionaries
        # Each dictionary must contain the join_key_field, in this case 'name'
        input_dicts = [
            {'id': 1, 'name': 'James', 'extra_info': 'some_data_1'},
            {'id': 2, 'name': 'Mary', 'extra_info': 'some_data_2'},
            {'id': 3, 'name': 'John', 'extra_info': 'some_data_3'},
            {'id': 4, 'name': 'Patricia', 'extra_info': 'some_data_4'},
            {'id': 5, 'name': 'Robert', 'extra_info': 'some_data_5'},
            # Add more names to test batching effectively
            {'id': 6, 'name': 'Jennifer', 'extra_info': 'some_data_6'},
            {'id': 7, 'name': 'Michael', 'extra_info': 'some_data_7'},
            {'id': 8, 'name': 'Linda', 'extra_info': 'some_data_8'},
            {'id': 9, 'name': 'William', 'extra_info': 'some_data_9'},
            {'id': 10, 'name': 'Elizabeth', 'extra_info': 'some_data_10'},
            {'id': 11, 'name': 'David', 'extra_info': 'some_data_11'}, # Test potential non-match
            {'id': 12, 'name': 'Susan', 'extra_info': 'some_data_12'},
        ]

        # Convert dictionaries to beam.Row objects
        # The BatchedParallelBigQueryEnrichmentTransform uses element._asdict(),
        # implying beam.Row inputs for the _extract_key_and_dict function.
        input_data = (
            pipeline
            | "CreateInput" >> beam.Create(input_dicts)
            | "ToBeamRow" >> beam.Map(lambda x: BeamRow(**x))
        )

        # Configure and apply the enrichment transform
        enriched_data = input_data | "EnrichWithBigQueryData" >> BatchedParallelBigQueryEnrichmentTransform(
            project=GCP_PROJECT_ID_FOR_BILLING,  # Project ID for API usage billing/quotas
            table_name="bigquery-public-data.usa_names.usa_1910_current", # Public table
            join_key_field="name",  # Field in input elements to join on
            fields=["name"],  # Fields from input elements to use in row_restriction_template
            # Ensure the values are properly quoted if they are strings in SQL
            row_restriction_template="name = '{name}'",  # SQL-like template for filtering BQ
            column_names=["name", "number", "state"],  # Columns to select from BigQuery
            # Batching parameters (adjust as needed for testing)
            min_batch_size=2, # Small for testing
            max_batch_size=5, # Small for testing
            enrichment_mode="merge_new" # or "add_nested"
            # nested_bq_key='bq_profile' # if using "add_nested"
        )

        # Log the enriched data
        enriched_data | "LogEnrichedData" >> beam.Map(print)

if __name__ == '__main__':
    # You might need to configure logging to see Beam's output clearly,
    # especially for debugging messages from the transform.
    import logging
    logging.getLogger().setLevel(logging.INFO) # Set to DEBUG for more verbose output
    
    # It's good practice to also set the project for the pipeline runner if needed,
    # especially if running on Dataflow or if API calls require a billing project.
    # Example:
    # options = PipelineOptions(
    #     flags=None, # Pass command-line arguments if any
    #     project=GCP_PROJECT_ID_FOR_BILLING, # Replace with your GCP project ID
    #     # runner='DataflowRunner', # Uncomment to run on Dataflow
    #     # temp_location='gs://your-gcs-bucket/temp/', # Required for Dataflow
    #     # region='your-gcp-region' # Required for Dataflow
    # )
    # run_pipeline(options) # Pass options here
    
    # For local testing with DirectRunner, explicit project for runner might not be needed
    # if ADC (Application Default Credentials) are set up correctly.
    # However, the BigQuery Storage Read API calls themselves will use GCP_PROJECT_ID_FOR_BILLING
    # for quota and billing, even for public datasets. This is often handled by ADC
    # or explicitly setting GOOGLE_CLOUD_PROJECT environment variable.

    print("Starting Beam pipeline to test BatchedParallelBigQueryEnrichmentTransform...")
    print(f"Using GCP Project for Billing/Quotas: {GCP_PROJECT_ID_FOR_BILLING}")
    print("Ensure you have authenticated with GCP (e.g., `gcloud auth application-default login`)")
    print("and the BigQuery API is enabled in the project used for billing.")
    
    if GCP_PROJECT_ID_FOR_BILLING == "your-gcp-project-for-billing":
        print("\nWARNING: GCP_PROJECT_ID_FOR_BILLING is not set. Please replace 'your-gcp-project-for-billing' in the script.")
        print("The pipeline will likely fail or use an unintended project for BigQuery API calls.\n")

    print("Output will show enriched rows (or original rows if no BQ match).")
    print("--------------------------------------------------------------------")
    run_pipeline()
    print("--------------------------------------------------------------------")
    print("Beam pipeline finished.")
