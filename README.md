# beam-custom-implementation

This project provides a custom Apache Beam PTransform for enriching data using the BigQuery Storage Read API.

## Project Structure

```text
LICENSE
README.md
requirements.txt
apache_beam/
  transforms/
    bigquery_storage_read_api.py # Core PTransform logic
    enrichment_handlers/
      bigquery_enrichment_handlers_storage_read_api.py # (If applicable, describe its role)
examples/
  custom_ptranform_read_from_bq_using_storage_read.py # Example pipeline
```

## Setup and Installation

Follow these steps to set up your environment and run the example pipeline.

### Prerequisites

* Python 3.8+
* Google Cloud SDK (gcloud CLI): Ensure you have the gcloud CLI installed and authenticated. This is necessary for interacting with Google Cloud services, including BigQuery.
  * Install gcloud: [https://cloud.google.com/sdk/docs/install](https://cloud.google.com/sdk/docs/install)
  * Authenticate: Run `gcloud auth application-default login`
* Enable BigQuery Storage Read API: Make sure the BigQuery Storage Read API is enabled in your Google Cloud Project.
* Set `GOOGLE_CLOUD_PROJECT`: It's recommended to set the `GOOGLE_CLOUD_PROJECT` environment variable to your Google Cloud Project ID, especially if you plan to use a project for billing or quotas for BigQuery API calls (even for public datasets).

  ```shell
  export GOOGLE_CLOUD_PROJECT="your-gcp-project-id"
  ```

### Installation Steps

1. **Clone the repository (if you haven't already):**

   ```shell
   git clone <repository-url>
   cd beam-custom-implementation
   ```

2. **Create and activate a virtual environment:**

   ```shell
   python3 -m venv venv
   source venv/bin/activate
   ```
   *(On Windows, use `venv\Scripts\activate`)*

3. **Install dependencies:**
   The `requirements.txt` file lists the necessary Python packages.

   ```shell
   pip install -r requirements.txt
   ```

## Running the Example

The example pipeline is located in `examples/custom_ptranform_read_from_bq_using_storage_read.py`. It demonstrates how to use the `BatchedParallelBigQueryEnrichmentTransform` to enrich a sample PCollection with data from a public BigQuery table (`bigquery-public-data.usa_names.usa_1910_current`).

To run the example:

1. **Ensure your virtual environment is activated.**
2. **Navigate to the root directory of the project (`beam-custom-implementation`).**
3. **Execute the example script:**

   ```shell
   python examples/custom_ptranform_read_from_bq_using_storage_read.py
   ```

   You should see output in your console showing the initial data, logging messages from the Beam pipeline and the transform, and finally the enriched data (or original data if no match was found in BigQuery).

### Understanding the Example Output

The example script prints the enriched `PCollection` to the console. Each element will be a Beam `Row` object.

* If a match is found in BigQuery for the `join_key_field` (which is 'name' in the example), the original row's fields will be merged with the columns selected from BigQuery (`name`, `number`, `state`).
* If no match is found, the original row will be outputted without any new fields from BigQuery.

### Customization

* **Input Data**: Modify the `input_dicts` list in the example script to test with different data.
* **BigQuery Table**: Change the `project`, `table_name`, `join_key_field`, `fields`, `row_restriction_template`, and `column_names` parameters in the `BatchedParallelBigQueryEnrichmentTransform` instantiation to point to your own BigQuery tables and configure the join and selection logic.
* **Batching**: Adjust `min_batch_size` and `max_batch_size` to test the batching behavior of the transform.
* **Enrichment Mode**: Experiment with `enrichment_mode` (`"merge_new"` or `"add_nested"`) and `nested_bq_key` (if using `"add_nested"`).

## Running Tests (If applicable)

*(Add instructions here if you have a dedicated test suite, e.g., using `pytest`)*