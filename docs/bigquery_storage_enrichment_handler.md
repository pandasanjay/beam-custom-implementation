# BigQuery Storage Enrichment Handler (`BigQueryStorageEnrichmentHandler`)

The `BigQueryStorageEnrichmentHandler` is a component for Apache Beam's `Enrichment` transform. It allows you to enrich elements in your PCollection by fetching data from a Google Cloud BigQuery table using the efficient BigQuery Storage Read API.

## How to Use It

Here's a basic example of how to configure and use the `BigQueryStorageEnrichmentHandler`:

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.enrichment import Enrichment
# Assuming the handler is in the specified path
from apache_beam.transforms.enrichment_handlers.bigquery_enrichment_handlers_storage_read_api import BigQueryStorageEnrichmentHandler

# Your GCP Project ID (for billing and API access)
GCP_PROJECT_ID = "your-gcp-project-id"
# The BigQuery table to enrich from (e.g., a public dataset)
BQ_TABLE = "bigquery-public-data.usa_names.usa_1910_current"

# Sample input data
main_input_data = [
    beam.Row(name="Mary", source_year=1980),
    beam.Row(name="John", source_year=1990),
]

# Configure the handler
bq_enrichment_handler = BigQueryStorageEnrichmentHandler(
    project=GCP_PROJECT_ID,
    table_name=BQ_TABLE,
    fields=["name"],  # Field from input PCollection to use for joining
    row_restriction_template="name = '{name}'", # SQL-like filter for BQ table
                                                # '{name}' will be replaced by the value
                                                # from the 'name' field of the input Row.
    column_names=["name as bq_name", "number", "state"], # Columns to select from BQ
                                                        # "name as bq_name" renames BQ 'name'
                                                        # to 'bq_name' in the output.
    # Optional: Batching and parallelism for performance
    # min_batch_size=10,
    # max_batch_size=1000,
    # max_parallel_streams=5,
)

# Create and run the pipeline
options = PipelineOptions()
with beam.Pipeline(options=options) as p:
    main_pcoll = p | "CreateMainInput" >> beam.Create(main_input_data)

    enriched_pcoll = main_pcoll | "EnrichData" >> Enrichment(
        source_handler=bq_enrichment_handler
    )

    enriched_pcoll | "PrintResults" >> beam.Map(print)

```

This pipeline will take each `Row` from `main_input_data`, look up the `name` in the specified BigQuery table, and if a match is found, merge the selected `bq_name`, `number`, and `state` fields into the original `Row`.

## Parameters

The `BigQueryStorageEnrichmentHandler` is initialized with the following parameters:

| Parameter                     | Type                                                 | Required | Default Value             | Description                                                                                                                                                                                                                                                                                          |
| :---------------------------- | :--------------------------------------------------- | :------- | :------------------------ | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `project`                     | `str`                                                | Yes      | -                         | Your Google Cloud Project ID. This project will be used for billing and API access when interacting with BigQuery.                                                                                                                                                                   |
| `table_name`                  | `str`                                                | Yes      | -                         | The fully qualified BigQuery table name to read from, in the format `project.dataset.table`.                                                                                                                                                                                         |
| `row_restriction_template`    | `Optional[str]`                                      | No       | `None`                    | A string template for the `WHERE` clause of the BigQuery query (e.g., `"customer_id = '{id}' AND status = '{status_value}'"`). Placeholders `{}` are filled by values from input `beam.Row` (specified by `fields` or `condition_value_fn`). Mutually exclusive with `row_restriction_template_fn`. |
| `row_restriction_template_fn` | `Optional[Callable[[BeamRow], str]]`                 | No       | `None`                    | A callable that takes a `beam.Row` (an input element) and returns the `WHERE` clause string for that row (e.g., `lambda row: f"name = '{row.name}' AND year = {row.year}"`). Mutually exclusive with `row_restriction_template`.                                                              |
| `fields`                      | `Optional[list[str]]`                                | No       | `None`                    | A list of field names from the input `beam.Row` whose values will be used to format `row_restriction_template` and generate the join key (e.g., `["id", "status_value"]`). Mutually exclusive with `condition_value_fn`.                                                                 |
| `additional_condition_fields` | `Optional[list[str]]`                                | No       | `None`                    | A list of field names from input `beam.Row` used *only* for filtering in `row_restriction_template`, not for the join key. Cannot be used with `condition_value_fn`.                                                                                                                  |
| `column_names`                | `Optional[list[str]]`                                | No       | `None` (selects all: `*`) | A list of columns to select from BigQuery. Supports aliasing (e.g., `["original_column_name as alias_name"]`). If `None` or empty, all columns (`*`) are selected (not recommended for wide tables).                                                                               |
| `condition_value_fn`          | `Optional[Callable[[BeamRow], Dict[str, Any]]]`      | No       | `None`                    | A callable that takes a `beam.Row` and returns a dictionary for formatting `row_restriction_template` and generating the join key (e.g., `lambda row: {"id": row.user_id, "status_value": "ACTIVE"}`). Mutually exclusive with `fields`.                                          |
| `min_batch_size`              | `int`                                                | No       | `1`                       | The minimum number of input elements to group before making a `CreateReadSession` call to BigQuery. Used when `max_batch_size > 1`.                                                                                                                                               |
| `max_batch_size`              | `int`                                                | No       | `1000`                    | The maximum number of input elements to group. Batching reduces API calls. Set to `1` to disable.                                                                                                                                                                                  |
| `max_batch_duration_secs`     | `int`                                                | No       | `5`                       | The maximum time in seconds to wait before processing a batch, even if `max_batch_size` isn't reached.                                                                                                                                                                             |
| `max_parallel_streams`        | `Optional[int]`                                      | No       | `None`                    | Max parallel streams to read from a BigQuery Read Session via `ThreadPoolExecutor`. If `None`/`0`, defaults to API-provided stream count. Controls client-side concurrency.                                                                                                      |

## Features

* **Efficient Data Retrieval**: Utilizes the BigQuery Storage Read API for fast data reads, especially beneficial for large datasets.
* **Field Renaming**: Allows renaming of columns selected from BigQuery using the `original_name as new_name` syntax in `column_names`, preventing naming collisions.
* **Flexible Filtering**:
  * Supports static filter templates (`row_restriction_template`).
  * Supports dynamic, per-element filter generation (`row_restriction_template_fn`).
* **Targeted Keying**:
  * `fields`: Specifies which fields from the input element form the join key and are used for templating.
  * `additional_condition_fields`: Allows using fields for filtering *without* including them in the join key.
  * `condition_value_fn`: Provides full control over generating key-value pairs for both filtering and join key generation.
* **Batching**: Groups multiple input elements to make fewer `CreateReadSession` calls to BigQuery, reducing control plane overhead and potentially improving throughput for small, frequent lookups.
* **Parallel Stream Reading**: Internally uses a `ThreadPoolExecutor` to read data from multiple streams of a BigQuery Read Session in parallel, improving data fetching performance. Concurrency can be controlled with `max_parallel_streams`.
* **Automatic Client Management**: Handles the lifecycle of the `BigQueryReadClient`.

## Limitations

* **Permissions**: The GCP project used must have the BigQuery API enabled. The service account or credentials need specific BigQuery permissions:
  * `bigquery.tables.getData` on the table.
  * `bigquery.readsessions.create` on the parent project of the table.
  * `bigquery.readsessions.getData` on the read session.
  * (Typically roles like `BigQuery Data Viewer` and `BigQuery User` or `BigQuery Read Session User` grant these).
* **Filter Limits**:
  * The combined filter string for a batch (multiple `OR`-ed row filters) has length limits in BigQuery.
  * The `row_restriction` itself has a maximum length.
* **Single Row Enrichment Focus**: The handler is primarily designed to fetch data corresponding to a single input element for the `Enrichment` transform, which usually expects a one-to-one or one-to-few mapping. While it returns all matching BQ rows, the merging logic depends on the `Enrichment` transform's setup.
* **`GroupByKey` Issue with DirectRunner**: Using `GroupByKey` directly after an `Enrichment` transform that uses this handler has been observed to cause pipelines to hang or stall when using the DirectRunner. This behavior has not been evaluated on DataflowRunner.

## Considerations

* **Billing**: Costs will be incurred on the specified `project` for BigQuery Storage Read API usage.
* **Data Skew**: If join keys (from `fields` or `condition_value_fn`) are heavily skewed, it could lead to hotspots. The batching mechanism uses a combined `OR` filter, potentially affecting the amount of data returned per batch.
* **Error Handling**:
  * Exceptions from `condition_value_fn` or `row_restriction_template_fn`, or missing fields in input rows, may cause specific rows to be skipped or errors. Check logs.
  * BigQuery API errors (e.g., `BadRequest`, `NotFound`) are logged.
* **Data Type Compatibility**: Ensure data types in `row_restriction_template` match BigQuery column types. String values in templates generally need quotes (e.g., `name = '{user_name}'`).
* **Schema Cache**: The Arrow schema is fetched upon creating a read session. Mid-pipeline table schema changes might not be immediately reflected for active sessions.
* **Avoid "Select All" (`*`)**: Using `column_names=None` or `["*"]` to select all columns is supported but not recommended for performance, especially with wide tables. Explicitly list needed columns.
