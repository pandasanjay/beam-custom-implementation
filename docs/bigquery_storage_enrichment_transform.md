# BatchedParallelBigQueryEnrichmentTransform Documentation

The `BatchedParallelBigQueryEnrichmentTransform` is an Apache Beam PTransform designed to enrich elements in a PCollection by fetching related data from Google BigQuery using the BigQuery Storage Read API. It offers efficient batching, parallel stream reading, and flexible enrichment modes.

## Overview

This transform works through the following main stages:

1.  **Keying Input**: Input elements are keyed based on a specified `join_key_field`.
2.  **Grouping Input**: Elements with the same key are grouped together.
3.  **Batching Key-Groups**: Groups of keys (and their associated elements) are batched to reduce the number of API calls.
4.  **Creating Read Sessions**: For each batch of key-groups, a single BigQuery Read Session is created. A combined filter (using `OR` logic) is constructed based on the keys in the batch to fetch relevant rows from the BigQuery table.
5.  **Parallel Stream Reading**: The Read Session provides multiple data streams. These streams are read in parallel by Beam's distributed runners.
6.  **Keying BigQuery Results**: Rows read from BigQuery are keyed based on the corresponding join column.
7.  **Joining Data**: The original input elements are joined with the BigQuery results using a CoGroupByKey operation.
8.  **Enriching Elements**: Each input element is then enriched with the fetched BigQuery data according to the specified `enrichment_mode`.

## Parameters

The transform is initialized with the following parameters:

| Parameter                     | Type                                                 | Required | Default Value                  | Description                                                                                                                                                                                                                                                           |
| :---------------------------- | :--------------------------------------------------- | :------- | :----------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `project`                     | `str`                                                | Yes      | -                              | The Google Cloud Project ID where the BigQuery table resides and which will be used for API calls (e.g., for quota, billing).                                                                                                                                             |
| `table_name`                  | `str`                                                | Yes      | -                              | The fully qualified BigQuery table name in the format `project.dataset.table`.                                                                                                                                                                                        |
| `join_key_field`              | `str`                                                | Yes      | -                              | The name of the field in the input PCollection elements that will be used to join with data from the BigQuery table. This field's value will be used to look up matching rows in BigQuery.                                                                               |
| `row_restriction_template`    | `Optional[str]`                                      | No       | `None`                         | A string template for the `WHERE` clause of the BigQuery query (e.g., `"user_id = {user_id} AND status = '{status}'"`). Placeholders `{field_name}` will be filled from the input element's `fields`. Mutually exclusive with `row_restriction_template_fn`.         |
| `row_restriction_template_fn` | `Optional[RowRestrictionTemplateFn]`                 | No       | `None`                         | A callable that takes a dictionary of condition values and the input `BeamRow` and returns a dynamically generated row restriction string. Mutually exclusive with `row_restriction_template`. `RowRestrictionTemplateFn = Callable[[Dict[str, Any], BeamRow], str]` |
| `fields`                      | `Optional[list[str]]`                                | No       | `None`                         | A list of field names from the input elements whose values will be used to format the `row_restriction_template` or be passed to `row_restriction_template_fn` and `condition_value_fn`. Required if `row_restriction_template` is used and not `condition_value_fn`. |
| `additional_condition_fields` | `Optional[list[str]]`                                | No       | `None`                         | A list of additional field names from input elements to be made available for `row_restriction_template_fn` or `condition_value_fn`, even if not explicitly listed in `fields`. Cannot be used with `condition_value_fn`.                                          |
| `column_names`                | `Optional[list[str]]`                                | No       | `None` (selects all: `*`)      | A list of column names to select from the BigQuery table. Supports aliasing (e.g., `["bq_col_name as new_name", "another_col"]`). If `None` or `["*"]`, all columns are selected.                                                                                 |
| `condition_value_fn`          | `Optional[ConditionValueFn]`                         | No       | `None`                         | A callable that takes an input `BeamRow` and returns a dictionary of key-value pairs used for formatting the `row_restriction_template` or passed to `row_restriction_template_fn`. Mutually exclusive with `fields`. `ConditionValueFn = Callable[[BeamRow], Dict[str, Any]]` |
| `select_bq_row_fn`            | `Optional[SelectBQRowFn]`                            | No       | `None`                         | A callable that takes a list of BigQuery row dictionaries (all matching the join key) and returns a single BQRowDict to be used for enrichment. If `None`, the first row is chosen. `SelectBQRowFn = Callable[[List[BQRowDict]], BQRowDict]` |
| `enrichment_mode`             | `EnrichmentMode`                                     | No       | `"merge_new"`                  | Defines how BigQuery data is merged into the input element. Options: `"merge_new"` (adds BQ fields only if not present in input), `"add_nested"` (adds all BQ fields as a nested dictionary).                                                                     |
| `nested_bq_key`               | `Optional[str]`                                      | No       | `None`                         | If `enrichment_mode` is `"add_nested"`, this is the key under which the BigQuery data dictionary will be added to the input element. Required if `enrichment_mode` is `"add_nested"`.                                                                              |
| `min_batch_size`              | `int`                                                | No       | `100`                          | The minimum number of key-groups to include in a batch before creating a BigQuery Read Session.                                                                                                                                                                     |
| `max_batch_size`              | `int`                                                | No       | `1000`                         | The maximum number of key-groups to include in a batch.                                                                                                                                                                                                               |

## How to Use

1.  **Import the transform**:
    ```python
    from apache_beam.transforms.bigquery_storage_read_api import BatchedParallelBigQueryEnrichmentTransform
    ```

2.  **Ensure your input PCollection contains `apache_beam.Row` objects or objects with an `_asdict()` method.** The transform internally converts elements to dictionaries. Each element must contain the `join_key_field`.

3.  **Apply the transform in your pipeline**:

    ```python
    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions
    from apache_beam.pvalue import Row as BeamRow
    # Adjust import path as per your project structure
    from apache_beam.transforms.bigquery_storage_read_api import BatchedParallelBigQueryEnrichmentTransform

    # Example input data
    input_elements = [
        BeamRow(user_id="user123", region="US", other_data="foo", event_timestamp="2023-01-15T10:00:00Z"),
        BeamRow(user_id="user456", region="EU", other_data="bar", event_timestamp="2023-01-16T11:00:00Z"),
        BeamRow(user_id="user123", region="US", other_data="foo_updated", event_timestamp="2023-01-15T12:00:00Z"), # Same user_id, different data
        BeamRow(user_id="user789", region="US", other_data="baz", event_timestamp="2023-01-17T09:00:00Z"), # This user might not be in BQ
    ]

    # Example callback function for select_bq_row_fn
    def select_latest_bq_row(bq_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Selects the BQ row with the most recent 'last_login' timestamp."""
        if not bq_rows:
            return {} # Should ideally not be called with empty list by the transform
        # Assuming 'last_login' is a field in your BQ table that can be compared (e.g., ISO timestamp string or datetime object)
        return max(bq_rows, key=lambda r: r.get("last_login", "")) # Handle missing 'last_login' gracefully

    with beam.Pipeline(options=PipelineOptions()) as p:
        input_pcoll = p | "CreateInput" >> beam.Create(input_elements)

        enriched_pcoll = input_pcoll | "EnrichWithBQData" >> BatchedParallelBigQueryEnrichmentTransform(
            project="your-gcp-project-id",
            table_name="your-gcp-project-id.your_dataset.user_profiles",
            join_key_field="user_id",  # Field in input_elements to join on
            fields=["user_id", "region"],
            row_restriction_template="profile_id = '{user_id}' AND user_region = '{region}'",
            column_names=["profile_id as bq_user_id", "email", "status", "last_login"],
            select_bq_row_fn=select_latest_bq_row, # Using the custom selection function
            enrichment_mode="merge_new",
            min_batch_size=50,
            max_batch_size=500
        )

        enriched_pcoll | "LogOutput" >> beam.Map(print)
    ```

### Defining Row Restriction Logic

You have two primary ways to define the filtering logic (row restriction) for BigQuery:

*   **`row_restriction_template` with `fields`**:
    *   Provide a SQL `WHERE` clause-like string with placeholders (e.g., `id = {join_key_field_value}`).
    *   `fields` specifies which fields from the input element should be used to fill these placeholders.
    *   Example: `fields=["user_id"]`, `row_restriction_template="customer_id = '{user_id}'"`

*   **`row_restriction_template_fn` with `fields` (or `condition_value_fn`)**:
    *   Provide a Python callable that receives:
        1.  `condition_values_dict`: A dictionary containing values derived from the input element (based on `fields` or `condition_value_fn`).
        2.  `beam_row`: The representative input `BeamRow` for the current key.
    *   This function should return the complete filter string for a single input key.
    *   This allows for more complex, dynamic filter generation.
    *   Example:
        ```python
        def my_filter_builder(condition_values: dict, input_row: BeamRow) -> str:
            # condition_values will contain {'user_id': 'some_value'} if fields=['user_id']
            # input_row can be used for more complex logic, e.g., date calculations
            return f"id = '{condition_values['user_id']}' AND signup_date > '{input_row.signup_date - timedelta(days=30)}'"

        # ... in transform:
        # fields=["user_id"], # 'user_id' will be in condition_values
        # row_restriction_template_fn=my_filter_builder
        ```

*   **`condition_value_fn`**:
    *   If you need more control over how the values for the template/template_fn are generated than just picking fields directly, you can use `condition_value_fn`.
    *   This function takes the input `BeamRow` and must return a dictionary of values. These values are then used by `row_restriction_template` or passed as `condition_values_dict` to `row_restriction_template_fn`.
    *   This is mutually exclusive with directly specifying `fields` for the purpose of populating condition values.

## Features

*   **Efficient Batching**: Groups multiple distinct join keys into a single `CreateReadSession` call to BigQuery, reducing API overhead. The actual data fetching for these keys is then parallelized by the BigQuery Storage API streams.
*   **Parallel Data Reading**: Leverages the BigQuery Storage Read API's ability to split data into multiple streams, which are then processed in parallel by Beam workers.
*   **Configurable Enrichment Modes**:
    *   `merge_new` (default): Fetched BigQuery fields are added to the input element only if the field name does not already exist in the input element. This prevents overwriting existing data.
    *   `add_nested`: All fetched BigQuery fields (after any aliasing) are added as a new nested dictionary within the input element, using the key specified by `nested_bq_key`.
*   **Customizable BigQuery Row Selection**: When multiple rows are returned from BigQuery for a single join key (after applying the row restriction), the `select_bq_row_fn` callback allows you to define custom logic to choose which specific BigQuery row should be used for enrichment (e.g., selecting the most recent record). If not provided, the transform defaults to picking the first row returned.
*   **Column Selection and Aliasing**: Allows specifying which columns to retrieve from BigQuery and aliasing them to different names in the enriched output.
*   **Resilience to Missing Data (No Discard)**: If an input element's key is not found in the BigQuery table, or if no data is returned for that key, **the original input element is still yielded downstream**. It will not be enriched with BigQuery data (and an `__enrichment_status` field might indicate this), but it is not discarded.
*   **Separate Join Key and Filter Conditions**:
    *   The `join_key_field` is used for the primary join operation between the input PCollection and the BigQuery data.
    *   The `row_restriction_template` (or `row_restriction_template_fn`) allows you to define more complex filtering conditions on the BigQuery table that can involve other fields from the input element (specified via `fields` or `condition_value_fn`), not just the `join_key_field`. This means you can filter the BigQuery data based on multiple attributes of your input records before the join occurs. For example, you might join on `user_id` but only want BQ records where `bq_table.status = 'active'` and `bq_table.registration_date > input_element.some_date_field`.

## Limitations

*   **Single Key Join**: The current implementation only supports joining on a single key field (`join_key_field`). Composite keys are not directly supported for the join mechanism itself, though multiple fields can be used in the `row_restriction` logic.
*   **Input Element Type**: Assumes input elements are `apache_beam.Row` or have an `_asdict()` method for conversion to a dictionary.
*   **Error Handling for BQ Data**: If multiple rows are returned from BigQuery for a single join key after applying the row restriction, the `select_bq_row_fn` callback can be used to implement a selection strategy. If this callback is not provided, the transform defaults to selecting the first BQ row encountered and logs a warning if multiple were found.

## Internal `__enrichment_status` field

The transform adds a field named `__enrichment_status` to the output `BeamRow` objects. This field can provide insights into the outcome of the enrichment for that specific element. Example values include:
* `SUCCESS`: Enrichment was successful.
* `NO_BQ_DATA`: No matching data was found in BigQuery for the key.
* `NO_BQ_DATA_NESTED_KEY_MISSING`: `enrichment_mode` was `add_nested` but `nested_bq_key` was not provided.
* `SUCCESS_UNKNOWN_MODE`: An unknown `enrichment_mode` was specified.
* Other internal error indicators might be appended.
