# -*- coding: utf-8 -*-
# Copyright 2024 Google LLC & Apache Software Foundation (Original License Header)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
BigQuery Enrichment Source Handler using the BigQuery Storage Read API
with support for field renaming, additional filtering fields, dynamic templates,
batching of input requests, and internal parallel stream reading via ThreadPoolExecutor.
"""
import logging
import re
import time # For timing internal operations if needed
import concurrent.futures # For parallel stream reading
from collections.abc import Callable, Mapping
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple, Union

import apache_beam as beam
# Import Row explicitly for type checking where needed
from apache_beam.pvalue import Row as BeamRow
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from google.api_core.exceptions import BadRequest, NotFound, GoogleAPICallError
from google.cloud.bigquery_storage import BigQueryReadClient, types
from google.cloud import bigquery_storage
import pyarrow as pa
import pyarrow.ipc # Import the ipc module

# --- Configure Logging ---
logger = logging.getLogger(__name__)


# Type hints for functions
# Input functions expect beam.Row for clarity, use beam.Row.as_dict inside if needed
ConditionValueFn = Callable[[BeamRow], Dict[str, Any]]
RowRestrictionTemplateFn = Callable[[BeamRow], str]
BQRowDict = Dict[str, Any] # Added for clarity

# Regex to parse "column as alias" format, ignoring case for "as"
ALIAS_REGEX = re.compile(r"^(.*?)\s+as\s+(.*)$", re.IGNORECASE)

def _validate_bigquery_metadata(
    project, table_name, row_restriction_template, row_restriction_template_fn, fields, condition_value_fn, additional_condition_fields
):
    """Validates parameters for Storage API usage."""
    if not project: raise ValueError("`project` must be provided.")
    if not table_name: raise ValueError("`table_name` must be provided.")
    if (row_restriction_template and row_restriction_template_fn) or \
       (not row_restriction_template and not row_restriction_template_fn):
        raise ValueError("Provide exactly one of `row_restriction_template` or `row_restriction_template_fn`.")
    if (fields and condition_value_fn) or (not fields and not condition_value_fn):
        raise ValueError("Provide exactly one of `fields` or `condition_value_fn`.")
    if additional_condition_fields and condition_value_fn:
        raise ValueError("`additional_condition_fields` cannot be used with `condition_value_fn`.")


class BigQueryStorageEnrichmentHandler(EnrichmentSourceHandler[Union[BeamRow, list[BeamRow]], Union[BeamRow, list[BeamRow]]]):
    """Enrichment handler for Google Cloud BigQuery using the Storage Read API.

    Supports field renaming (`"original as alias"` in `column_names`).
    Supports `additional_condition_fields` for filtering without affecting keys.
    Supports dynamic filter templates via `row_restriction_template_fn`.

    Leverages batching (via `min_batch_size`, `max_batch_size`) to make one
    `CreateReadSession` API call per batch of input elements, reducing control
    plane load. Uses a combined `OR` filter for the batch.

    Internally uses a ThreadPoolExecutor to read multiple streams from a single
    session in parallel. Control concurrency with `max_parallel_streams`.

    Requires: `project`, `table_name`, one template option, one field/value option.
    """

    def __init__(
        self,
        project: str,
        table_name: str,
        *,
        row_restriction_template: Optional[str] = None,
        row_restriction_template_fn: Optional[RowRestrictionTemplateFn] = None,
        fields: Optional[list[str]] = None, # Fields for KEY and filtering
        additional_condition_fields: Optional[list[str]] = None, # Fields ONLY for filtering
        column_names: Optional[list[str]] = None, # Columns to select + aliases
        condition_value_fn: Optional[ConditionValueFn] = None, # Alt way to get filter/key values
        min_batch_size: int = 1,
        max_batch_size: int = 1000, # Batching enabled by default
        max_batch_duration_secs: int = 5,
        # --- Added Parameter for ThreadPoolExecutor ---
        max_parallel_streams: Optional[int] = None, # Max workers for ThreadPoolExecutor
        # --- End Added Parameter ---
    ):
        """ Initializes the handler. See class docstring for arg details. """
        _validate_bigquery_metadata(
            project, table_name, row_restriction_template, row_restriction_template_fn,
            fields, condition_value_fn, additional_condition_fields
        )
        self.project = project
        self.table_name = table_name
        self.row_restriction_template = row_restriction_template
        self.row_restriction_template_fn = row_restriction_template_fn
        self.fields = fields
        self.additional_condition_fields = additional_condition_fields or []
        self.condition_value_fn = condition_value_fn
        # --- Store max_parallel_streams ---
        self.max_parallel_streams = max_parallel_streams
        # --- End store ---


        # --- Parse column_names for selection and renaming ---
        self._rename_map: Dict[str, str] = {}
        bq_columns_to_select_set: Set[str] = set()
        self._select_all_columns = False
        if column_names:
            for name_or_alias in column_names:
                match = ALIAS_REGEX.match(name_or_alias)
                if match:
                    original_col, alias_col = match.group(1).strip(), match.group(2).strip()
                    if not original_col or not alias_col: raise ValueError(f"Invalid alias: '{name_or_alias}'")
                    bq_columns_to_select_set.add(original_col)
                    self._rename_map[original_col] = alias_col
                else:
                    col = name_or_alias.strip()
                    if not col: raise ValueError("Empty column name.")
                    if col == '*': self._select_all_columns = True; break
                    bq_columns_to_select_set.add(col)
        else: self._select_all_columns = True

        # --- Determine final list of columns to select from BQ ---
        key_gen_fields_set = set(self.fields or [])
        if self._select_all_columns:
             self._bq_select_columns = ["*"]
             if key_gen_fields_set: logger.debug(f"Selecting all columns ('*'). Key fields {key_gen_fields_set} assumed present.")
        else:
             fields_to_ensure_selected = set()
             if self.fields:
                 reverse_rename_map = {v: k for k, v in self._rename_map.items()}
                 for field in self.fields:
                      original_name = reverse_rename_map.get(field, field)
                      fields_to_ensure_selected.add(original_name)
             final_select_set = bq_columns_to_select_set.union(fields_to_ensure_selected)
             self._bq_select_columns = sorted(list(final_select_set))
             if not self._bq_select_columns: raise ValueError("No columns determined for selection.")

        logger.info(f"Handler Initialized. Selecting BQ Columns: {self._bq_select_columns}. Renaming map: {self._rename_map}")
        # --- End column parsing ---

        self._batching_kwargs = {}
        if max_batch_size > 1:
             self._batching_kwargs["min_batch_size"] = min_batch_size
             self._batching_kwargs["max_batch_size"] = max_batch_size
             self._batching_kwargs["max_batch_duration_secs"] = max_batch_duration_secs
        else:
             self._batching_kwargs["min_batch_size"] = 1
             self._batching_kwargs["max_batch_size"] = 1

        self._client: Optional[BigQueryReadClient] = None
        self._arrow_schema: Optional[pa.Schema] = None

    def __enter__(self):
        """Initializes the BigQuery Storage client."""
        if not self._client:
            self._client = BigQueryReadClient()
            logger.info("BigQueryStorageEnrichmentHandler: Client created.")
        self._arrow_schema = None

    def _get_condition_values_dict(self, req: BeamRow) -> Optional[Dict[str, Any]]:
        """Gets values dictionary for filter formatting and key generation."""
        try:
            if self.condition_value_fn:
                values_dict = self.condition_value_fn(req)
                if values_dict is None or any(v is None for v in values_dict.values()):
                     logger.warning(f"condition_value_fn returned None or dict with None value(s). Skipping request: {req}. Values: {values_dict}")
                     return None
                return values_dict
            elif self.fields is not None:
                req_dict = BeamRow.as_dict(req)
                values_dict = {}
                all_req_fields = (self.fields or []) + self.additional_condition_fields
                for field in all_req_fields:
                    if field not in req_dict or req_dict[field] is None:
                         logger.warning(f"Input row missing required field '{field}' or value is None. Skipping request: {req}")
                         return None
                    values_dict[field] = req_dict[field]
                return values_dict
            else: raise ValueError("Internal error: Neither fields nor condition_value_fn.")
        except Exception as e:
             logger.error(f"Error getting condition values for row {req}: {e}", exc_info=True)
             return None

    def _build_single_row_filter(self, req_row: BeamRow, condition_values_dict: Dict[str, Any]) -> str:
        """Builds the filter string part for a single row."""
        try:
            if self.row_restriction_template_fn:
                template = self.row_restriction_template_fn(req_row)
                if not isinstance(template, str): raise TypeError("row_restriction_template_fn must return str")
            elif self.row_restriction_template:
                template = self.row_restriction_template
            else: raise ValueError("Internal Error: No template available.")
            return template.format(**condition_values_dict)
        except Exception as e:
            logger.error(f"Error building filter for row {req_row} with values {condition_values_dict}: {e}", exc_info=True)
            return ""

    def _apply_renaming(self, bq_row_dict: BQRowDict) -> BQRowDict:
        """Applies the renaming map."""
        if not self._rename_map: return bq_row_dict
        return {self._rename_map.get(k, k): v for k, v in bq_row_dict.items()}

    def _arrow_to_dicts_static(self, response: types.ReadRowsResponse, arrow_schema: pa.Schema) -> Iterator[BQRowDict]:
        """ Static helper to convert Arrow RecordBatch using provided schema. """
        # Made static to be callable by _read_single_stream_worker
        if response.arrow_record_batch:
            if not arrow_schema:
                 logger.error("Cannot process Arrow batch: Schema not provided for static conversion.")
                 return
            try:
                serialized_batch = response.arrow_record_batch.serialized_record_batch
                batch_buffer = pa.py_buffer(serialized_batch)
                record_batch = pa.ipc.read_record_batch(batch_buffer, arrow_schema)
                arrow_table = pa.Table.from_batches([record_batch])
                yield from arrow_table.to_pylist()
            except Exception as e:
                logger.error(f"Error converting Arrow batch (static): {e}", exc_info=True)


    # --- MODIFIED METHOD to use ThreadPoolExecutor ---
    def _execute_storage_read(self, combined_row_filter: str) -> List[BQRowDict]:
        """Performs BQ Storage read using ThreadPoolExecutor for streams, returns dicts with ORIGINAL keys."""
        if not self._client:
            self.__enter__() # Ensure client is initialized
            if not self._client: raise RuntimeError("BQ Storage client failed to initialize.")

        if not combined_row_filter:
            logger.warning("Empty combined row filter provided. Skipping Storage API read.")
            return []

        try:
            table_project, dataset_id, table_id = self.table_name.split('.')
        except ValueError:
            raise ValueError(f"Invalid table_name: '{self.table_name}'. Expected 'project.dataset.table'.")

        parent_project = self.project
        table_resource = f"projects/{table_project}/datasets/{dataset_id}/tables/{table_id}"

        # --- Create Read Session ---
        session_creation_start_time = time.time()
        session = None
        try:
            read_session_request = {
                "parent": f"projects/{parent_project}",
                "read_session": types.ReadSession(
                    table=table_resource,
                    data_format=types.DataFormat.ARROW,
                    read_options=types.ReadSession.TableReadOptions(
                        row_restriction=combined_row_filter,
                        selected_fields=self._bq_select_columns,
                    ),
                ),
                "max_stream_count": 0, # Let API optimize number of streams
            }
            session = self._client.create_read_session(request=read_session_request)
            session_creation_duration = time.time() - session_creation_start_time
            logger.debug(
                f"Created read session with {len(session.streams)} streams. "
                f"Duration: {session_creation_duration:.4f}s. Filter: {combined_row_filter}"
            )

            if session.streams and session.arrow_schema:
                if not self._arrow_schema: # Cache schema for this __call__
                    schema_bytes = session.arrow_schema.serialized_schema
                    self._arrow_schema = pa.ipc.read_schema(pa.py_buffer(schema_bytes))
                    logger.debug("Deserialized and cached Arrow schema for current call.")
            elif session.streams:
                logger.error("Read session created streams but no Arrow schema was returned.")
                return []
        except (BadRequest, NotFound, GoogleAPICallError) as e:
            logger.error(f"BQ Storage API error creating session. Filter: '{combined_row_filter}'. Error: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error creating read session. Filter: '{combined_row_filter}'. Error: {e}", exc_info=True)
            return []

        if not session or not session.streams:
            logger.warning(f"No streams available to read for filter: {combined_row_filter}")
            return []

        # --- Worker function for ThreadPoolExecutor ---
        # Needs to be defined here or passed self._client and self._arrow_schema
        # Making it static and passing client/schema is cleaner if possible,
        # but for simplicity of access to self._client and self._arrow_schema:
        def _read_single_stream_worker(stream_name: str) -> List[BQRowDict]:
            # This worker will use the self._client and self._arrow_schema from the handler instance
            # This assumes the handler instance (and thus its client/schema) is stable during execution
            stream_results = []
            if not self._client or not self._arrow_schema:
                 logger.error(f"Stream {stream_name}: Client or schema not available in worker.")
                 return stream_results
            try:
                reader = self._client.read_rows(stream_name)
                for response in reader:
                    stream_results.extend(self._arrow_to_dicts_static(response, self._arrow_schema))
            except EOFError:
                 logger.debug(f"Reached end of stream (EOFError) for: {stream_name}")
            except Exception as e:
                logger.error(f"Error reading stream {stream_name} in worker: {e}", exc_info=True)
            return stream_results

        # --- Parallel Stream Reading ---
        all_bq_rows_original_keys = []
        stream_read_start_time = time.time()
        num_api_streams = len(session.streams)
        # Determine max_workers for the ThreadPoolExecutor
        max_workers = num_api_streams # Default to number of streams
        if self.max_parallel_streams is not None and self.max_parallel_streams > 0:
            max_workers = min(num_api_streams, self.max_parallel_streams)
        if max_workers <= 0 : max_workers = 1 # Ensure at least one worker

        logger.debug(f"Starting parallel read for {num_api_streams} API streams using {max_workers} worker threads.")

        futures = []
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                for stream in session.streams:
                    futures.append(executor.submit(_read_single_stream_worker, stream.name))

                for future in concurrent.futures.as_completed(futures):
                    try:
                        all_bq_rows_original_keys.extend(future.result())
                    except Exception as e:
                        logger.error(f"Error processing future result from stream read: {e}", exc_info=True)
        except Exception as pool_error:
             logger.error(f"Error during ThreadPoolExecutor execution for BQ read: {pool_error}", exc_info=True)

        stream_read_duration = time.time() - stream_read_start_time
        logger.debug(
             f"Finished parallel stream read. Rows fetched: {len(all_bq_rows_original_keys)}. "
             f"Duration: {stream_read_duration:.4f}s."
        )
        return all_bq_rows_original_keys
    # --- END MODIFIED METHOD ---

    def create_row_key(self, row: BeamRow) -> Optional[tuple]:
        """Creates hashable key based ONLY on `self.fields` or `condition_value_fn` output."""
        try:
            if self.condition_value_fn:
                key_values_dict = self.condition_value_fn(row)
            elif self.fields is not None:
                row_dict = BeamRow.as_dict(row)
                key_values_dict = {}
                for field in self.fields:
                    if field not in row_dict or row_dict[field] is None:
                        logger.debug(f"Row missing key field '{field}' or None. Cannot generate key: {row}")
                        return None
                    key_values_dict[field] = row_dict[field]
            else: raise ValueError("Internal error: Neither fields nor condition_value_fn for key.")

            if key_values_dict is None: return None
            key_tuple = tuple(sorted(key_values_dict.items()))
            return key_tuple
        except Exception as e:
             logger.error(f"Unexpected error generating key for row {row}: {e}", exc_info=True)
             return None

    def __call__(self, request: Union[BeamRow, list[BeamRow]], *args, **kwargs) -> Union[Tuple[BeamRow, BeamRow], List[Tuple[BeamRow, BeamRow]]]:
        """ Handles enrichment request (single or batched), applying renaming after mapping. """
        self._arrow_schema = None # Reset schema cache for each call

        if isinstance(request, list):
            # --- Batch Processing ---
            batch_responses: List[Tuple[BeamRow, BeamRow]] = []
            requests_map: Dict[tuple, BeamRow] = {}
            single_row_filters: List[str] = []

            # 1. Prepare batch
            for req_row in request:
                condition_values = self._get_condition_values_dict(req_row)
                if condition_values is None:
                    batch_responses.append((req_row, BeamRow())); continue

                req_key = self.create_row_key(req_row)
                if req_key is None:
                     batch_responses.append((req_row, BeamRow())); continue

                if req_key not in requests_map:
                    requests_map[req_key] = req_row
                    single_filter = self._build_single_row_filter(req_row, condition_values)
                    if single_filter: single_row_filters.append(f"({single_filter})")
                    else: batch_responses.append((req_row, BeamRow())); del requests_map[req_key]
                else:
                     logger.warning(f"Duplicate key '{req_key}' in batch. Processing first: {requests_map[req_key]}")
                     batch_responses.append((req_row, BeamRow()))

            # 2. Execute Read (now uses ThreadPoolExecutor internally)
            bq_results_map: Dict[tuple, BeamRow] = {}
            if single_row_filters:
                combined_filter = " OR ".join(single_row_filters)
                logger.debug(f"Executing BQ read for {len(single_row_filters)} unique conditions.")
                bq_results_list_orig_keys = self._execute_storage_read(combined_filter)

                # 3. Map results (rename THEN key - requires self.fields uses aliases)
                for bq_row_dict_orig_keys in bq_results_list_orig_keys:
                    try:
                        renamed_bq_row_dict = self._apply_renaming(bq_row_dict_orig_keys)
                        bq_row_renamed_keys_temp = BeamRow(**renamed_bq_row_dict)
                        resp_key = self.create_row_key(bq_row_renamed_keys_temp)

                        if resp_key:
                            if resp_key not in bq_results_map:
                                bq_results_map[resp_key] = bq_row_renamed_keys_temp
                        # else: key gen failed/logged
                    except Exception as e:
                         logger.warning(f"Error processing BQ response row: {bq_row_dict_orig_keys}. Error: {e}. Cannot map back.")

            # 4. Assemble final response list
            for req_key, req_row in requests_map.items():
                 response_row = bq_results_map.get(req_key, BeamRow())
                 batch_responses.append((req_row, response_row))

            return batch_responses

        else:
            # --- Single Element Processing ---
            req_row = request
            condition_values = self._get_condition_values_dict(req_row)
            if condition_values is None: return (req_row, BeamRow())

            single_filter = self._build_single_row_filter(req_row, condition_values)
            if not single_filter: return (req_row, BeamRow())

            bq_results_orig_keys = self._execute_storage_read(single_filter) # Uses ThreadPoolExecutor

            response_row = BeamRow()
            if bq_results_orig_keys:
                 renamed_dict = self._apply_renaming(bq_results_orig_keys[0])
                 response_row = BeamRow(**renamed_dict)
                 if len(bq_results_orig_keys) > 1:
                      logger.warning(f"Single request -> {len(bq_results_orig_keys)} BQ rows. Using first (renamed). Filter:'{single_filter}'")

            return (req_row, response_row)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleans up client resources."""
        if self._client:
            logger.info("BigQueryStorageEnrichmentHandler: Releasing client resources.")
            self._client = None

    def get_cache_key(self, request: Union[BeamRow, list[BeamRow]]) -> Union[str, List[str]]:
        """Generates cache key(s) based ONLY on the primary key fields."""
        if isinstance(request, list):
            return [str(self.create_row_key(req) or "__invalid_key__") for req in request]
        else:
            return str(self.create_row_key(request) or "__invalid_key__")

    def batch_elements_kwargs(self) -> Mapping[str, Any]:
        """Returns kwargs suitable for `beam.BatchElements`."""
        return self._batching_kwargs
