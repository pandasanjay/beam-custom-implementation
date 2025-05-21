# -*- coding: utf-8 -*-
# Copyright 2024 Google LLC
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
Apache Beam PTransform for enriching elements with BigQuery data using the
Storage Read API. This version groups elements by key, batches the key-groups,
makes one API call per batch (OR filter), and reads the resulting streams
in parallel using Beam's runner. Includes custom BQ row selection callback
and configurable enrichment modes.
"""
import logging
import re
from collections.abc import Callable, Mapping
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple, Union

import apache_beam as beam
# Using Any for input/output type hints for flexibility with schema-aware PCollections
from apache_beam.pvalue import PCollection
# Import Row explicitly for type checking where needed (e.g., function signatures)
from apache_beam.pvalue import Row as BeamRow
from apache_beam.transforms.util import BatchElements # Import BatchElements
from google.api_core.exceptions import BadRequest, NotFound, GoogleAPICallError
from google.cloud.bigquery_storage import BigQueryReadClient, types
from google.cloud import bigquery_storage
import pyarrow as pa
import pyarrow.ipc

# --- Configure Logging ---
logger = logging.getLogger(__name__)

# --- Type Hints ---
Key = Any
# InputElement can be beam.Row or a schema-aware row type or dict
InputElement = Any
BQRowDict = Dict[str, Any]
# Functions still expect beam.Row signature for clarity, use beam.Row.as_dict inside if needed
ConditionValueFn = Callable[[BeamRow], Dict[str, Any]]
# Updated RowRestrictionTemplateFn signature
RowRestrictionTemplateFn = Callable[[Dict[str, Any], BeamRow], str] # Expects condition dict and row
# Type for a batch of key-groups: List[Tuple[Key, List[Dict]]]
KeyGroupBatch = List[Tuple[Key, List[Dict]]]
# --- Added Type Hint for BQ Row Selection Callback ---
SelectBQRowFn = Callable[[List[BQRowDict]], BQRowDict]
EnrichmentMode = str # Type hint for enrichment mode


# --- Constants ---
ALIAS_REGEX = re.compile(r"^(.*?)\s+as\s+(.*)$", re.IGNORECASE)
ENRICHMENT_MODE_MERGE_NEW = "merge_new"
ENRICHMENT_MODE_ADD_NESTED = "add_nested"

# =============================================================================
# Helper Functions & Validation (Should be defined globally or imported)
# =============================================================================

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

def _apply_renaming(bq_row_dict: BQRowDict, rename_map: Dict[str, str]) -> BQRowDict:
    """Applies the renaming map."""
    if not rename_map: return bq_row_dict
    return {rename_map.get(k, k): v for k, v in bq_row_dict.items()}

def _arrow_to_dicts(response: types.ReadRowsResponse, arrow_schema: Optional[pa.Schema]) -> Iterator[BQRowDict]:
    """ Converts Arrow RecordBatch in response to Python dicts. """
    if response.arrow_record_batch:
        if not arrow_schema: logger.error("Schema missing for Arrow conversion."); return
        try:
            serialized_batch = response.arrow_record_batch.serialized_record_batch
            record_batch = pa.ipc.read_record_batch(pa.py_buffer(serialized_batch), arrow_schema)
            arrow_table = pa.Table.from_batches([record_batch])
            yield from arrow_table.to_pylist()
        except Exception as e:
            logger.error(f"Error converting Arrow batch: {e}", exc_info=True)

# --- Updated Helper Function for Keying ---
def _extract_key_and_dict(element: InputElement, config: Dict[str, Any]) -> Tuple[Optional[Key], Optional[Dict]]:
    """
    Safely converts input element to dict and extracts key.
    Returns (None, None) on failure.
    NOTE: Uses _asdict() based on user request, assumes input is beam.Row.
    """
    row_dict = None
    key_field = config['join_key_field']
    try:
        # --- Reverted Change based on user request ---
        # Assumes element is beam.Row or compatible object with _asdict()
        # This might raise AttributeError if element is dict or other type.
        row_dict = element._asdict()
        # --- End Reverted Change ---

        key = row_dict.get(key_field)
        if key is None:
             logger.debug(f"Key field '{key_field}' is missing or None in element: {row_dict}")
             return (None, None) # Filter out immediately

        return (key, row_dict)

    except AttributeError:
         logger.error(f"Failed to call _asdict() on element. Type: {type(element)}. Element: {element}. Ensure input PCollection contains beam.Row objects.")
         return (None, None)
    except Exception as e:
        logger.error(f"Failed to extract key '{key_field}' or convert row: {element}. Error: {e}", exc_info=True)
        return (None, None)
# --- End Helper Function ---

# =============================================================================
# Core DoFns for the PTransform Stages
# =============================================================================

class _BatchCreateStreamsDoFn(beam.DoFn):
    """ Creates ONE BQ Read Session for a batch of key-groups. """
    def __init__(self, config: Dict[str, Any]):
        self._config = config
        self._client: Optional[BigQueryReadClient] = None
        self._bq_select_columns: List[str] = config['bq_select_columns']
        self._rename_map: Dict[str, str] = config['rename_map']
        self.fields = config['fields']
        self.condition_value_fn = config['condition_value_fn']
        self.additional_condition_fields = config['additional_condition_fields']
        self.row_restriction_template = config['row_restriction_template']
        self.row_restriction_template_fn = config['row_restriction_template_fn']
        self.table_name = config['table_name']
        self.project = config['project']

    def setup(self):
        self._client = BigQueryReadClient()
        logger.info("_BatchCreateStreamsDoFn: Client created.")

    def _get_condition_values_dict(self, req_dict: Dict) -> Optional[Dict[str, Any]]:
        """Gets values dictionary for filter formatting from element dict."""
        temp_row = None
        try:
            if self.condition_value_fn:
                temp_row = BeamRow(**req_dict)
                values_dict = self.condition_value_fn(temp_row)
                if values_dict is None or any(v is None for v in values_dict.values()): return None
                return values_dict
            elif self.fields is not None:
                values_dict = {}
                all_req_fields = (self.fields or []) + self.additional_condition_fields
                for field in all_req_fields:
                    # Check only required if NOT using template function
                    if not self.row_restriction_template_fn:
                         if field not in req_dict or req_dict[field] is None:
                             logger.warning(
                                 f"Group representative element missing field '{field}' or value is None (needed for filter). Skipping request: {req_dict}"
                             )
                             return None
                    # logger.debug(f"Request dictionary: {req_dict}") # Reduce verbosity
                    values_dict[field] = req_dict.get(field) # Use get for safety with template_fn
                return values_dict
            else: raise ValueError("Internal error: Neither fields nor condition_value_fn.")
        except Exception as e:
             logger.error(f"Error getting condition values for dict {req_dict}: {e}", exc_info=True)
             return None

    def _build_single_row_filter(self, req_dict: Dict, condition_values_dict: Dict[str, Any]) -> str:
        """Builds the filter string part for a single row."""
        temp_row = None
        try:
            if self.row_restriction_template_fn:
                temp_row = BeamRow(**req_dict)
                # Pass both condition values and the row to the function
                template = self.row_restriction_template_fn(condition_values_dict, temp_row)
                if not isinstance(template, str): raise TypeError("row_restriction_template_fn must return str")
                # Template is returned directly, assumed already formatted
                return template
            elif self.row_restriction_template:
                template = self.row_restriction_template
                # Format the fixed template
                return template.format(**condition_values_dict)
            else: raise ValueError("Internal Error: No template available.")
        except KeyError as e:
             raise ValueError(
                 f"Placeholder {{{e}}} in row restriction template not found "
                 f"in condition values dictionary keys: {condition_values_dict.keys()}"
            )
        except Exception as e:
            logger.error(f"Error building filter for element dict {req_dict} with values {condition_values_dict}: {e}", exc_info=True)
            return ""

    def process(self, batch_of_key_groups: KeyGroupBatch) -> Iterator[Tuple[str, bytes]]:
        if not batch_of_key_groups: return
        if not self._client: self.setup();
        if not self._client: raise RuntimeError("BQ Client failed in _BatchCreateStreamsDoFn")

        single_row_filters: List[str] = []
        processed_keys = set()
        for key, elements_list in batch_of_key_groups:
            if key in processed_keys or not elements_list: continue
            rep_dict = elements_list[0]
            cond_vals = self._get_condition_values_dict(rep_dict)
            if cond_vals:
                s_filter = self._build_single_row_filter(rep_dict, cond_vals)
                if s_filter: single_row_filters.append(f"({s_filter})"); processed_keys.add(key)
                else: logger.warning(f"Filter generation failed for key {key}.")
            else: logger.warning(f"Could not get condition values for key {key}.")

        if not single_row_filters: logger.warning("No valid filters for batch."); return
        combined_filter = " OR ".join(single_row_filters)
        logger.debug(f"BatchCreateStreams: Combined filter for {len(processed_keys)} keys: {combined_filter}")

        session = None; serialized_schema = b""
        try:
            t_proj, d_id, t_id = self.table_name.split('.')
            p_proj = self.project; t_res = f"projects/{t_proj}/datasets/{d_id}/tables/{t_id}"
            req = {"parent": f"projects/{p_proj}", "read_session": types.ReadSession(
                    table=t_res, data_format=types.DataFormat.ARROW,
                    read_options=types.ReadSession.TableReadOptions(
                        row_restriction=combined_filter, selected_fields=self._bq_select_columns),
                ), "max_stream_count": 100 } # Increased max_stream_count example
            session = self._client.create_read_session(request=req)
            logger.debug(f"BatchCreateStreams: Session created with {len(session.streams)} streams.")
            if session.streams and session.arrow_schema: serialized_schema = session.arrow_schema.serialized_schema
            elif session.streams: logger.error("BatchCreateStreams: Session has streams but no schema."); return
        except Exception as e: logger.error(f"BatchCreateStreams: Error creating session: {e}", exc_info=True); return

        if session and session.streams:
             for stream in session.streams: yield (stream.name, serialized_schema)
        else: logger.debug("BatchCreateStreams: No streams created or session failed.")


class _BatchReadStreamAndExtractKeyDoFn(beam.DoFn):
    """ Reads rows from a BQ Storage stream, extracts key, yields (key, bq_row_dict). """
    def __init__(self, config: Dict[str, Any]):
         self._config = config
         self._client: Optional[BigQueryReadClient] = None
         self._join_key_field = config['join_key_field']
         reverse_rename_map = {v: k for k, v in config['rename_map'].items()}
         self._bq_join_key_column = reverse_rename_map.get(self._join_key_field, self._join_key_field)

    def setup(self):
        self._client = BigQueryReadClient()
        logger.info("_BatchReadStreamAndExtractKeyDoFn: Client created.")

    def process(self, element: Tuple[str, bytes]) -> Iterator[Tuple[Key, BQRowDict]]:
        stream_name, serialized_schema = element
        if not self._client: self.setup();
        if not self._client: raise RuntimeError("BQ Client failed in _ReadStreamDoFn")
        if not serialized_schema: logger.error(f"Stream {stream_name}: No schema."); return

        arrow_schema = None
        try: arrow_schema = pa.ipc.read_schema(pa.py_buffer(serialized_schema))
        except Exception as e: logger.error(f"Stream {stream_name}: Failed schema deserialize: {e}"); return

        logger.debug(f"Reading stream {stream_name}")
        try:
            reader = self._client.read_rows(stream_name)
            for response in reader:
                for bq_row_dict in _arrow_to_dicts(response, arrow_schema):
                    key = bq_row_dict.get(self._bq_join_key_column)
                    if key is not None: yield (key, bq_row_dict)
                    else: logger.warning(f"Stream {stream_name}: Cannot extract key '{self._bq_join_key_column}' from row: {bq_row_dict}")
        except EOFError: logger.debug(f"Stream {stream_name}: EOF.")
        except Exception as e: logger.error(f"Stream {stream_name}: Error reading: {e}", exc_info=True)


class _EnrichBatchedDoFn(beam.DoFn):
    """ Performs final enrichment after CoGroupByKey, allows custom BQ row selection and merge modes. """
    def __init__(self, config: Dict[str, Any]):
        self._config = config
        self._rename_map = config['rename_map']
        self._enrichment_mode: EnrichmentMode = config.get('enrichment_mode', ENRICHMENT_MODE_MERGE_NEW)
        self._nested_bq_key: Optional[str] = config.get('nested_bq_key')
        # --- Get the BQ Row Selection callback function ---
        self._select_bq_row_fn: Optional[SelectBQRowFn] = config.get('select_bq_row_fn')
        # --- End Get callback ---

    def process(self, element: Tuple[Key, Dict[str, Iterable]]) -> Iterator[BeamRow]:
        key, joined_data = element
        input_elements_groups = joined_data.get('inputs', [])
        bq_results_groups = joined_data.get('bq_data', [])
        input_elements = [item for sublist in input_elements_groups for item in sublist]
        bq_results = [item for sublist in bq_results_groups for item in sublist] # ORIGINAL BQ keys

        if not input_elements: logger.warning(f"Key {key}: No input elements found."); return

        selected_bq_row_orig_keys: Optional[BQRowDict] = None # Holds the single BQ row dict (with original keys) chosen for enrichment
        enrichment_status = 'NO_BQ_DATA'
        bq_data_to_merge: Optional[BQRowDict] = None # Holds RENAMED data for merging

        if bq_results:
            # --- Custom Selection Logic ---
            if self._select_bq_row_fn:
                try:
                    # Pass the list of dicts with original keys to the callback
                    selected_bq_row_orig_keys = self._select_bq_row_fn(bq_results)
                    if not isinstance(selected_bq_row_orig_keys, dict):
                         logger.warning(f"Key {key}: select_bq_row_fn did not return a dict. Falling back to first result.")
                         selected_bq_row_orig_keys = bq_results[0] if bq_results else None # Fallback, ensure bq_results not empty
                except Exception as e:
                    logger.error(f"Key {key}: Error in select_bq_row_fn: {e}. Falling back to first result.", exc_info=True)
                    selected_bq_row_orig_keys = bq_results[0] if bq_results else None # Fallback
            else:
                # Default: pick the first result
                selected_bq_row_orig_keys = bq_results[0]
            # --- End Custom Selection ---

            if selected_bq_row_orig_keys:
                 # Apply renaming AFTER selection
                 bq_data_to_merge = _apply_renaming(selected_bq_row_orig_keys, self._rename_map) # RENAMED dict
                 enrichment_status = 'SUCCESS'
                 # Log if multiple rows existed and default selection was used
                 if len(bq_results) > 1 and not self._select_bq_row_fn :
                      logger.info(f"Key {key}: Multiple BQ rows found ({len(bq_results)}). Using first for enrichment as no select_bq_row_fn provided.")
            else:
                 # Case where callback returned None or something invalid, and fallback also failed (e.g., empty list initially)
                 logger.warning(f"Key {key}: Could not select a BQ row for enrichment (selected_bq_row_orig_keys is None).")
                 enrichment_status = 'BQ_SELECTION_FAILED'
        else:
            logger.debug(f"Key {key}: No BQ data found.")

        # Enrich each original input element
        for original_element_dict in input_elements:
             if not isinstance(original_element_dict, dict):
                  logger.error(f"Key {key}: Expected dict, got {type(original_element_dict)}. Skipping.")
                  continue

             enriched_element_dict = original_element_dict.copy()
             enriched_element_dict['__enrichment_status'] = enrichment_status

             # --- Apply selected enrichment mode ---
             if bq_data_to_merge: # Check if a BQ row was successfully selected and renamed
                 if self._enrichment_mode == ENRICHMENT_MODE_ADD_NESTED:
                     if self._nested_bq_key:
                         if self._nested_bq_key in enriched_element_dict:
                              logger.warning(f"Key {key}: Nested key '{self._nested_bq_key}' collision. Overwriting.")
                         enriched_element_dict[self._nested_bq_key] = bq_data_to_merge # Add whole dict
                     else:
                          logger.error(f"Key {key}: Mode is '{ENRICHMENT_MODE_ADD_NESTED}' but nested_bq_key missing.")
                          enriched_element_dict['__enrichment_status'] += '_NESTED_KEY_MISSING'
                 elif self._enrichment_mode == ENRICHMENT_MODE_MERGE_NEW:
                     for bq_key, bq_value in bq_data_to_merge.items():
                          if bq_key not in enriched_element_dict:
                              enriched_element_dict[bq_key] = bq_value
                          else: logger.debug(f"Key {key}: Field '{bq_key}' collision (merge_new). Keeping original.")
                 else:
                      logger.error(f"Key {key}: Unknown enrichment_mode '{self._enrichment_mode}'.")
                      enriched_element_dict['__enrichment_status'] += '_UNKNOWN_MODE'
             # --- End enrichment mode logic ---

             yield BeamRow(**enriched_element_dict)


# =============================================================================
# Main PTransform Definition
# =============================================================================

class BatchedParallelBigQueryEnrichmentTransform(beam.PTransform):
    """
    A PTransform that enriches elements using BigQuery Storage API.
    Groups inputs by key, batches key-groups, makes one API call per batch,
    reads streams in parallel via Beam runner, joins results, and allows
    custom BQ row selection and configurable enrichment merge modes.
    """
    def __init__(
        self,
        project: str,
        table_name: str,
        join_key_field: str,
        *,
        row_restriction_template: Optional[str] = None,
        row_restriction_template_fn: Optional[RowRestrictionTemplateFn] = None,
        fields: Optional[list[str]] = None,
        additional_condition_fields: Optional[list[str]] = None,
        column_names: Optional[list[str]] = None,
        condition_value_fn: Optional[ConditionValueFn] = None,
        # --- Added Callback Parameter ---
        select_bq_row_fn: Optional[SelectBQRowFn] = None,
        # --- End Added Callback ---
        enrichment_mode: EnrichmentMode = ENRICHMENT_MODE_MERGE_NEW,
        nested_bq_key: Optional[str] = None,
        min_batch_size: int = 100,
        max_batch_size: int = 1000,
        # max_buffering_duration_secs removed
    ):
        """ Initializes the transform. See DoFn docstrings for arg details. """
        if not join_key_field: raise ValueError("`join_key_field` must be provided.")
        _validate_bigquery_metadata(
            project, table_name, row_restriction_template, row_restriction_template_fn,
            fields, condition_value_fn, additional_condition_fields
        )
        valid_modes = [ENRICHMENT_MODE_MERGE_NEW, ENRICHMENT_MODE_ADD_NESTED]
        if enrichment_mode not in valid_modes:
            raise ValueError(f"Invalid enrichment_mode '{enrichment_mode}'. Must be one of {valid_modes}")
        if enrichment_mode == ENRICHMENT_MODE_ADD_NESTED and not nested_bq_key:
            raise ValueError(f"`nested_bq_key` must be provided when enrichment_mode is '{ENRICHMENT_MODE_ADD_NESTED}'")

        # --- Parse columns and store config ---
        rename_map: Dict[str, str] = {}
        bq_columns_to_select_set: Set[str] = set()
        select_all_columns = False
        if column_names:
            for name_or_alias in column_names:
                match = ALIAS_REGEX.match(name_or_alias)
                if match:
                    original_col, alias_col = match.group(1).strip(), match.group(2).strip()
                    if not original_col or not alias_col: raise ValueError(f"Invalid alias: '{name_or_alias}'")
                    bq_columns_to_select_set.add(original_col)
                    rename_map[original_col] = alias_col
                else:
                    col = name_or_alias.strip();
                    if not col: raise ValueError("Empty column name.")
                    if col == '*': select_all_columns = True; break
                    bq_columns_to_select_set.add(col)
        else: select_all_columns = True

        key_gen_fields_set = set(fields or [])
        if select_all_columns: bq_select_columns = ["*"]
        else:
             fields_to_ensure = set()
             if fields:
                 reverse_map = {v: k for k, v in rename_map.items()}
                 fields_to_ensure.update(reverse_map.get(f, f) for f in fields)
             join_key_original = {v: k for k, v in rename_map.items()}.get(join_key_field, join_key_field)
             fields_to_ensure.add(join_key_original)
             final_select_set = bq_columns_to_select_set.union(fields_to_ensure)
             bq_select_columns = sorted(list(final_select_set))
             if not bq_select_columns: raise ValueError("No columns to select.")
        # --- End parsing ---

        self._config = {
            "project": project, "table_name": table_name, "join_key_field": join_key_field,
            "row_restriction_template": row_restriction_template,
            "row_restriction_template_fn": row_restriction_template_fn,
            "fields": fields,
            "additional_condition_fields": additional_condition_fields or [],
            "condition_value_fn": condition_value_fn,
            "bq_select_columns": bq_select_columns, "rename_map": rename_map,
            # --- Store select_bq_row_fn in config ---
            "select_bq_row_fn": select_bq_row_fn,
            # --- End store ---
            "enrichment_mode": enrichment_mode,
            "nested_bq_key": nested_bq_key,
        }
        self._batch_params = { # Params for BatchElements on key-groups
            "min_batch_size": min_batch_size,
            "max_batch_size": max_batch_size,
            # "max_buffering_duration_secs": max_buffering_duration_secs, # Removed
        }

    def expand(self, pcoll: PCollection) -> PCollection[BeamRow]:
        """ Applies the batched, parallel enrichment logic. """

        keyed_input = (
            pcoll
            # Use _extract_key_and_dict for robust conversion and key extraction
            | f"KeyInput_{self._config['join_key_field']}" >> beam.Map(_extract_key_and_dict, self._config)
            | f"FilterMissingInputKeys_{self._config['join_key_field']}" >> beam.Filter(lambda kv: kv[0] is not None)
            # Output: (Key, Dict)
        )
        grouped_input = ( keyed_input | f"GroupInputByKey_{self._config['join_key_field']}" >> beam.GroupByKey() )
        batched_key_groups = ( grouped_input | "MapGroupToList" >> beam.MapTuple(lambda key, rows: (key, list(rows)))
                                             | "BatchKeyGroups" >> BatchElements(**self._batch_params) )
        stream_info = ( batched_key_groups | "BatchCreateStreams" >> beam.ParDo(_BatchCreateStreamsDoFn(self._config)) )
        keyed_bq_results = ( stream_info | "MapToReadStreamInput" >> beam.Map(lambda x: (x[0], x[1])) # Only need stream_name, schema_bytes
                                         | "BatchReadStreamAndKey" >> beam.ParDo(_BatchReadStreamAndExtractKeyDoFn(self._config)) )
        grouped_bq_results = ( keyed_bq_results | f"GroupBQResultsByKey_{self._config['join_key_field']}" >> beam.GroupByKey() )
        grouped_input_dicts = grouped_input | "MapInputGroupsToLists" >> beam.MapTuple(lambda key, rows: (key, list(rows)))
        joined_data = ( {'inputs': grouped_input_dicts, 'bq_data': grouped_bq_results}
                        | f"JoinInputsWithBQData_{self._config['join_key_field']}" >> beam.CoGroupByKey() )
        enriched_output = ( joined_data | "EnrichBatchedElements" >> beam.ParDo(_EnrichBatchedDoFn(self._config)) )

        return enriched_output

# =============================================================================
# Example Usage (Conceptual)
# =============================================================================
# def select_latest_bq_row(bq_rows: List[BQRowDict]) -> BQRowDict:
#     """Callback example: Selects row with the latest timestamp."""
#     if not bq_rows: return {} # Should not happen if called only when bq_results exist
#     # Assumes a 'timestamp_field' exists in the BQ data
#     return max(bq_rows, key=lambda r: r.get('timestamp_field', '')) # Handle missing field gracefully
#
# def run_pipeline(pipeline_options):
#     with beam.Pipeline(options=pipeline_options) as pipeline:
#         input_dicts = [...]
#         input_data = pipeline | "Create" >> beam.Create(input_dicts)
#
#         # Example 1: Merge new fields
#         enriched_merge = input_data | "EnrichMerge" >> BatchedParallelBigQueryEnrichmentTransform(
#             project="your-proj", table_name="your-proj.dataset.table", join_key_field="user_id",
#             fields=["user_id"], row_restriction_template="id = {user_id}",
#             column_names=["id as user_id", "email", "status"],
#             enrichment_mode='merge_new'
#         )
#
#         # Example 2: Add BQ data nested under 'bq_info' key, using custom selection
#         enriched_nested = input_data | "EnrichNested" >> BatchedParallelBigQueryEnrichmentTransform(
#             project="your-proj", table_name="your-proj.dataset.table", join_key_field="user_id",
#             fields=["user_id"], row_restriction_template="id = {user_id}",
#             column_names=["id as user_id", "email", "status", "timestamp_field"],
#             select_bq_row_fn=select_latest_bq_row, # Pass the callback function
#             enrichment_mode='add_nested',
#             nested_bq_key='bq_info' # Specify key for nesting
#         )
#
#         enriched_merge | "LogMerge" >> beam.Map(print)
#         enriched_nested | "LogNested" >> beam.Map(print)
