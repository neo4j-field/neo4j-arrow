from typing import Any, AnyStr, Dict, IO, List, Optional, Union
from pyarrow.lib import MemoryPool, Schema, Table

class CSVStreamingReader(): ...

class ConvertOptions():
    def __init__(self, check_utf8: bool = True,
                 column_types: Optional[Union[Dict[Any, Any], Schema]] = None,
                 null_values: Optional[List[str]] = None,
                 true_values: Optional[List[str]] = None,
                 false_values:Optional[List[str]] = None,
                 decimal_point: str = '.',
                 timestamp_parsers: Optional[List[str]] = None,
                 strings_can_be_null: bool = False,
                 quoted_strings_can_be_null: bool = True,
                 auto_dict_encode: bool = False,
                 auto_dict_max_cardinality: Optional[int] = None,
                 include_columns: Optional[List[str]] = None,
                 include_missing_columns: bool = False) -> None: ...

class ParseOptions(): ...

class ReadOptions():
    def __init__(self, use_threads: bool = True,
                 block_size: Optional[int] = None,
                 skip_rows: int = 0, skip_rows_after_names: int = 0,
                 column_names: Optional[List[str]] = None,
                 autogenerate_column_names: bool = False,
                 encoding: str = 'utf8') -> None: ...

def open_csv(input_file: Union[str, IO[AnyStr]],
             read_options: Optional[ReadOptions] = None,
             parse_options: Optional[ParseOptions] = None,
             convert_options: Optional[ConvertOptions] = None) \
        -> CSVStreamingReader: ...

def read_csv(input_file: Union[str, IO[AnyStr]],
             read_options: Optional[ReadOptions] = None,
             parse_options: Optional[ParseOptions] = None,
             convert_options: Optional[ConvertOptions] = None,
             memory_pool: Optional[MemoryPool] = None) -> Table: ...
