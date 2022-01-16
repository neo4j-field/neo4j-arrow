from typing import IO, List, Union
from pyarrow.lib import MemoryPool, Schema, Table

class CSVStreamingReader(): ...

class ConvertOptions():
    def __init__(self, check_utf8: bool = True,
                 column_types: Union[dict, Schema] = None,
                 null_values: List[str] = None, true_values: List[str] = None,
                 false_values: List[str] = None, decimal_point: str = '.',
                 timestamp_parsers: List[str] = None,
                 strings_can_be_null: bool = False,
                 quoted_strings_can_be_null: bool = True,
                 auto_dict_encode: bool = False,
                 auto_dict_max_cardinality: int = None,
                 include_columns: List[str] = None,
                 include_missing_columns: bool = False) -> None: ...

class ParseOptions(): ...

class ReadOptions():
    def __init__(self, use_threads: bool = True, block_size: int = None,
                 skip_rows: int = 0, skip_rows_after_names: int = 0,
                 column_names: List[str] = None,
                 autogenerate_column_names: bool = False,
                 encoding: str = 'utf8') -> None: ...

def open_csv(input_file: Union[str, IO], read_options: ReadOptions = None,
             parse_options: ParseOptions = None,
             convert_options: ConvertOptions = None) -> CSVStreamingReader: ...

def read_csv(input_file: Union[str, IO], read_options: ReadOptions = None,
             parse_options: ParseOptions = None,
             convert_options: ConvertOptions = None,
             memory_pool: MemoryPool = None) -> Table: ...
