from typing import Any, Iterable, Sequence, Union
from pyarrow.lib import Array, ChunkedArray, DataType, Mapping, MemoryPool, \
    Schema, Table

def array(obj: Union[Sequence, Iterable],  # also ndarray or Series
          type: DataType = None, mask: Any = None, size: int = None,
          from_pandas: bool = None, safe: bool = True,
          memory_pool: MemoryPool = None) -> Union[Array, ChunkedArray]: ...

def concat_tables(tables: Iterable[Table], promote:bool = False,
                  MemoryPool: Any = None) -> Table: ...

def enable_signal_handlers(enable: bool) -> None: ...

def table(data: Any, names: list[str] = None,
          schema: Schema = None, metadata: Union[dict, Mapping] = None,
          nthreads: int = None) -> Table: ...
