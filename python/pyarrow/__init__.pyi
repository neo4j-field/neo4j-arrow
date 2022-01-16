from typing import Any, Dict, Iterable, List, Optional, Sequence, Union
from pyarrow.lib import Array, ChunkedArray, DataType, Mapping, \
    MemoryPool, Schema, Table

def array(obj: Union[Sequence[Any], Iterable[Any]],  # also ndarray or Series
          type: Optional[DataType] = None, mask: Any = None, size: Optional[int] = None,
          from_pandas: Optional[bool] = None, safe: bool = True,
          memory_pool: Optional[MemoryPool] = None) -> Union[Array, ChunkedArray]: ...

def concat_tables(tables: Iterable[Table], promote: Optional[bool] = False,
                  MemoryPool: Any = None) -> Table: ...

def enable_signal_handlers(enable: bool) -> None: ...

def table(data: Any, names: Optional[List[str]] = None,
          schema: Optional[Schema] = None,
          metadata: Optional[Union[Dict[Any, Any], Mapping]] = None,
          nthreads: Optional[int] = None) -> Table: ...
