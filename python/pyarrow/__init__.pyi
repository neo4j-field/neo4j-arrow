from typing import Any, Union
from pyarrow.lib import Mapping, Schema, Table

def enable_signal_handlers(enable: bool) -> None: ...

def table(data: Any, names: list[str] = None,
          schema: Schema = None, metadata: Union[dict, Mapping] = None,
          nthreads: int = None) -> Table: ...
