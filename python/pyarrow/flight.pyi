from typing import Any, Dict, Iterable, Iterator, Generator, List, Optional, \
    Tuple, Union
from pyarrow.lib import Buffer, RecordBatch, Schema, Table

class Action: ...

class FlightCallOptions:
    def __init__(self, timeout: Optional[float] = None, write_options: Any = None,
                 headers: Optional[Union[List[Tuple[str, str]],
                                         List[Tuple[bytes, bytes]]]] = None) \
            -> None: ...

class FlightDescriptor:
    @classmethod
    def deserialize(cls, serialized: bytes) -> FlightDescriptor: ...
    @classmethod
    def for_command(cls, command: bytes) -> FlightDescriptor: ...
    @classmethod
    def for_path(cls, path: str) -> FlightDescriptor: ...
    command: Any
    descriptor_type: Any
    path: Any

class FlightInfo:
    schema: Schema
    descriptor: FlightDescriptor
    total_bytes: int
    total_records: int

class FlightMetadataReader: ...

class FlightStreamChunk:
    data: RecordBatch
    app_metadata: Union[None, Any]

class FlightStreamReader(Iterable[FlightStreamChunk]):
    def __iter__(self) -> Iterator[FlightStreamChunk]: ...
    def read_chunk(self) -> FlightStreamChunk: ...
    schema: Schema

class _CRecordBatchWriter:
    def close(self) -> None: ...
    def write(self, table_or_batch: Union[RecordBatch, Table]) -> None: ...
    def write_table(self, table: Table,
                    max_chunksize: Optional[int] = None,
                    kwargs: Optional[Dict[str, Any]] = None) -> None: ...

class MetadataRecordBatchWriter(_CRecordBatchWriter):
    def write_batch(self, batch: RecordBatch) -> None: ...
    def write_metadata(self, buf: Any) -> None: ...
    def write_with_metdata(self, batch: RecordBatch, buf: Any) -> None: ...

class FlightStreamWriter(MetadataRecordBatchWriter):
    def done_writing(self) -> None: ...


class Location:
    @classmethod
    def for_grpc_tcp(cls, host: str, port: int) -> Location: ...
    @classmethod
    def for_grpc_tls(cls, host: str, port: int) -> Location: ...
    @classmethod
    def for_grpc_unix(cls, path: str) -> Location: ...

class Result:
    def __init__(self, buf: Union[Buffer, bytes]) -> None: ...
    body: Buffer

class Ticket:
    def serialize(self) -> bytes: ...
    @classmethod
    def deserialize(cls, serialized: bytes) -> Ticket: ...

class FlightClient:
    def __init__(self, location: Location,
                 tls_root_certs: Optional[bytes] = None,
                 cert_chain: Optional[bytes] = None,
                 private_key: Optional[bytes] = None,
                 override_hostname: Optional[str] = None,
                 middleware: Optional[List[Any]] = None,
                 write_size_limit_bytes: Optional[int] = None,
                 disable_server_verification: bool = False,
                 generic_options: Optional[List[Any]] = None) -> None: ...

    # TODO: do_action() supports other types for 'action' arg
    def do_action(self, action: Union[Tuple[str, bytes], Action],
                  options: Optional[FlightCallOptions] = None) \
            -> Iterator[Result]: ...

    def do_get(self, ticket: Ticket,
                  options: Optional[FlightCallOptions] = None) \
            -> FlightStreamReader: ...

    def do_put(self, descriptor: FlightDescriptor, schema: Schema,
                  options:  Optional[FlightCallOptions] = None) \
            -> Tuple[FlightStreamWriter, FlightStreamReader]: ...

    def get_flight_info(self, descriptor: FlightDescriptor,
                  options: Optional[FlightCallOptions] = None) -> FlightInfo: ...

    def list_actions(self, options: Optional[FlightCallOptions] = None) \
            -> List[Action]: ...

    def list_flights(self, criteria: Optional[bytes] = None,
                     options: Optional[FlightCallOptions] = None) \
            -> Generator[FlightInfo, None, None]: ...

    def wait_for_available(self, timeout: int = 5) -> None: ...
