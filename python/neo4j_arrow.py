import base64
import json
import struct
from collections import abc
from enum import Enum
from os import environ as env
from time import sleep, time
from typing import cast, Any, Dict, Iterable, Iterator, List, Optional, \
    Tuple, TypeVar, Union

import pyarrow as pa
from pyarrow.lib import ArrowKeyError, RecordBatch, Schema, Table
import pyarrow.flight as flight

# Known job types supported by the Java plugin.
_JOB_BULK_IMPORT = "import.bulk"
_JOB_CYPHER = "cypher.read"
_JOB_GDS_READ = "gds.read"  # TODO: rename
_JOB_GDS_WRITE_NODES = "gds.write.nodes"
_JOB_GDS_WRITE_RELS = "gds.write.relationships"
_JOB_KHOP = "khop"
_JOB_STATUS = "job.status"
_JOB_INFO_VERSION = "info.version"
_JOB_INFO_STATUS = "info.jobs"

# These defaults should stay in sync with those in the Java plugin.
# See org.neo4j.arrow.Neo4jDefaults for reference.
_ID = 'ID'
_LABELS = 'LABELS'
_START_ID = 'START_ID'
_END_ID = 'END_ID'
_TYPE = 'TYPE'

_DEFAULT_HOST = env.get('NEO4J_ARROW_HOST', 'localhost')
_DEFAULT_PORT = int(env.get('NEO4J_ARROW_PORT', '9999'))

pa.enable_signal_handlers(True)

TableLike = TypeVar('TableLike', bound=Union[RecordBatch, Table])


class JobStatus(Enum):
    """Represents the state of a server-side job."""
    UNKNOWN = "UNKNOWN"
    INITIALIZING = "INITIALIZING"
    PENDING = "PENDING"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"
    PRODUCING = "PRODUCING"

    @classmethod
    def from_str(cls, s: str) -> 'JobStatus':
        for status in JobStatus:
            if status.value == s:
                return status
        return JobStatus.UNKNOWN


def _coerce_ticket(maybe_ticket: Union[bytes, flight.Ticket]) -> flight.Ticket:
    """
    Coerce the given value into a Flight Ticket.
    :param maybe_ticket: possible Ticket
    :return: a Ticket
    """
    ticket: flight.Ticket
    if type(maybe_ticket) is flight.Ticket:
        ticket = maybe_ticket
    else:
        ticket = flight.Ticket.deserialize(cast(bytes, maybe_ticket))
    return ticket


def _coerce_table(data: Union[Dict[Any, Any],
                              TableLike,
                              flight.FlightStreamChunk]) -> Table:
    """
    Coerce a TableLike value into a PyArrow Table.
    :param data: coercible value
    :return: a PyArrow Table
    """
    if type(data) is dict:
        return Table.from_pydict(data)
    elif type(data) is RecordBatch:
        return Table.from_batches([data])
    elif type(data) is Table:
        return data
    elif type(data) is flight.FlightStreamChunk:
        # TODO: this is a pretty wasteful wrapping
        return Table.from_batches([data.data])
    # yolo
    return pa.table(data=data)


class Neo4jArrow:
    """
    A client for interacting with a remote Neo4j Arrow service. Useful for
    working with large datasets, retrieving bulk data, and async batch jobs!
    """
    # TODO: rename camelCase args to snake case

    _client: flight.FlightClient
    _location: flight.Location
    _options: flight.FlightCallOptions

    def __init__(self, user: str, password: str,
                 location: Tuple[str, int] = (_DEFAULT_HOST, _DEFAULT_PORT),
                 tls: bool = False, verify_tls: bool = True):
        """
        Create a new Neo4jArrow client. Note: the client connects
        :param user: Neo4j user to authenticate as
        :param password: password for user
        :param location: tuple of host, port (optional)
        :param tls: use TLS?
        :param verify_tls: verify server identity in x.509 certificate?
        """
        token = base64.b64encode(f'{user}:{password}'.encode('utf8'))
        self._options = flight.FlightCallOptions(headers=[
            (b'authorization', b'Basic ' + token)
        ])

        host, port = location
        if tls:
            self._location = flight.Location.for_grpc_tls(host, port)
        else:
            self._location = flight.Location.for_grpc_tcp(host, port)
        self._client = flight.FlightClient(self._location,
                                           disable_server_verification=(not verify_tls))

    def list_actions(self) -> List[flight.Action]:
        """
        List all actions available on the server.
        :return: list of all available Actions
        """
        return list(self._client.list_actions(self._options))

    def list_flights(self) -> List[flight.FlightInfo]:
        """
        List all known/existing Flights on the server.
        :return: list of Flights
        """
        return list(self._client.list_flights(None, self._options))

    def info(self) -> Dict[str, Any]:
        """
        Get info on the Neo4j Arrow server
        :return: metadata describing Neo4j Arrow server (e.g. version)
        """
        result = self._client.do_action(
            (_JOB_INFO_VERSION, b''), self._options)
        obj = json.loads(next(result).body.to_pybytes())
        if type(obj) is dict:
            return obj
        raise RuntimeError("server returned unexpected data format")

    def _submit(self, action: Union[Tuple[str, bytes],
                                    flight.Action]) -> flight.Ticket:
        """Attempt to ticket the given action/job"""
        results = self._client.do_action(action, self._options)
        return flight.Ticket.deserialize((next(results).body.to_pybytes()))

    def cypher(self, cypher: str, database: str = 'neo4j',
               params: Optional[Dict[str, Any]] = None) -> flight.Ticket:
        """Submit a Cypher job with optional parameters. Returns a ticket."""
        cypher_bytes = cypher.encode('utf8')
        db_bytes = database.encode('utf8')
        params_bytes = json.dumps(params or {}).encode('utf8')

        # Our CypherMessage format is simple:
        #   - 16 bit unsigned length of the cypher byte string
        #   - the cypher byte string payload
        #   - 16 bit unsigned length of the database byte string
        #   - the database byte string payload
        #   - 16 bit unsigned length of the param json payload
        #   - the param json byte string payload
        fmt = f"!H{len(cypher_bytes)}sH{len(db_bytes)}sH{len(params_bytes)}s"
        buffer = struct.pack(fmt,
                             len(cypher_bytes), cypher_bytes,
                             len(db_bytes), db_bytes,
                             len(params_bytes), params_bytes)
        return self._submit((_JOB_CYPHER, buffer))

    def gds_nodes(self, graph: str, database: str = 'neo4j',
                  properties: Optional[List[str]] = None,
                  filters: Optional[List[str]] = None,
                  node_id: str = '',
                  extra: Optional[Dict[str, Any]] = None) -> flight.Ticket:
        """Submit a GDS job for streaming Node properties. Returns a ticket."""
        params = {
            'db': database,
            'graph': graph,
            'type': 'node',
            'node_id': node_id,
            'properties': properties or [],
            'filters': filters or [],
        }
        params.update(extra or {})
        params_bytes = json.dumps(params).encode('utf8')
        return self._submit((_JOB_GDS_READ, params_bytes))

    def gds_write_nodes(self, graph: str, database: str = 'neo4j',
                        id_field: str = _ID,
                        labels_field: str = _LABELS) -> flight.Ticket:
        """Submit a GDS Write Job for creating Nodes and Node Properties."""
        params = {
            'db': database,
            'graph': graph,
            'idField': id_field,
            'labelsField': labels_field,
        }
        params_bytes = json.dumps(params).encode('utf8')
        return self._submit((_JOB_GDS_WRITE_NODES, params_bytes))

    def gds_write_relationships(self, graph: str, database: str = 'neo4j',
                                source_field: str = _START_ID,
                                target_field: str = _END_ID,
                                type_field: str = _TYPE) -> flight.Ticket:
        """Submit a GDS Write Job for creating Rels and Rel Properties."""
        params = {
            'db': database,
            'graph': graph,
            'source_field': source_field,
            'target_field': target_field,
            'type_field': type_field,
        }
        params_bytes = json.dumps(params).encode('utf8')
        return self._submit((_JOB_GDS_WRITE_RELS, params_bytes))

    def gds_relationships(self, graph: str, database: str = 'neo4j',
                          properties: Optional[List[str]] = None,
                          filters: Optional[List[str]] = None,
                          node_id: Optional[str] = None,
                          extra: Optional[Dict[str, Any]] = None) -> flight.Ticket:
        """
        Submit a GDS job for retrieving Relationship properties.
        :param graph: name of the GDS graph
        :param database: name of the underlying Neo4j database
        :param properties: relationship properties to retrieve
        :param filters: relationship type filter
        :param node_id: property to use as an alternative node id (default is
                        to use the internal opaque id)
        :param extra: additional custom message parameters
        :return: new Ticket
        """
        params = {
            'db': database,
            'graph': graph,
            'type': 'relationship',
            'node_id': node_id or '',
            'properties': properties or [],
            'filters': filters or [],
        }
        params.update(extra or {})
        params_bytes = json.dumps(params).encode('utf8')
        return self._submit((_JOB_GDS_READ, params_bytes))

    def khop(self, graph: str, database: str = 'neo4j',
             node_id: Optional[str] = None, rel_property: str = '_type_',
             extra: Optional[Dict[str, Any]] = None) -> pa.flight.Ticket:
        """
        **Experimental** K-Hop Job support
        :param graph: gds graph to analyze
        :param database: underlying neo4j database
        :param node_id: optional property to use as a logical node id
        :param rel_property: special relationship property used to encode
        orientation of the edge
        :param extra: any extra k/v pairs for the KhopMessage
        :return: ticket to a new KHop job
        """
        params = {
            'db': database,
            'graph': graph,
            'node_id': node_id or '',
            'type': 'khop',
            'properties': [rel_property],
            'filters': [],
        }
        params.update(extra or {})
        params_bytes = json.dumps(params).encode('utf8')
        return self._submit((_JOB_GDS_READ, params_bytes))

    def status(self, ticket: Union[bytes, flight.Ticket]) -> JobStatus:
        """
        Inspect the status a server-side Job associated with a given Ticket.
        :param ticket: Optional Ticket for filtering Jobs
        :return: list of tuples of Job ID (a string) and Job Status
        """
        body = _coerce_ticket(ticket).serialize()
        action = (_JOB_STATUS, body)

        results = self._client.do_action(action, self._options)
        status = next(results).body.to_pybytes().decode('utf8')
        return JobStatus.from_str(status)

    def wait_for_job(self, ticket: Union[bytes, pa.flight.Ticket],
                     desired: JobStatus = JobStatus.PRODUCING,
                     must_exist: bool = True,
                     timeout: Optional[int] = None) -> bool:
        """Block until a given job (specified by a ticket) reaches a status."""
        start = time()
        timeout = timeout or (1 << 25)  # well beyond someone's patience
        while time() - start < timeout:
            try:
                current = self.status(ticket)
                if current == desired:
                    return True
            except ArrowKeyError:
                if must_exist:
                    print(f'no job found for ticket {ticket!r}')
                    return False
            sleep(1)  # TODO: is 1s too fast? too slow? just right?
        return False

    def stream(self, ticket: Union[bytes, flight.Ticket],
               timeout: Optional[int] = None) -> flight.FlightStreamReader:
        """
        Read the stream associated with the given ticket.
        :param ticket: ticket to an active Read Job
        :param timeout: timeout to wait for stream to start producing
        :return: new FlightStreamReader for consuming the results
        """
        ticket = _coerce_ticket(ticket)
        self.wait_for_job(ticket, timeout=timeout)
        return self._client.do_get(ticket, self._options)

    def put(self, ticket: Union[bytes, flight.Ticket],
            data: Union[Dict[Any, Any], TableLike, Iterable[TableLike],
                        Iterator[TableLike], flight.FlightStreamReader],
            schema: Optional[Schema] = None,
            metadata: Optional[Dict[Any, Any]] = None) -> Tuple[int, int]:
        """
        Send data to the server for the corresponding Flight.

        :param ticket: a Ticket to a Flight stream
        :param data: the data to stream to the server
        :param metadata: optional metadata to append to the stream's Schema
        :return: number of rows sent, number of bytes sent
        """
        ticket = _coerce_ticket(ticket)
        if isinstance(data, flight.FlightStreamReader):
            # XXX must come first as it's also an instance of Iterable!
            return self.put_stream_from_reader(ticket, data, schema, metadata)
        elif isinstance(data, (abc.Iterable, abc.Iterator)):
            return self.put_stream_batches(ticket, data, schema, metadata)
        return self.put_stream(ticket, data, metadata)

    def put_stream(self, ticket: Union[bytes, flight.Ticket],
                   data: Union[Dict[Any, Any], TableLike],
                   metadata: Optional[Dict[Any, Any]] = None) -> Tuple[int, int]:
        """
        Write a stream to the server

        :param ticket: ticket for the associated Flight
        :param data: Table or convertible table
        :param metadata: optional metadata to include in the Table Schema
        :return: number of rows and number of bytes transmitted
        """
        table = _coerce_table(data)
        ticket = _coerce_ticket(ticket)

        if metadata:
            schema = table.schema.with_metadata(metadata)
            table = table.replace_schema_metadata(schema.metadata)

        try:
            descriptor = flight.FlightDescriptor.for_command(
                ticket.serialize())
            writer, _ = self._client.do_put(descriptor, table.schema,
                                            self._options)
            # TODO: configurable or auto-chosen chunksize
            writer.write_table(table, max_chunksize=8192)
            writer.close()
            # TODO: server should be telling us what the results were.
            #  We shouldn't assume all data was accepted.
            return table.num_rows, table.nbytes
        except Exception as e:
            print(f"put_stream error: {e}")
            return 0, 0

    def put_stream_batches(self, ticket: flight.Ticket,
                           batches: Union[Iterable[TableLike],
                                          Iterator[TableLike]],
                           schema: Optional[Schema] = None,
                           metadata: Optional[Dict[Any, Any]] = None) \
            -> Tuple[int, int]:
        """
        Write a stream using a batch producer.
        :param ticket: ticket for the Flight
        :param batches: a RecordBatchStream producing the input data
        :param schema: optional overriding Schema for the stream
        :param metadata: optional metadata to append to the Schema
        :return: number of rows and number of bytes transmitted
        """
        descriptor = flight.FlightDescriptor.for_command(ticket.serialize())
        batches = iter(batches)

        # peek and get our schema, updating with any overrides desired
        batch = next(batches)
        table = _coerce_table(batch)
        schema = schema or table.schema
        if metadata:
            schema = schema.with_metadata(metadata)

        writer, _ = self._client.do_put(descriptor, schema, self._options)
        try:
            writer.write_table(table)
            rows, nbytes = len(batch), batch.nbytes

            for batch in batches:
                writer.write_table(_coerce_table(batch))
                nbytes += batch.nbytes
                rows += len(batch)
        finally:
            writer.close()

        print(f"wrote {rows:,} rows, {round(nbytes / (1 << 20), 2):,} MiB")
        return rows, nbytes

    def put_stream_from_reader(self, ticket: flight.Ticket,
                               reader: flight.FlightStreamReader,
                               schema: Optional[Schema] = None,
                               metadata: Optional[Dict[Any, Any]] = None) \
        -> Tuple[int, int]:
        """
        Relay an existing Arrow Flight stream provided by the given reader.
        :param ticket:
        :param reader:
        :param schema:
        :param metadata:
        :return:
        """
        descriptor = flight.FlightDescriptor.for_command(ticket.serialize())
        chunk_stream = iter(reader)
        table = _coerce_table(next(chunk_stream))
        schema = schema or table.schema
        if metadata:
            schema = schema.with_metadata(metadata)

        writer, _ = self._client.do_put(descriptor, schema, self._options)
        try:
            writer.write_table(table)
            rows, nbytes = len(table), table.nbytes

            for chunk in chunk_stream:
                table = _coerce_table(chunk)
                writer.write_table(table)
                nbytes += table.nbytes
                rows += len(table)
        finally:
            writer.close()

        print(f"wrote {rows:,} rows, {round(nbytes / (1 << 20), 2):,} MiB")
        return rows, nbytes

    def bulk_import(self, database: str, id_field: str = _ID,
                    labels_field: str = _LABELS, type_field: str = _TYPE,
                    source_field: str = _START_ID,
                    target_field: str = _END_ID) -> flight.Ticket:
        params = {
            'db': database,
            'id_field': id_field,
            'labels_field': labels_field,
            'source_field': source_field,
            'target_field': target_field,
            'type_field': type_field,
        }
        params_bytes = json.dumps(params).encode('utf8')
        return self._submit((_JOB_BULK_IMPORT, params_bytes))
