import pyarrow as pa
import pyarrow.flight as flight
import base64
import json
import struct
import time as _time
from enum import Enum
from os import environ as env

_JOB_CYPHER = "cypherRead"
_JOB_GDS = "gdsNodeProperties"      # TODO: rename
_JOB_GDS_WRITE = "gds.write.nodes"
_JOB_GDS_WRITE_RELS = "gds.write.relationships"
_JOB_STATUS = "jobStatus"

_DEFAULT_HOST = env.get('NEO4J_ARROW_HOST', 'localhost')
_DEFAULT_PORT = int(env.get('NEO4J_ARROW_PORT', '9999'))

pa.enable_signal_handlers(True)

class JobStatus(Enum):
    INITIALIZING = "INITIALIZING"
    PENDING = "PENDING"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"
    PRODUCING = "PRODUCING"

class Neo4jArrow:
    """
    A client for interacting with a remote Neo4j Arrow service. Useful for
    working with large datasets, retrieving bulk data, and async batch jobs!
    """

    def __init__(self, username, password, location=()):
        token = base64.b64encode(f'{username}:{password}'.encode('utf8'))
        self._options = flight.FlightCallOptions(headers=[
            (b'authorization', b'Basic ' + token)
        ])

        real_location = [_DEFAULT_HOST, _DEFAULT_PORT]
        if len(location) > 0:
            real_location[0] = location[0]
        if len(location) > 1:
            real_location[1] = location[1]
        self._client = flight.FlightClient(tuple(real_location))

    def list_actions(self):
        """List all actions available on the server."""
        return list(self._client.list_actions(options=self._options))

    def list_flights(self):
        """List all known flights. (No filtering support yet.)"""
        return list(self._client.list_flights(options=self._options))

    def cypher(self, cypher, database='neo4j', params={}):
        """Submit a Cypher job with optional parameters. Returns a ticket."""
        cypher_bytes = cypher.encode('utf8')
        db_bytes = database.encode('utf8')
        params_bytes = json.dumps(params).encode('utf8')

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
        action = (_JOB_CYPHER, buffer)
        results = self._client.do_action(action, options=self._options)
        return pa.flight.Ticket.deserialize((next(results).body.to_pybytes()))

    def gds_nodes(self, graph, properties=[], database='neo4j', filters=[]):
        """Submit a GDS job for streaming Node properties. Returns a ticket."""
        params = {
            'db': database,
            'graph': graph,
            'type': 'node',
            'properties': properties,
            'filters': filters,
        }
        params_bytes = json.dumps(params).encode('utf8')
        action = (_JOB_GDS, params_bytes)
        results = self._client.do_action(action, options=self._options)
        return pa.flight.Ticket.deserialize((next(results).body.to_pybytes()))

    def gds_write_nodes(self, graph, database='neo4j', idField='id', labelsField='labels'):
        """Submit a GDS Write Job for creating Nodes and Node Properties."""
        params = {
             'db': database,
             'graph': graph,
             'idField': idField,
             'labelsField': labelsField,
         }
        params_bytes = json.dumps(params).encode('utf8')
        action = (_JOB_GDS_WRITE, params_bytes)
        results = self._client.do_action(action, options=self._options)
        return pa.flight.Ticket.deserialize((next(results).body.to_pybytes()))

    def gds_relationships(self, graph, properties=[], database='neo4j', filters=[]):
        """
        Submit a GDS job for streaming Relationship properties.
        Returns a ticket.
        """
        params = {
            'db': database,
            'graph': graph,
            'type': 'relationships',
            'properties': properties,
            'filters': filters,
        }
        params_bytes = json.dumps(params).encode('utf8')
        action = (_JOB_GDS, params_bytes)
        results = self._client.do_action(action, options=self._options)
        return pa.flight.Ticket.deserialize((next(results).body.to_pybytes()))

    def status(self, ticket):
        """Check job status for a ticket."""
        if type(ticket) == pa.flight.Ticket:
            buffer = ticket.serialize()
        else:
            buffer = ticket
        action = (_JOB_STATUS, buffer)
        results = self._client.do_action(action, options=self._options)
        return JobStatus(next(results).body.to_pybytes().decode('utf8'))
    
    def wait_for_job(self, ticket, status=JobStatus.PRODUCING, timeout=60):
        """Block until a given job (specified by a ticket) reaches a status."""
        start = _time.time()
        while _time.time() - start < timeout:
            try:
                if self.status(ticket) == status:
                    return True
            except Exception as e:
                print(f"no job (yet?): {e}")
            _time.sleep(1)
        return False
    
    def stream(self, ticket, timeout=60):
        """Read the stream associated with the given ticket."""
        self.wait_for_job(ticket, timeout=timeout)
        return self._client.do_get(ticket, options=self._options)

    def put_stream(self, ticket, data):
        """Write a stream to the server"""
        # no need to wait for writes
        table = pa.table(data=data)
        descriptor = pa.flight.FlightDescriptor.for_command(ticket.serialize())
        writer, _ = self._client.do_put(descriptor, table.schema, options=self._options)
        writer.write_table(table)
        writer.close()