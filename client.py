import pyarrow as pa
import pyarrow.flight as flight
import base64
import cmd
import json
import struct
import sys
from time import sleep, time

pa.enable_signal_handlers(True)

def build_location(inputs=sys.argv[1:]):
    it = iter(inputs)
    host = next(it, "localhost")
    port = int(next(it, 9999))
    return (host, port)


def wait_for_connection(client):
    """Perform a blocking check that a connection can be made to the server"""
    try:
        client.wait_for_available(5)
        print(f"Connected")
    except Exception as e:
        if type(e) is not flight.FlightUnauthenticatedError:
            print(f"⁉ Failed to connect to {location}: {e.args}")
            sys.exit(1)
        else:
            print("Server requires auth, but connection possible")

def get_actions(client, options={}):
    """Discover available actions on the server"""
    if type(options) == dict:
        options = flight.FlightCallOptions(headers=list(options.items()))

    actions = list(client.list_actions(options=options))
    if len(actions) == 0:
        print("Found zero actions 😕")
    else:
        print(f"💥 Found {len(actions)} actions!")
    return actions

def submit_read(client, cypher, params={}, options={}):
    """Submit a cypherRead action and get a flight ticket"""
    if type(options) == dict:
        options = flight.FlightCallOptions(headers=list(options.items()))

    cypher_bytes = cypher.encode("utf8")
    params_bytes = json.dumps(params).encode("utf8")

    # Our CypherMessage format is simple:
    #   - 16 bit unsigned length of the cypher byte string
    #   - the cypher byte string payload
    #   - 16 bit unsigned length of the param json payload
    #   - the param json byte string payload
    pattern = f"!H{len(cypher_bytes)}sH{len(params_bytes)}s"
    buffer = struct.pack(pattern,
        len(cypher_bytes), cypher_bytes, 
        len(params_bytes), params_bytes)
    ticket = None
    try:
        results = client.do_action(("cypherRead", buffer), options=options)
        ticket = pa.flight.Ticket.deserialize((next(results).body.to_pybytes()))
    except Exception as e:
        print(f"⚠ submit_read: {e}")
        sys.exit(1)
    return ticket

def check_flight_status(client, ticket, options):
    """Check on a flight's status given a particular Ticket"""
    if type(options) == dict:
        options = flight.FlightCallOptions(headers=list(options.items()))
    if type(ticket) == pa.flight.Ticket:
        buffer = ticket.serialize()
    else:
        buffer = ticket
    status = None
    try:
        results = client.do_action(("jobStatus", buffer), options=options)
        status = next(results).body.to_pybytes().decode("utf8")
    except Exception as e:
        print(f"⚠ check_flight_status: {e}")
        sys.exit(1)
    return status

def list_flights(client, options={}):
    """List all available flights"""
    if type(options) == dict:
        options = flight.FlightCallOptions(headers=list(options.items()))
    pass

def get_flight_info(client, ticket, options):
    """Find a flight based on the given ticket"""
    if type(options) == dict:
        options = flight.FlightCallOptions(headers=list(options.items()))
    if type(ticket) == pa.flight.Ticket:
        buffer = ticket.serialize()
    else:
        buffer = ticket
    descriptor = pa.flight.FlightDescriptor.for_command(buffer)
    info = None
    try:
        info = client.get_flight_info(descriptor, options=options)
    except Exception as e:
        print(f"⚠ get_flight_info: {e}")
    return info

def stream_flight(client, ticket, options):
    """Stream back a given flight, assuming it's ready to stream"""
    if type(options) == dict:
        options = flight.FlightCallOptions(headers=list(options.items()))
    result = client.do_get(ticket, options=options)
    start = time()
    cnt = 0
    for chunk, metadata in result:
        cnt = cnt + chunk.num_rows
        print(f"Current Row @ {cnt:,}:\t[fields: {chunk.schema.names}, rows: {chunk.num_rows:,}]")
        #for col in chunk:
        #    print(col)
    finish = time()
    print(f"Done! Time Delta: {round(finish - start, 1):,}s")
    print(f"Count: {cnt:,} rows, Rate: {round(cnt / (finish - start)):,} rows/s")

##############################################################################

if __name__ == "__main__":
    location = build_location()
    client = flight.FlightClient(location)
    
    print(f"Trying to connect to location {location}")
    wait_for_connection(client)

    # TODO: username/password args? env?
    options = flight.FlightCallOptions(headers=[
        (b'authorization', b'Basic ' + base64.b64encode(b'neo4j:password'))
    ])

    print(f"Enumerating available actions from location {location}")
    for action in get_actions(client, options):
        print(f"  {action}")

    # TODO: user-supplied cypher/params
    print("Submitting a read cypher action/job using:")
    cypher = """
        UNWIND range(1, $rows) AS row
        RETURN row, [_ IN range(1, $dimension) | rand()] as fauxEmbedding
    """
    params = {"rows": 1_000_000, "dimension": 128}
    print(f"  cypher: {cypher}")
    print(f"  params: {params}")
    ticket = submit_read(client, cypher, params, options)
    print(f"Got ticket: {ticket}")

    print("Waiting for flight to be available...")
    for i in range(1, 10):
        status = check_flight_status(client, ticket, options)
        print(f"  status: {status}")
        if status == "PRODUCING":
            break
        else:
            sleep(3)
    
    print("Flight ready! Getting flight info...")
    info = None
    while info is None:
        sleep(3)
        try:
            info = get_flight_info(client, ticket, options)
        except Exception as e:
            print(f"failed to get flight info...retrying in 5s")
            sleep(5)

    print(f"Got info on our flight: {info}")

    print("Boarding flight and getting stream...")
    stream_flight(client, ticket, options)
