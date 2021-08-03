import pyarrow as pa
import pyarrow.flight as flight
import base64
import json
import struct
import sys
from time import time

pa.enable_signal_handlers(True)

#location = ("localhost", 9999)
location = ("192.168.1.42", 9999)
client = flight.FlightClient(location)
print(f"Trying to connect to location {location}")

try:
    client.wait_for_available(5)
    print(f"Connected")
except Exception as e:
    if type(e) is not flight.FlightUnauthenticatedError:
        print(f"⁉ Failed to connect to {location}: {e.args}")
        sys.exit(1)
    else:
        print("Server requires auth, but connection possible")

options = flight.FlightCallOptions(headers=[
    (b'authorization', b'Basic ' + base64.b64encode(b'neo4j:password'))
])

actions = list(client.list_actions(options=options))
if len(actions) == 0:
    print("Found zero actions 😕")
else:
    print(f"💥 Found {len(actions)} actions!")
    for action in actions:
        print(f"action {action}")

cypher = """
UNWIND range($i, $j) AS n RETURN n
""".encode("utf8")
params = json.dumps({"i": 1, "j": 10_000_000}).encode("utf8")
pattern = f"!H{len(cypher)}sH{len(params)}s"
print("pattern = " + pattern)
buf = struct.pack(pattern, len(cypher), cypher, len(params), params)

print(f"XXX: buf = {buf}")

action = ("cypherRead", buf)
ticket = None
try:
    for row in client.do_action(action, options=options):
        print(f"row: {row.body.to_pybytes()}")
        ticket = row.body
        break
except Exception as e:
    print(f"⚠ {e}")
    sys.exit(1)

action = ("cypherStatus", ticket.to_pybytes())
try:
    for row in client.do_action(action, options=options):
        print(f"status: {row.body.to_pybytes()}")
except Exception as e:
    print(f"problem getting status: {e}")
    sys.exit(1)

flights = list(client.list_flights(options=options))
if len(flights) == 0:
    print("Found zero flights 😕")
else:
    print(f"Found {len(flights)} flights")
    for flight in flights:
        ticket = flight.endpoints[0].ticket
        print(f"flight: [cmd={flight.descriptor.command}, ticket={ticket}")
        result = client.do_get(ticket, options=options)
        start = time()
        cnt = 0
        for chunk, metadata in result:
            for col in chunk:
                print(f"col: {col}")
                cnt = cnt + len(col)
        finish = time()
        print(f"Done! Time Delta: {round(finish - start, 1):,}s")
        print(f"Count: {cnt:,} rows, Rate: {round(cnt / (finish - start)):,} rows/s")

        action = ("cypherStatus", ticket.serialize())
        try:
            for row in client.do_action(action, options=options):
                print(f"status: {row.body.to_pybytes()}")
        except Exception as e:
            print(f"problem getting status: {e}")
            sys.exit(1)

        break
