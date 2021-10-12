import neo4j_arrow as na
import pandas as pd
from time import sleep

client = na.Neo4jArrow('neo4j', 'password', ('<YOUR HOST HERE>', 9999))

db, graph, props = 'neo4j', 'mygraph', ['fastRp']
print(f"fetching graph '{graph}' from db '{db}' with node props {props}")

ticket = client.gds_nodes(graph, database=db, properties=props)
nodes = client.stream(ticket).read_all().to_pandas()
print(f"Got nodes:\n{nodes}")

ticket = client.gds_relationships(graph, database=db)
rels = client.stream(ticket).read_all().to_pandas()
# for now we need to slice off rel props
subrels = rels[['_source_id_', '_target_id_', '_type_']]
print(f"Got relationships:\n{subrels}")

#### Now for writing!
db, graph = 'hacks', 'test'
print(f"Writing our graph to db '{db}', graph '{graph}'")

ticket = client.gds_write_nodes(graph, database=db)
client.put_stream(ticket, nodes)
print("Wrote nodes...")

sleep(0.5)

ticket = client.gds_write_relationships(graph, database=db)
client.put_stream(ticket, subrels)
print("Wrote rels...")


