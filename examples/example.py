from src.main.neo4j_arrow import neo4j_arrow as na

client = na.Neo4jArrow('neo4j', 'password', ('voutila-arrow-test', 9999))
ticket = client.gds_nodes('mygraph', properties=['n'])

table = client.stream(ticket).read_all()
print(f"table num_rows={table.num_rows:,}")

data = table[5:8].to_pydict()
print(f"nodeIds: {data['nodeId']}")
print(f"embeddings (sample): {[x[0:4] for x in data['n']]}")

