# simplified_duckdb_client.py

import uuid
from datetime import datetime
import pyarrow as pa
import pyarrow.flight
from uhashring import HashRing

# Static configuration
SHARD_PORTS = [10000, 10001, 10002]
TABLE_NAME = "distributed_data"


def write_to_duckdb(key, value):
    """Write data to the appropriate DuckDB instance based on consistent hashing"""
    # Create node configuration as per uhashring documentation
    nodes = {}
    for i, port in enumerate(SHARD_PORTS):
        shard_name = f"shard{i}"
        nodes[shard_name] = {"hostname": "localhost", "port": port}

    # Create the hash ring with properly configured nodes
    ring = HashRing(nodes)

    # Determine which node should receive this data using consistent hashing
    target_node = ring.get_node(key)
    node_config = nodes[target_node]
    host, port = node_config["hostname"], node_config["port"]

    # Connect to the target node
    location = pyarrow.flight.Location.for_grpc_tcp(host, int(port))
    client = pyarrow.flight.FlightClient(location)

    # Create a PyArrow table with the data
    data = pa.Table.from_pylist([{
        "id": key,
        "value": value,
        "timestamp": datetime.now()
    }])

    # Write the data to the appropriate shard
    descriptor = pyarrow.flight.FlightDescriptor.for_path(TABLE_NAME)
    writer, _ = client.do_put(descriptor, data.schema)
    writer.write_table(data)
    writer.close()

    return target_node


def read_from_duckdb(key):
    """Read data from the appropriate DuckDB instance based on consistent hashing"""
    # Create node configuration as per uhashring documentation
    nodes = {}
    for i, port in enumerate(SHARD_PORTS):
        shard_name = f"shard{i}"
        nodes[shard_name] = {"hostname": "localhost", "port": port}

    # Create the hash ring with properly configured nodes
    ring = HashRing(nodes)

    # Determine which node should have this data using consistent hashing
    target_node = ring.get_node(key)
    node_config = nodes[target_node]
    host, port = node_config["hostname"], node_config["port"]

    # Connect to the target node
    location = pyarrow.flight.Location.for_grpc_tcp(host, int(port))
    client = pyarrow.flight.FlightClient(location)

    # Create a query to fetch the data
    query = f"SELECT * FROM {TABLE_NAME} WHERE id = '{key}'"
    ticket = pyarrow.flight.Ticket(query.encode('utf-8'))

    # Execute the query
    reader = client.do_get(ticket)
    table = reader.read_all()

    return table, target_node


if __name__ == "__main__":
    # Example usage
    key = str(uuid.uuid4())
    value = "test-value"

    # Write the data
    target_node = write_to_duckdb(key, value)

    print(f"Key: {key}")
    print(f"Value: {value}")
    print(f"Written to: {target_node}")

    # Read the data back (optional example)
    try:
        result, read_node = read_from_duckdb(key)
        print(f"Read from: {read_node}")
        print(f"Read result: {result.to_pylist()}")
    except Exception as e:
        print(f"Error reading data: {e}")
