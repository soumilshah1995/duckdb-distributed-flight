# duckdb_server.py

import argparse
import time
from pathlib import Path
import duckdb
import pyarrow as pa
import pyarrow.flight

TABLE_NAME = "distributed_data"

class DuckDBFlightServer:
    def __init__(self, port, db_path):
        self.port = port
        self.db_path = db_path
        self.server = None

    def serve(self):
        """Start the Flight server using DuckDB as the backend"""
        # Define a FlightServerImpl that connects to DuckDB
        class DuckDBFlightServerImpl(pyarrow.flight.FlightServerBase):
            def __init__(self, location, db_path):
                super().__init__(location)
                self.db_path = db_path
                self.conn = duckdb.connect(db_path)
                # Create the table if it doesn't exist
                self.conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                        id VARCHAR,
                        value VARCHAR,
                        timestamp TIMESTAMP
                    )
                """)
                print(f"DuckDB database initialized at {db_path}")

            def do_put(self, context, descriptor, reader, writer):
                """Handle incoming data to be stored in DuckDB"""
                table = reader.read_all()
                # Convert PyArrow Table to DuckDB relation and append to the table
                duckdb_relation = self.conn.from_arrow(table)
                self.conn.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM duckdb_relation")
                self.conn.commit()
                print(f"Inserted {len(table)} rows into {TABLE_NAME}")
                return None

            def do_get(self, context, ticket):
                """Handle data retrieval requests"""
                # Execute the query in the ticket and return the result
                query = ticket.ticket.decode('utf-8')
                result = self.conn.execute(query).arrow()
                return pyarrow.flight.RecordBatchStream(result)

            def get_flight_info(self, context, descriptor):
                """Return flight info for a given descriptor"""
                # This is a simplified implementation
                schema = pa.schema([
                    pa.field('id', pa.string()),
                    pa.field('value', pa.string()),
                    pa.field('timestamp', pa.timestamp('ns'))
                ])
                endpoints = [pyarrow.flight.FlightEndpoint(
                    ticket=descriptor.command,
                    locations=[pyarrow.flight.Location.for_grpc_tcp("localhost", self.port)]
                )]
                return pyarrow.flight.FlightInfo(schema, descriptor, endpoints, -1, -1)

        # Start the server
        location = pyarrow.flight.Location.for_grpc_tcp("localhost", self.port)
        self.server = DuckDBFlightServerImpl(location, self.db_path)
        self.server.serve()
        print(f"Flight server for {self.db_path} started on port {self.port}")


def run_server(port, shard_name):
    """Start a DuckDB Flight server on the specified port"""
    db_path = f"{shard_name}.db"
    server = DuckDBFlightServer(port, db_path)
    try:
        server.serve()
    except KeyboardInterrupt:
        print(f"Server on port {port} shutting down")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DuckDB Flight Server")
    parser.add_argument("--port", type=int, required=True, help="Port for the server")
    parser.add_argument("--shard", required=True, help="Shard name (used for the database filename)")

    args = parser.parse_args()
    run_server(args.port, args.shard)