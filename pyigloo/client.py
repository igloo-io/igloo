import pyarrow.flight
import pyarrow as pa
import polars as pl

class IglooClient:
    def __init__(self, host="localhost", port=50051):
        self.location = f"grpc+tcp://{host}:{port}"
        self.client = pyarrow.flight.FlightClient(self.location)

    def execute(self, sql: str, return_type: str = "arrow"):
        # Prepare Flight SQL command
        descriptor = pyarrow.flight.FlightDescriptor.for_command(sql)
        flight_info = self.client.get_flight_info(descriptor)
        # Get the endpoint ticket
        endpoint = flight_info.endpoints[0]
        ticket = endpoint.ticket
        # Fetch results
        reader = self.client.do_get(ticket)
        table = reader.read_all()
        if return_type == "arrow":
            return table
        elif return_type == "polars":
            return pl.from_arrow(table)
        else:
            raise ValueError(f"Unsupported return_type: {return_type}")
