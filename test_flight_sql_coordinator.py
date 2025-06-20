import pyarrow.flight as fl
import pyarrow as pa

def test_flight_sql_select_1():
    # Connect to the server
    client = fl.connect("grpc+tcp://localhost:50051")
    print("Connected to Flight SQL server.")

    # Prepare the ticket with the SQL query
    # For Flight SQL, the SQL query is typically sent as part of a CommandStatementQuery,
    # then the ticket is obtained from get_flight_info.
    # However, for a basic test and if the server expects SQL directly in the ticket's cmd:
    # sql_query = "SELECT 1"
    # ticket_bytes = sql_query.encode('utf-8')
    # ticket = fl.Ticket(ticket_bytes)

    # The example in the issue uses fl.Ticket(pa.to_buffer(b'SELECT 1'))
    # This seems to be a simplified way to send a command directly via do_get's ticket
    # Let's follow the issue's example for the ticket.
    query_ticket = fl.Ticket(pa.to_buffer(b'SELECT 1'))
    print(f"Sending query: SELECT 1 with ticket {query_ticket.ticket}")

    # Execute the query using do_get
    reader = client.do_get(query_ticket)
    print("do_get call successful, reading results...")

    # Read all RecordBatches
    table = reader.read_all()
    print(f"Received table:\n{table}")

    # Assertions
    assert table.num_rows == 1, f"Expected 1 row, got {table.num_rows}"
    assert table.column_names == ['result'], f"Expected column name 'result', got {table.column_names}"

    # Further inspect the data
    column_data = table.column(0)
    assert len(column_data) == 1, f"Expected 1 value in column, got {len(column_data)}"
    actual_value = column_data[0].as_py()
    assert actual_value == 1, f"Expected value 1, got {actual_value}"

    print("Flight SQL test successful!")

if __name__ == "__main__":
    # Ensure pyarrow and grpcio are installed:
    # pip install pyarrow grpcio
    # The issue also mentions grpcio-tools, but it's not strictly needed for the client.
    try:
        test_flight_sql_select_1()
    except Exception as e:
        print(f"An error occurred: {e}")
        print("Please ensure the igloo-coordinator is running and accessible at localhost:50051.")
        print("Required Python packages: pyarrow, grpcio")
        exit(1)
