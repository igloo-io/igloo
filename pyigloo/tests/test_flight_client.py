import pytest
import subprocess
import time
import pyarrow as pa
from pyigloo import IglooClient # Assuming IglooClient is available after maturin build
import os
import signal

# Configuration for the coordinator
COORDINATOR_HOST = "127.0.0.1"
COORDINATOR_PORT = 50051 # Default Flight port
COORDINATOR_PATH = os.path.expanduser("~/.igloo/bin/igloo-coordinator") # Path to coordinator binary

@pytest.fixture(scope="module")
def coordinator_process():
    # Command to start the coordinator
    # Ensure IGLOO_HOME is set or coordinator path is correct
    # Adjust command as necessary based on how igloo-coordinator is run
    cmd = [COORDINATOR_PATH, "--flight-port", str(COORDINATOR_PORT)]

    process = None
    try:
        # Start the coordinator process
        print(f"Starting coordinator with command: {' '.join(cmd)}")
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Wait for the coordinator to start
        # This might need a more sophisticated check, e.g., polling a health endpoint
        time.sleep(5) # Adjust sleep time as needed

        # Check if process started successfully
        if process.poll() is not None: # Process terminated
            stdout, stderr = process.communicate()
            raise RuntimeError(f"Coordinator failed to start. Exit code: {process.returncode}\nStdout: {stdout.decode()}\nStderr: {stderr.decode()}")

        print("Coordinator process started.")
        yield process

    finally:
        if process and process.poll() is None: # If process is still running
            print("Terminating coordinator process...")
            # Terminate politely first
            process.terminate()
            try:
                process.wait(timeout=10) # Wait for graceful shutdown
            except subprocess.TimeoutExpired:
                print("Coordinator did not terminate gracefully, killing...")
                process.kill() # Force kill if terminate doesn't work
            print("Coordinator process terminated.")
        elif process and process.poll() is not None:
             print(f"Coordinator process already terminated with code {process.returncode}.")


def test_connect_and_execute_query(coordinator_process):
    """
    Tests connecting to the coordinator and executing a simple SQL query.
    """
    client = IglooClient()

    # Attempt to connect
    try:
        print(f"Attempting to connect to {COORDINATOR_HOST}:{COORDINATOR_PORT}...")
        client.connect(COORDINATOR_HOST, COORDINATOR_PORT)
        print("Connection successful.")
    except Exception as e:
        pytest.fail(f"Failed to connect to the coordinator: {e}")

    # Execute a query
    sql_query = "SELECT 1 AS value, 'hello' AS greeting"
    try:
        print(f"Executing query: {sql_query}")
        result = client.execute(sql_query)
        print(f"Query executed. Result type: {type(result)}")
    except Exception as e:
        # If the coordinator isn't fully functional or the Rust part has issues, this will fail.
        # In a real CI, we'd need a fully working coordinator.
        # For now, this helps identify if the Python -> Rust call is made.
        pytest.fail(f"Failed to execute query: {e}")

    # Validate the results
    # This part assumes the query execution is successful and returns a pyarrow object.
    # Due to sandbox limitations, the actual execution might not get this far.
    assert result is not None, "Query returned None, expected a pyarrow object."

    if isinstance(result, list):
        assert len(result) > 0, "Query returned an empty list of RecordBatches."
        # Validate the first RecordBatch as an example
        record_batch = result[0]
    else:
        record_batch = result

    assert isinstance(record_batch, pa.RecordBatch), f"Expected a pyarrow.RecordBatch, got {type(record_batch)}"

    expected_schema = pa.schema([
        ("value", pa.int32()),      # Adjusted to int32 based on typical SQL behavior for '1'
        ("greeting", pa.string())
    ])
    assert record_batch.schema.equals(expected_schema), f"Schema mismatch. Expected: {expected_schema}, Got: {record_batch.schema}"

    # Check data - this is a very basic check
    # A more robust check would convert to_pydict() and compare values.
    assert record_batch.num_rows == 1, f"Expected 1 row, got {record_batch.num_rows}"

    # Example of how to check actual data:
    # data_dict = record_batch.to_pydict()
    # assert data_dict['value'] == [1], "Value column mismatch"
    # assert data_dict['greeting'] == ['hello'], "Greeting column mismatch"
    print("Test test_connect_and_execute_query passed.")
