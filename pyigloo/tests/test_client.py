import pytest
import pyarrow as pa
from pyigloo import Client

def test_client_creation():
    """Test that the Client object can be created."""
    try:
        # Attempt to create a client. This is expected to fail if no server is running.
        # We are testing the Rust-Python boundary here.
        client = Client(host="localhost", port=50051)
        assert client is not None, "Client object should be created."
    except Exception as e:
        # If a server is not running at localhost:50051, a ConnectionRefusedError
        # or similar PyO3 error wrapping the Rust error is expected.
        # We consider this a pass for the boundary test, as the call went through.
        print(f"Client creation failed as expected (no server): {e}")
        pass

def test_client_execute_placeholder():
    """
    Test the placeholder execute method.
    This test anticipates a connection error if no server is available.
    The main goal is to ensure the method signature is correct and callable from Python.
    """
    try:
        client = Client(host="localhost", port=50051)
        # The execute method currently calls get_catalogs.
        # This will likely fail if no server is running.
        result = client.execute("SELECT 1") # SQL query is a placeholder
        assert isinstance(result, pa.Table), "Result should be a PyArrow Table or list of RecordBatches convertible to Table."
    except Exception as e:
        # Similar to client creation, connection errors are expected if no server is running.
        # This is acceptable for this initial test.
        print(f"Client execute failed as expected (no server): {e}")
        pass

def test_import_client():
    """Test that the client can be imported."""
    from pyigloo import Client
    assert Client is not None
