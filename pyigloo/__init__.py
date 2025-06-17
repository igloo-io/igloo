from .pyigloo import PyIglooClient

class IglooClient:
    def __init__(self):
        self._client = PyIglooClient()

    def connect(self, host: str, port: int):
        """
        Connects to the Igloo coordinator.

        Args:
            host: The hostname or IP address of the coordinator.
            port: The port number of the coordinator.
        """
        self._client.connect(host, port)

    def execute(self, sql: str):
        """
        Executes a SQL query.

        Args:
            sql: The SQL query string to execute.

        Returns:
            A pyarrow.RecordBatch or a list of pyarrow.RecordBatches containing the query results,
            or None if the query produces no results.
        """
        return self._client.execute(sql)

__all__ = ["IglooClient"]
