use arrow_flight::sql::{CommandGetCatalogs, FlightSqlIpcClient};
use arrow_flight::{FlightData, FlightDataStream, utils::flight_data_to_arrow_batch};
use futures::TryStreamExt;
use pyo3::prelude::*;
use pyo3_arrow::PyArrowType;
use std::sync::Arc;
use tokio::runtime::Runtime;
use arrow_array::RecordBatch;
use arrow_schema::Schema;


#[pyclass]
struct Client {
    client: FlightSqlIpcClient,
}

#[pymethods]
impl Client {
    #[new]
    fn new(host: String, port: u16) -> PyResult<Self> {
        let rt = Runtime::new()?;
        let addr = format!("grpc://{}:{}", host, port);
        // TODO: Pass a channel to FlightSqlIpcClient::connect_generic instead of addr directly.
        // This is a workaround for a bug in arrow-flight that causes a hang when connecting to a server that is not running.
        // See https://github.com/apache/arrow-rs/issues/5515 for more details.
        let client_result = rt.block_on(async {
            let channel = arrow_flight::grpc::create_flight_channel(&addr, None, None, None).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError>(format!("Failed to create channel: {}", e)))?;
            FlightSqlIpcClient::new(channel).await
        });

        match client_result {
            Ok(client) => Ok(Client { client }),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyValueError>(format!(
                "Failed to connect: {}",
                e
            ))),
        }
    }

    fn execute(&mut self, _py: Python<'_>, _sql: String) -> PyResult<PyArrowType<Vec<RecordBatch>>> {
        let rt = Runtime::new()?;
        rt.block_on(async {
            let command = CommandGetCatalogs {};
            let flight_info = self.client.get_catalogs(command).await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyValueError>(format!("Failed to get catalogs: {}", e))
            })?;

            // Assuming there's at least one endpoint.
            // In a real scenario, you might need to iterate or choose a specific endpoint.
            if flight_info.endpoint.is_empty() || flight_info.endpoint[0].ticket.is_none() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError>("No ticket found for GetCatalogs command"));
            }
            let ticket = flight_info.endpoint[0].ticket.clone().unwrap();


            let mut stream: FlightDataStream = self.client.do_get(ticket).await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyValueError>(format!("Failed to execute command: {}", e))
            })?;

            let mut batches = Vec::new();
            // The schema should be the first message in the stream
            let flight_data_schema = stream.try_next().await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError>(format!("Failed to get schema from stream: {}",e)))?.ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError>("Did not receive schema batch from flight server"))?;
            let schema = Arc::new(Schema::try_from(&flight_data_schema).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError>(format!("Could not convert FlightData to Schema: {}", e)))?);

            let mut dictionaries_by_id = std::collections::HashMap::new();

            while let Some(flight_data) = stream.try_next().await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyValueError>(format!("Failed to fetch flight data: {}", e))
            })? {
                // Dictionaries are not handled in this simplified example but would be in a full implementation
                if flight_data.flight_descriptor.is_some() {
                    // This is likely a schema message or dictionary batch, handle accordingly
                    // For now, we skip it, assuming the first message was the schema and subsequent are record batches
                    continue;
                }

                match flight_data_to_arrow_batch(&flight_data, schema.clone(), &dictionaries_by_id) {
                    Ok(batch) => batches.push(batch),
                    Err(e) => return Err(PyErr::new::<pyo3::exceptions::PyValueError>(format!("Failed to convert flight data to Arrow batch: {}", e))),
                }
            }

            Ok(PyArrowType(batches))
        })
    }
}

#[pymodule]
fn pyigloo(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Client>()?;
    Ok(())
}
