use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightDescriptor, FlightData};
use futures::stream::{StreamExt, TryStreamExt};
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_arrow::PyArrowConvert;
use tonic::transport::Channel;

#[pyclass]
struct PyIglooClient {
    client: Option<FlightServiceClient<Channel>>,
    runtime: tokio::runtime::Runtime,
}

#[pymethods]
impl PyIglooClient {
    #[new]
    fn new() -> PyResult<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to create Tokio runtime: {}", e)))?;
        Ok(PyIglooClient { client: None, runtime })
    }

    #[pyo3(signature = (host, port))]
    fn connect(&mut self, host: String, port: u16) -> PyResult<()> {
        self.runtime.block_on(async {
            let addr = format!("http://{}:{}", host, port);
            let channel = Channel::from_shared(addr)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(format!("Invalid URI: {}", e)))?
                .connect()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(format!("Failed to connect: {}", e)))?;
            self.client = Some(FlightServiceClient::new(channel));
            Ok(())
        })
    }

    #[pyo3(signature = (sql))]
    fn execute(&mut self, sql: String) -> PyResult<PyObject> {
        self.runtime.block_on(async {
            let client = self.client.as_mut().ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Client not connected")
            })?;

            let request = FlightDescriptor::new_cmd(sql.into());
            let mut flight_info = client
                .get_flight_info(request)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get flight info: {}", e)))?
                .into_inner();

            let endpoint = flight_info.endpoint.pop().ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No endpoint found"))?;
            let ticket = endpoint.ticket.ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No ticket found"))?;

            let mut stream = client.do_get(ticket).await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to execute do_get: {}", e)))?
                .into_inner();

            let mut record_batches = vec![];
            let mut schema = None;

            Python::with_gil(|gil| {
                while let Some(flight_data_result) = stream.next().await {
                    let flight_data = flight_data_result.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to read flight data: {}", e)))?;

                    // Assuming the first message contains the schema
                    if schema.is_none() {
                         if let Some(s) = flight_data.schema.as_ref() {
                            schema = Some(s.clone());
                        }
                    }

                    // pyo3_arrow needs a schema to convert FlightData to RecordBatch
                    // We are trying to get it from the FlightData itself or from a previous message
                    // This part might need adjustment based on how Igloo actually sends schema information
                    let current_schema_ref = flight_data.schema.as_ref().or_else(|| schema.as_ref());

                    if let Some(s_ref) = current_schema_ref {
                        let arrow_schema = arrow_flight::utils::arrow_schema_from_flight_data(&flight_data, s_ref)
                            .map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!("Could not convert FlightData schema to Arrow schema: {}", e)))?;

                        let record_batch = arrow_flight::utils::flight_data_to_arrow_batch(&flight_data, arrow_schema.into(), &vec![])
                            .map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!("Could not convert FlightData to Arrow RecordBatch: {}", e)))?;

                        record_batches.push(record_batch.to_pyarrow(gil)?);
                    } else if !flight_data.data_body.is_empty() {
                        // If there's data but no schema, it's an issue.
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("Received data before schema, or schema not found in FlightData."));
                    }
                }

                if record_batches.is_empty() {
                    // If no record batches were produced, return None or an empty list
                    // depending on desired Python API. Here, returning PyNone.
                    Ok(pyo3::Python::None(gil))
                } else if record_batches.len() == 1 {
                    Ok(record_batches.remove(0))
                } else {
                    Ok(PyList::new(gil, record_batches).to_object(gil))
                }
            })
        })
    }
}

#[pymodule]
fn pyigloo(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyIglooClient>()?;
    Ok(())
}
