// #![allow(clippy::doc_overindented_list_items)]
// TODO: API crate for gRPC and Arrow Flight definitions

// Re-export the generated proto code
pub mod igloo {
    include!(concat!(env!("OUT_DIR"), "/igloo.rs")); // Defines FlightService trait

    use crate::arrow::flight::protocol::{FlightData, FlightInfo, Ticket};
    use flight_service_server::FlightService; // Corrected import
    use tokio_stream::wrappers::ReceiverStream;

    #[tonic::async_trait]
    impl FlightService for super::IglooFlightSqlService {
        type DoGetStream = ReceiverStream<Result<FlightData, tonic::Status>>;

        async fn get_flight_info(
            &self,
            _request: tonic::Request<FlightInfo>,
        ) -> Result<tonic::Response<FlightInfo>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "IglooFlightSqlService::get_flight_info is not implemented",
            ))
        }

        async fn do_get(
            &self,
            _request: tonic::Request<Ticket>,
        ) -> Result<tonic::Response<Self::DoGetStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("IglooFlightSqlService::do_get is not implemented"))
        }
    }
}

pub mod arrow {
    pub mod flight {
        pub mod protocol {
            include!(concat!(env!("OUT_DIR"), "/arrow.flight.protocol.rs"));
        }
    }
}

use arrow_flight::{
    flight_service_server::FlightService, /*Action, ActionType, Criteria, Empty,*/
};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use futures::Stream;
use igloo_engine::QueryEngine;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};
use tokio::sync::mpsc; // For MPSC channel
use tokio_stream::wrappers::ReceiverStream; // For creating stream from MPSC receiver
use arrow_flight::utils::{flight_data_from_arrow_schema, flight_data_from_arrow_batch};

pub struct IglooFlightSqlService {
    engine: Arc<QueryEngine>,
}

impl IglooFlightSqlService {
    pub fn new(engine: QueryEngine) -> Self {
        Self { engine: Arc::new(engine) }
    }
}

#[tonic::async_trait]
impl FlightService for IglooFlightSqlService {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + 'static>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake is not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights is not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let cmd_bytes = descriptor.cmd;
        if cmd_bytes.is_empty() {
            return Err(Status::invalid_argument("No SQL command in FlightDescriptor"));
        }
        let sql = String::from_utf8(cmd_bytes.to_vec()).unwrap_or_default();
        let batches = self.engine.execute(&sql).await;
        let schema = batches.first().map(|b| b.schema()).ok_or(Status::not_found("No results"))?;
        let options = IpcWriteOptions::default();
        let schema_ipc = SchemaAsIpc::new(schema.as_ref(), &options);
        let flight_data = FlightData::from(schema_ipc);
        let schema_bytes = flight_data.data_header;
        let flight_info = FlightInfo { schema: schema_bytes, ..Default::default() };
        Ok(Response::new(flight_info))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema is not yet implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let sql = match String::from_utf8(ticket.ticket) {
            Ok(s) => s,
            Err(_) => return Err(Status::invalid_argument("Ticket is not valid UTF-8")),
        };

        let batches = self.engine.execute(&sql).await;

        if batches.is_empty() {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            drop(tx); // Close sender immediately
            let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
            return Ok(Response::new(Box::pin(stream)));
        }

        let schema = batches[0].schema(); // schema is Arc<Schema>
        let (mut tx, rx) = tokio::sync::mpsc::channel(4);
        // The stream is created here and returned at the end.
        // The Box::pin is important for type compatibility with DoGetStream
        let stream = Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)) as Self::DoGetStream;

        // Clone schema for the spawned task, as batches will be moved.
        let task_schema = schema.clone();

        tokio::spawn(async move {
            let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();

            // Send Schema
            // Attempt to use arrow_flight::utils::flight_data_from_arrow_schema
            // Fallback to manual creation similar to get_flight_info if needed or if util has issues
            let schema_flight_data = match arrow_flight::utils::flight_data_from_arrow_schema(task_schema.as_ref(), &options) {
                Ok(data) => data,
                Err(_e) => { // Consider logging the error _e
                    // Fallback: manual creation (as in get_flight_info)
                    let schema_ipc = datafusion::arrow::ipc::SchemaAsIpc::new(task_schema.as_ref(), &options);
                    FlightData::from(schema_ipc) // This primarily sets data_header for schema
                }
            };

            if tx.send(Ok(schema_flight_data)).await.is_err() {
                // Receiver likely dropped, e.g., client disconnected
                return;
            }

            // Send RecordBatches
            // Re-fetch batches in the spawned task if not cloning/moving them.
            // For simplicity, let's assume `batches` (Vec<RecordBatch>) is moved into the task.
            // If `batches` is large, consider re-executing or a different strategy.
            // The current `self.engine.execute` returns `Vec<RecordBatch>`, so moving it is feasible.
            for batch in batches { // Assuming `batches` is moved into this async block
                match arrow_flight::utils::flight_data_from_arrow_batch(&batch, &options) {
                    Ok((flight_dictionaries, flight_batch)) => {
                        for dict_flight_data in flight_dictionaries {
                            if tx.send(Ok(dict_flight_data)).await.is_err() {
                                // Receiver likely dropped
                                return;
                            }
                        }
                        if tx.send(Ok(flight_batch)).await.is_err() {
                            // Receiver likely dropped
                            return;
                        }
                    }
                    Err(e) => {
                        // Log error or send an error status through the channel
                        // For now, attempt to send an error and then stop processing for this request
                        let error_status = Status::internal(format!("Failed to convert batch to FlightData: {}", e));
                        if tx.send(Err(error_status)).await.is_err() {
                            // If sending error also fails, just return
                        }
                        return;
                    }
                }
            }
            // Implicitly, tx is dropped when the task finishes, closing the stream for the client.
        });

        Ok(Response::new(stream))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put is not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange is not yet implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action is not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions is not yet implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info is not yet implemented"))
    }
}

#[cfg(test)]
mod tests {
    use super::*; // Imports IglooFlightSqlService
    use arrow_flight::{FlightData, Ticket};
    use datafusion::arrow::array::{BooleanArray, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema}; // SchemaRef removed as not directly used
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tokio_stream::StreamExt;
    use tonic::Request;
    use igloo_engine::QueryEngine; // As per prompt's decision

    #[tokio::test]
    async fn test_do_get_simple_query() {
        // 1. Setup
        let service = IglooFlightSqlService::new(QueryEngine::new());
        let sql = "SELECT CAST(1 AS BIGINT) AS int_col, 'test_val' AS str_col, true AS bool_col;";

        // 2. Create Ticket
        let ticket = Ticket { ticket: sql.into_bytes() };

        // 3. Call do_get
        let response = service.do_get(Request::new(ticket)).await.expect("do_get failed");

        // 4. Consume Stream & Collect FlightData
        let mut stream = response.into_inner();
        let mut flight_data_vec: Vec<FlightData> = Vec::new();
        while let Some(Ok(data)) = stream.next().await {
            flight_data_vec.push(data);
        }
        assert!(!flight_data_vec.is_empty(), "FlightData vector should not be empty");

        // 5. Verify Schema
        let schema_flight_data = &flight_data_vec[0];
        let schema = arrow_flight::utils::try_schema_from_flight_data(schema_flight_data)
            .expect("Failed to get schema from FlightData");

        let expected_schema = Arc::new(Schema::new(vec![
            Field::new("int_col", DataType::Int64, true),
            Field::new("str_col", DataType::Utf8, true),
            Field::new("bool_col", DataType::Boolean, true),
        ]));
        assert_eq!(schema, expected_schema, "Schema mismatch");

        // 6. Verify Data
        assert!(flight_data_vec.len() >= 2, "Expected at least schema and one data batch");
        let data_flight_data = &flight_data_vec[1];
        // Assuming no dictionaries for this simple query.
        let batch = arrow_flight::utils::flight_data_to_arrow_batch(data_flight_data, schema.clone(), &[])
            .expect("Failed to convert FlightData to RecordBatch");

        assert_eq!(batch.num_rows(), 1, "Expected one row");

        let int_col = batch.column_by_name("int_col").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int_col.value(0), 1);

        let str_col = batch.column_by_name("str_col").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_col.value(0), "test_val");

        let bool_col = batch.column_by_name("bool_col").unwrap().as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(bool_col.value(0), true);
    }
}
