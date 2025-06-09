use igloo_api::igloo::{
    worker_service_server::WorkerService,
    DataForTaskRequest,
    DataForTaskResponse,
    TaskDefinition,
    TaskStatus, // Changed from TaskResult
};
use tonic::{Request, Response, Status};

pub struct MyWorkerService;

#[tonic::async_trait]
impl WorkerService for MyWorkerService {
    async fn execute_task(
        &self,
        request: Request<TaskDefinition>,
    ) -> Result<Response<TaskStatus>, Status> {
        // Changed return type
        // TODO: Authentication/authorization stub
        // e.g., check request.metadata() for auth token
        // TODO: Add TLS support for gRPC in main.rs
        println!("Worker received ExecuteTask: {:?}", request.get_ref().task_id);
        // Return a TaskStatus instead of TaskResult
        Ok(Response::new(TaskStatus { status: "SUBMITTED".to_string() }))
    }
    async fn get_data_for_task(
        &self,
        request: Request<DataForTaskRequest>,
    ) -> Result<Response<DataForTaskResponse>, Status> {
        println!("Worker received GetDataForTask: {:?}", request.get_ref().task_id);
        Ok(Response::new(DataForTaskResponse { data: vec![] }))
    }
}
