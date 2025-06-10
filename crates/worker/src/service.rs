use igloo_api::igloo::{
    worker_service_server::WorkerService, DataForTaskRequest, DataForTaskResponse, TaskDefinition,
    TaskResult,
};
use tonic::{Request, Response, Status};

pub struct MyWorkerService;

#[tonic::async_trait]
impl WorkerService for MyWorkerService {
    async fn execute_task(
        &self,
        request: Request<TaskDefinition>,
    ) -> Result<Response<TaskResult>, Status> {
        // TODO: Authentication/authorization stub
        // e.g., check request.metadata() for auth token
        // TODO: Add TLS support for gRPC in main.rs
        println!("Worker received ExecuteTask: {:?}", request.get_ref().task_id);
        Ok(Response::new(TaskResult { task_id: request.get_ref().task_id.clone(), result: vec![] }))
    }
    async fn get_data_for_task(
        &self,
        request: Request<DataForTaskRequest>,
    ) -> Result<Response<DataForTaskResponse>, Status> {
        println!("Worker received GetDataForTask: {:?}", request.get_ref().task_id);
        Ok(Response::new(DataForTaskResponse { data: vec![] }))
    }
}
