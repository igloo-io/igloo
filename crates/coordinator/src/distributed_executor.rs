//! Distributed query executor

use crate::fragment::QueryFragment;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use futures::stream::{BoxStream, StreamExt};
use igloo_api::igloo::distributed_query_service_client::DistributedQueryServiceClient;
use igloo_api::igloo::{FragmentRequest, RecordBatchMessage};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

/// Distributed query executor that orchestrates fragment execution across workers
pub struct DistributedExecutor {
    worker_clients: HashMap<String, DistributedQueryServiceClient<Channel>>,
}

impl DistributedExecutor {
    pub fn new() -> Self {
        Self {
            worker_clients: HashMap::new(),
        }
    }

    pub async fn add_worker(&mut self, address: String) -> DataFusionResult<()> {
        let client = DistributedQueryServiceClient::connect(address.clone())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        
        self.worker_clients.insert(address, client);
        Ok(())
    }

    pub async fn execute_fragments(
        &mut self,
        fragments: Vec<QueryFragment>,
    ) -> DataFusionResult<BoxStream<'static, DataFusionResult<RecordBatch>>> {
        let (tx, rx) = mpsc::channel(100);
        
        let worker_clients = self.worker_clients.clone();
        
        tokio::spawn(async move {
            let mut completed_fragments = HashSet::new();
            let mut fragment_results: HashMap<String, Vec<RecordBatch>> = HashMap::new();
            let mut pending_fragments = fragments;
            
            while !pending_fragments.is_empty() {
                // Find fragments that are ready to execute
                let ready_fragments: Vec<_> = pending_fragments
                    .iter()
                    .enumerate()
                    .filter(|(_, fragment)| fragment.is_ready(&completed_fragments))
                    .map(|(i, _)| i)
                    .collect();
                
                if ready_fragments.is_empty() {
                    let _ = tx.send(Err(DataFusionError::Internal(
                        "Circular dependency detected in query fragments".to_string()
                    ))).await;
                    return;
                }
                
                // Execute ready fragments in parallel
                let mut tasks = Vec::new();
                let mut executed_indices = Vec::new();
                
                for &index in &ready_fragments {
                    let fragment = pending_fragments[index].clone();
                    executed_indices.push(index);
                    
                    if fragment.worker_address == "coordinator" {
                        // Execute locally on coordinator
                        let tx_clone = tx.clone();
                        let fragment_id = fragment.id.clone();
                        
                        tasks.push(tokio::spawn(async move {
                            let mut stream = match fragment.physical_plan.execute().await {
                                Ok(stream) => stream,
                                Err(e) => {
                                    let _ = tx_clone.send(Err(e)).await;
                                    return (fragment_id, Vec::new());
                                }
                            };
                            
                            let mut batches = Vec::new();
                            while let Some(batch_result) = stream.next().await {
                                match batch_result {
                                    Ok(batch) => {
                                        batches.push(batch.clone());
                                        if tx_clone.send(Ok(batch)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        let _ = tx_clone.send(Err(e)).await;
                                        break;
                                    }
                                }
                            }
                            
                            (fragment_id, batches)
                        }));
                    } else {
                        // Execute on remote worker
                        let mut client = match worker_clients.get(&fragment.worker_address) {
                            Some(client) => client.clone(),
                            None => {
                                let _ = tx.send(Err(DataFusionError::Internal(
                                    format!("Worker client not found: {}", fragment.worker_address)
                                ))).await;
                                return;
                            }
                        };
                        
                        let tx_clone = tx.clone();
                        let fragment_id = fragment.id.clone();
                        
                        tasks.push(tokio::spawn(async move {
                            let request = FragmentRequest {
                                fragment_id: fragment_id.clone(),
                                serialized_plan: serialize_plan(&fragment.physical_plan),
                                session_config: HashMap::new(),
                            };
                            
                            let mut batches = Vec::new();
                            
                            match client.execute_fragment(request).await {
                                Ok(response) => {
                                    let mut stream = response.into_inner();
                                    
                                    while let Some(message_result) = stream.next().await {
                                        match message_result {
                                            Ok(message) => {
                                                match deserialize_batch(&message) {
                                                    Ok(batch) => {
                                                        batches.push(batch.clone());
                                                        if tx_clone.send(Ok(batch)).await.is_err() {
                                                            break;
                                                        }
                                                    }
                                                    Err(e) => {
                                                        let _ = tx_clone.send(Err(e)).await;
                                                        break;
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                let _ = tx_clone.send(Err(
                                                    DataFusionError::External(Box::new(e))
                                                )).await;
                                                break;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    let _ = tx_clone.send(Err(
                                        DataFusionError::External(Box::new(e))
                                    )).await;
                                }
                            }
                            
                            (fragment_id, batches)
                        }));
                    }
                }
                
                // Wait for all tasks to complete
                for task in tasks {
                    match task.await {
                        Ok((fragment_id, batches)) => {
                            completed_fragments.insert(fragment_id.clone());
                            fragment_results.insert(fragment_id, batches);
                        }
                        Err(e) => {
                            let _ = tx.send(Err(DataFusionError::External(Box::new(e)))).await;
                            return;
                        }
                    }
                }
                
                // Remove executed fragments
                executed_indices.sort_by(|a, b| b.cmp(a)); // Sort in descending order
                for index in executed_indices {
                    pending_fragments.remove(index);
                }
            }
        });
        
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

impl Default for DistributedExecutor {
    fn default() -> Self {
        Self::new()
    }
}

// Placeholder functions for plan serialization/deserialization
fn serialize_plan(_plan: &Arc<dyn igloo_engine::physical_plan::ExecutionPlan>) -> Vec<u8> {
    // In a real implementation, this would serialize the physical plan
    // For now, return empty bytes
    Vec::new()
}

fn deserialize_batch(message: &RecordBatchMessage) -> DataFusionResult<RecordBatch> {
    // In a real implementation, this would deserialize the Arrow RecordBatch
    // For now, create a dummy batch
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("dummy", DataType::Int32, false),
    ]));
    
    let array = Int32Array::from(vec![1, 2, 3]);
    RecordBatch::try_new(schema, vec![Arc::new(array)])
        .map_err(|e| DataFusionError::ArrowError(e))
}