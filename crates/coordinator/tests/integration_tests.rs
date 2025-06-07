use igloo_coordinator::{run_server, ClusterState, QueryState};
use igloo_api::igloo::{
    coordinator_service_client::CoordinatorServiceClient, QueryRequest, QueryStatusRequest,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};
use tonic::transport::Channel;
use uuid::Uuid;

const TEST_SERVER_ADDR: &str = "127.0.0.1:50058"; // Using a fixed port for tests for simplicity

#[tokio::test]
async fn test_milestone1_query_submission_and_planning() -> Result<(), anyhow::Error> {
    // 1. Setup server
    let addr: SocketAddr = TEST_SERVER_ADDR.parse()?;
    let cluster_state: ClusterState = Arc::new(Mutex::new(HashMap::new()));
    let query_state: QueryState = Arc::new(Mutex::new(HashMap::new()));

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_cluster_state = cluster_state.clone();
    let server_query_state = query_state.clone();

    let server_handle = tokio::spawn(async move {
        println!("Test server starting on {}...", addr);
        if let Err(e) = run_server(addr, server_cluster_state, server_query_state, shutdown_rx).await {
            eprintln!("Test server failed: {:?}", e);
        }
        println!("Test server shut down.");
    });

    // Give the server a moment to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 2. Test Logic
    println!("Connecting to test server...");
    let mut client: CoordinatorServiceClient<Channel> = match CoordinatorServiceClient::connect(format!("http://{}", TEST_SERVER_ADDR)).await {
        Ok(c) => c,
        Err(e) => {
            // Attempt to shutdown server if connection fails
            let _ = shutdown_tx.send(());
            server_handle.await?;
            return Err(anyhow::anyhow!("Failed to connect to test server: {}. Server was shut down.", e));
        }
    };
    println!("Connected.");

    let query_id = Uuid::new_v4().to_string();
    let sql = "SELECT * FROM test_table".to_string();

    println!("Submitting query {}...", query_id);
    let submit_response = client
        .submit_query(QueryRequest {
            query_id: query_id.clone(),
            sql: sql.clone(),
        })
        .await?
        .into_inner();

    assert_eq!(submit_response.query_id, query_id);
    assert_eq!(submit_response.status, "Submitted");
    println!("Query {} submitted successfully.", query_id);

    // Polling for status
    let polling_timeout = Duration::from_secs(5);
    let poll_interval = Duration::from_millis(100);
    let mut time_elapsed = Duration::from_secs(0);
    let mut final_status_response: Option<igloo_api::igloo::QueryStatusResponse> = None;

    println!("Polling for status of query {}...", query_id);
    while time_elapsed < polling_timeout {
        tokio::time::sleep(poll_interval).await;
        time_elapsed += poll_interval;

        println!("Polling attempt for {}: elapsed {:?}", query_id, time_elapsed);
        match client
            .get_query_status(QueryStatusRequest {
                query_id: query_id.clone(),
            })
            .await
        {
            Ok(response) => {
                let status_resp = response.into_inner();
                println!("Status for {}: {}", query_id, status_resp.status);

                // For Milestone 1, we want to ensure it has at least reached task generation.
                // It might quickly move to "Dispatching" or "DispatchFailed_NoWorkers".
                // So, we check if plan and tasks are populated once it's past "Planned".
                if status_resp.status != "Submitted" && status_resp.status != "Planned" {
                    final_status_response = Some(status_resp);
                    break;
                }
                 // If it's exactly "TasksGenerated", we can also break early.
                if status_resp.status == "TasksGenerated" {
                    final_status_response = Some(status_resp);
                    break;
                }
            }
            Err(e) => {
                println!("Error getting status for {}: {:?}", query_id, e);
                // Consider breaking on NotFound if it persists, but for now, let timeout handle it.
            }
        }
    }

    assert!(final_status_response.is_some(), "Polling timed out or query did not progress past 'Planned' state for query {}", query_id);

    let status_response = final_status_response.unwrap();
    println!("Final polled status for query {}: {}", query_id, status_response.status);

    // Core assertions for Milestone 1: Planning and Task Generation occurred.
    assert_eq!(status_response.query_id, query_id);
    assert_eq!(status_response.sql, sql);

    assert!(status_response.physical_plan_bytes.is_some(), "Physical plan bytes should not be None. Status was: {}", status_response.status);
    let plan_bytes = status_response.physical_plan_bytes.as_ref().unwrap();
    assert!(!plan_bytes.is_empty(), "Physical plan bytes should not be empty. Status was: {}", status_response.status);

    // Verify dummy plan content (optional, but good for this test)
    // Assuming bincode is available for tests, or skip if not easily added to test context
    // For now, let's assume it's okay to not deserialize in test, just check presence.
    // let deserialized_plan: String = bincode::deserialize(plan_bytes).unwrap_or_default();
    // assert_eq!(deserialized_plan, "dummy_physical_plan");

    assert!(!status_response.tasks.is_empty(), "Should have generated some tasks. Status was: {}", status_response.status);
    println!("Query {} check complete. Final status '{}', plan and tasks are populated.", query_id, status_response.status);

    // 3. Teardown
    println!("Shutting down test server...");
    let _ = shutdown_tx.send(()); // Send shutdown signal
    match tokio::time::timeout(Duration::from_secs(5), server_handle).await {
        Ok(Ok(_)) => println!("Server task joined successfully."),
        Ok(Err(e)) => eprintln!("Server task panicked or returned an error: {:?}", e), // This is if the JoinHandle itself errors
        Err(_) => eprintln!("Timeout waiting for server task to join. It might have been aborted or stuck."),
    }

    Ok(())
}

const COORDINATOR_ADDR_M3: &str = "127.0.0.1:50059"; // Different port for M3 coordinator
const WORKER_ADDR_M3: &str = "127.0.0.1:50060";      // Port for M3 worker

#[tokio::test]
async fn test_milestone3_full_end_to_end_execution() -> Result<(), anyhow::Error> {
    // 1. Setup Servers
    let coordinator_addr: SocketAddr = COORDINATOR_ADDR_M3.parse()?;
    let worker_addr: SocketAddr = WORKER_ADDR_M3.parse()?;
    let worker_id = Uuid::new_v4().to_string();

    let coord_cluster_state: ClusterState = Arc::new(Mutex::new(HashMap::new()));
    let coord_query_state: QueryState = Arc::new(Mutex::new(HashMap::new()));

    let (coord_shutdown_tx_orig, coord_shutdown_rx) = oneshot::channel::<()>();
    let (worker_shutdown_tx_orig, worker_shutdown_rx) = oneshot::channel::<()>();

    let mut opt_coord_shutdown_tx = Some(coord_shutdown_tx_orig);
    let mut opt_worker_shutdown_tx = Some(worker_shutdown_tx_orig);

    // Start Coordinator
    let coord_cs_clone = coord_cluster_state.clone();
    let coord_qs_clone = coord_query_state.clone();
    let coordinator_handle = tokio::spawn(async move {
        println!("M3 Coordinator starting on {}...", coordinator_addr);
        if let Err(e) = igloo_coordinator::run_server(coordinator_addr, coord_cs_clone, coord_qs_clone, coord_shutdown_rx).await {
            eprintln!("M3 Coordinator server failed: {:?}", e);
        }
        println!("M3 Coordinator server shut down.");
    });

    // Start Worker
    // Ensure igloo_worker::run_worker_server is available. Add igloo_worker to dev-dependencies.
    let worker_id_clone = worker_id.clone();
    let coordinator_http_addr = format!("http://{}", COORDINATOR_ADDR_M3); // Worker needs http scheme
    let worker_handle = tokio::spawn(async move {
        println!("M3 Worker {} starting on {}, connecting to {}...", worker_id_clone, worker_addr, coordinator_http_addr);
        if let Err(e) = igloo_worker::run_worker_server(worker_id_clone, worker_addr, coordinator_http_addr, worker_shutdown_rx).await {
            eprintln!("M3 Worker server failed: {:?}", e);
        }
        println!("M3 Worker server shut down.");
    });

    // Give servers time to start and worker to register
    tokio::time::sleep(Duration::from_secs(1)).await; // Increased sleep for registration

    // 2. Test Logic
    println!("M3: Connecting to coordinator server {}...", COORDINATOR_ADDR_M3);
    let mut client = CoordinatorServiceClient::connect(format!("http://{}", COORDINATOR_ADDR_M3)).await
        .map_err(|e| {
            // If connection fails, try to clean up spawned tasks.
            if let Some(tx) = opt_coord_shutdown_tx.take() { tx.send(()).ok(); }
            if let Some(tx) = opt_worker_shutdown_tx.take() { tx.send(()).ok(); }
            anyhow::anyhow!("M3: Failed to connect to coordinator: {}", e)
        })?;
    println!("M3: Connected to coordinator.");

    let query_id = Uuid::new_v4().to_string();
    let sql = "SELECT * FROM e2e_table".to_string();

    println!("M3: Submitting query {}...", query_id);
    client.submit_query(QueryRequest { query_id: query_id.clone(), sql: sql.clone() }).await?;
    println!("M3: Query {} submitted.", query_id);

    // Polling for "Finished" status
    let polling_timeout = Duration::from_secs(15); // Increased timeout for E2E
    let poll_interval = Duration::from_millis(200);
    let mut time_elapsed = Duration::from_secs(0);
    let mut final_query_status: Option<igloo_api::igloo::QueryStatusResponse> = None;

    println!("M3: Polling for 'Finished' status for query {}...", query_id);
    while time_elapsed < polling_timeout {
        tokio::time::sleep(poll_interval).await;
        time_elapsed += poll_interval;

        match client.get_query_status(QueryStatusRequest { query_id: query_id.clone() }).await {
            Ok(response) => {
                let status_resp = response.into_inner();
                println!("M3: Status for {}: {}", query_id, status_resp.status);
                if status_resp.status == "Finished" {
                    final_query_status = Some(status_resp);
                    break;
                }
                if status_resp.status.starts_with("Failed") || status_resp.status.starts_with("DispatchFailed") {
                    // If it failed early, capture it to fail the test meaningfully
                    final_query_status = Some(status_resp);
                    break;
                }
            }
            Err(e) => {
                println!("M3: Error getting status for {}: {:?}", query_id, e);
            }
        }
    }

    assert!(final_query_status.is_some(), "M3: Polling timed out before query finished or failed for query {}", query_id);
    let status_response = final_query_status.unwrap();

    // Assertions for End-to-End Execution
    assert_eq!(status_response.status, "Finished", "Query status should be 'Finished'. Final status was: {}", status_response.status);
    assert!(status_response.finished_at.is_some(), "finished_at should be Some");

    assert_eq!(status_response.tasks.len(), 2, "Expected 2 tasks."); // From our dummy plan

    let task_statuses = status_response.task_statuses;
    assert!(!task_statuses.is_empty(), "Task statuses map should not be empty");
    assert_eq!(task_statuses.len(), 2, "Should have status for 2 tasks");

    for (task_id, status) in task_statuses {
        assert_eq!(status, "Succeeded", "Task {} should have succeeded. Status was: {}", task_id, status);
    }

    println!("M3: All assertions passed for query {}.", query_id);

    // 3. Teardown
    println!("M3: Shutting down worker server...");
    if let Some(tx) = opt_worker_shutdown_tx.take() { tx.send(()).ok(); }
    if let Err(e) = tokio::time::timeout(Duration::from_secs(5), worker_handle).await {
        eprintln!("M3: Timeout or error joining worker handle: {:?}", e);
    }else {
        println!("M3: Worker server task joined.");
    }

    println!("M3: Shutting down coordinator server...");
    if let Some(tx) = opt_coord_shutdown_tx.take() { tx.send(()).ok(); }
    if let Err(e) = tokio::time::timeout(Duration::from_secs(5), coordinator_handle).await {
         eprintln!("M3: Timeout or error joining coordinator handle: {:?}", e);
    } else {
        println!("M3: Coordinator server task joined.");
    }

    Ok(())
}

#[tokio::test]
async fn test_milestone2_task_generation() -> Result<(), anyhow::Error> {
    // 1. Setup server
    let addr: SocketAddr = TEST_SERVER_ADDR.parse()?; // Use the same test server address or a different one if needed
    let cluster_state: ClusterState = Arc::new(Mutex::new(HashMap::new()));
    let query_state: QueryState = Arc::new(Mutex::new(HashMap::new()));

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_cluster_state = cluster_state.clone();
    let server_query_state = query_state.clone();

    let server_handle = tokio::spawn(async move {
        println!("M2 Test server starting on {}...", addr);
        if let Err(e) = run_server(addr, server_cluster_state, server_query_state, shutdown_rx).await {
            eprintln!("M2 Test server failed: {:?}", e);
        }
        println!("M2 Test server shut down.");
    });

    tokio::time::sleep(Duration::from_millis(200)).await; // Give server time to start

    // 2. Test Logic
    println!("M2: Connecting to test server...");
    let mut client: CoordinatorServiceClient<Channel> = match CoordinatorServiceClient::connect(format!("http://{}", TEST_SERVER_ADDR)).await {
        Ok(c) => c,
        Err(e) => {
            let _ = shutdown_tx.send(());
            server_handle.await?;
            return Err(anyhow::anyhow!("M2: Failed to connect to test server: {}. Server shut down.", e));
        }
    };
    println!("M2: Connected.");

    let query_id = Uuid::new_v4().to_string();
    let sql = "SELECT * FROM another_table".to_string();

    println!("M2: Submitting query {}...", query_id);
    client
        .submit_query(QueryRequest {
            query_id: query_id.clone(),
            sql: sql.clone(),
        })
        .await?
        .into_inner();
    println!("M2: Query {} submitted.", query_id);

    // Polling for status until tasks are generated
    let polling_timeout = Duration::from_secs(5);
    let poll_interval = Duration::from_millis(100);
    let mut time_elapsed = Duration::from_secs(0);
    let mut final_status_response: Option<igloo_api::igloo::QueryStatusResponse> = None;

    println!("M2: Polling for tasks for query {}...", query_id);
    while time_elapsed < polling_timeout {
        tokio::time::sleep(poll_interval).await;
        time_elapsed += poll_interval;

        match client
            .get_query_status(QueryStatusRequest {
                query_id: query_id.clone(),
            })
            .await
        {
            Ok(response) => {
                let status_resp = response.into_inner();
                println!("M2: Status for {}: {}, Tasks count: {}", query_id, status_resp.status, status_resp.tasks.len());
                // We need tasks to be populated for this test's assertions
                if !status_resp.tasks.is_empty() {
                    final_status_response = Some(status_resp);
                    break;
                }
                // Also break if it failed dispatch, but tasks should still be there
                if status_resp.status == "DispatchFailed_NoWorkers" {
                     final_status_response = Some(status_resp);
                    break;
                }
            }
            Err(e) => {
                println!("M2: Error getting status for {}: {:?}", query_id, e);
            }
        }
    }

    assert!(final_status_response.is_some(), "M2: Polling timed out or tasks not generated for query {}", query_id);

    let status_response = final_status_response.unwrap();
    println!("M2: Final polled status for query {}: {}", query_id, status_response.status);

    // Assertions for Task Generation
    assert_eq!(status_response.tasks.len(), 2, "Expected 2 tasks to be generated.");

    for i in 0..2 {
        let task = status_response.tasks.get(i).expect(&format!("Task {} should be present", i));

        let expected_task_id_suffix = format!("_task_{}", i);
        assert!(task.task_id.starts_with(&query_id), "Task ID {} should start with query_id {}", task.task_id, query_id);
        assert!(task.task_id.ends_with(&expected_task_id_suffix), "Task ID {} should end with {}", task.task_id, expected_task_id_suffix);

        let deserialized_payload: String = bincode::deserialize(&task.payload)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize task payload for task {}: {}", task.task_id, e))?;

        let expected_payload = format!("dummy_plan_fragment_for_task_{}", i);
        assert_eq!(deserialized_payload, expected_payload, "Payload for task {} did not match", task.task_id);
        println!("M2: Task {} assertions passed. Payload: '{}'", task.task_id, deserialized_payload);
    }

    println!("M2: All task generation assertions passed for query {}.", query_id);

    // 3. Teardown
    println!("M2: Shutting down test server...");
    let _ = shutdown_tx.send(());
    match tokio::time::timeout(Duration::from_secs(5), server_handle).await {
        Ok(Ok(_)) => println!("M2: Server task joined successfully."),
        Ok(Err(e)) => eprintln!("M2: Server task panicked or returned an error: {:?}", e),
        Err(_) => eprintln!("M2: Timeout waiting for server task to join."),
    }

    Ok(())
}
