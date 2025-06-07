use igloo_api::igloo::{
    coordinator_service_client::CoordinatorServiceClient, HeartbeatInfo, WorkerInfo,
};
use igloo_coordinator::{
    config::Settings as CoordinatorSettings, // Assuming this is the Settings struct
    ClusterState, // Make sure ClusterState is pub or pub(crate)
    WorkerState,  // Make sure WorkerState is pub or pub(crate)
    run_server, // The new public function
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration; // For tokio::time::sleep
use tokio::sync::Mutex;
use tokio::time::sleep;
use tonic::Request;
use uuid::Uuid;

// Helper to create default settings for testing, pointing to a dynamic port.
fn test_settings(port: u16) -> CoordinatorSettings {
    CoordinatorSettings {
        host: "127.0.0.1".to_string(),
        port,
        prune_interval_secs: 2, // Short interval for faster tests
        worker_timeout_secs: 5,  // Short timeout for faster tests
    }
}

#[tokio::test]
async fn test_worker_registration_heartbeat_and_pruning() {
    // 0. Initialize a shared ClusterState for the test server
    let test_cluster_state: ClusterState = Arc::new(Mutex::new(HashMap::new()));

    // 1. Find a free port and configure settings
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind to random port for test server");
    let local_addr = listener
        .local_addr()
        .expect("Failed to get local address from test listener");
    let port = local_addr.port();
    // Ensure the listener is dropped *before* the server is spawned on this port.
    drop(listener);

    let settings = test_settings(port);
    let server_settings = settings.clone(); // Clone for the server task
    let server_cluster_state = test_cluster_state.clone(); // Clone for the server task

    // 2. Run the coordinator server in a background task
    tokio::spawn(async move {
        if let Err(e) = run_server(server_settings, server_cluster_state).await {
            eprintln!("Test server failed: {}", e);
        }
    });

    // Give the server a moment to start
    sleep(StdDuration::from_millis(100)).await;

    // 3. Create a gRPC client
    let coordinator_service_url = format!("http://127.0.0.1:{}", port);
    let mut client = CoordinatorServiceClient::connect(coordinator_service_url.clone())
        .await
        .expect("Failed to connect to test coordinator server");

    // 4. Test Worker Registration
    let worker_id = Uuid::new_v4().to_string();
    let worker_address = "127.0.0.1:50000".to_string(); // Example worker address
    let worker_info = WorkerInfo {
        id: worker_id.clone(),
        address: worker_address.clone(),
    };

    let reg_response = client
        .register_worker(Request::new(worker_info.clone()))
        .await
        .expect("RegisterWorker RPC failed");
    assert!(!reg_response.into_inner().message.is_empty());
    println!("Registration successful for worker {}", worker_id);

    // Verify state in coordinator
    {
        let state = test_cluster_state.lock().await;
        assert!(state.contains_key(&worker_id));
        println!("Worker {} found in coordinator state after registration.", worker_id);
    }

    // 5. Test SendHeartbeat
    let heartbeat_info = HeartbeatInfo {
        worker_id: worker_id.clone(),
        timestamp: 0, // Timestamp not strictly checked by current coordinator logic beyond presence
    };
    let hb_response = client
        .send_heartbeat(Request::new(heartbeat_info.clone()))
        .await
        .expect("SendHeartbeat RPC failed");
    assert!(hb_response.into_inner().ok);
    println!("Heartbeat successful for worker {}", worker_id);

    // Check last_seen updated (optional, but good)
    let initial_last_seen;
    {
        let state = test_cluster_state.lock().await;
        initial_last_seen = state.get(&worker_id).unwrap().last_seen;
    }
    sleep(StdDuration::from_secs(1)).await; // Ensure time progresses for timestamp change

    let hb_response_updated = client
        .send_heartbeat(Request::new(heartbeat_info.clone()))
        .await
        .expect("Second SendHeartbeat RPC failed (for Liveness check)");
    assert!(hb_response_updated.into_inner().ok); // Also ensure we check the response

    {
        let state = test_cluster_state.lock().await;
        let worker_state = state.get(&worker_id).expect("Worker disappeared prematurely");
        assert!(worker_state.last_seen > initial_last_seen, "Heartbeat did not update last_seen time");
        println!("Verified last_seen time updated for worker {}", worker_id);
    }


    // 6. Test Pruning
    // Use settings defined above: prune_interval_secs = 2, worker_timeout_secs = 5
    // Worker should be pruned after 5 seconds of inactivity, checked every 2 seconds.
    println!("Testing pruning for worker {}. Waiting for {}s timeout + {}s interval...", worker_id, settings.worker_timeout_secs, settings.prune_interval_secs);
    sleep(StdDuration::from_secs(settings.worker_timeout_secs + settings.prune_interval_secs + 1)).await; // Wait long enough for pruning

    {
        let state = test_cluster_state.lock().await;
        assert!(!state.contains_key(&worker_id), "Worker {} was not pruned", worker_id);
        println!("Worker {} successfully pruned from coordinator state.", worker_id);
    }
}
