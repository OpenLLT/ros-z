//! Unified driver loop for action server event handling.
//!
//! This module provides a single event loop that handles all server-side
//! action protocol events (goal requests, cancel requests, result requests)
//! in a sequential, race-condition-free manner.

use std::future::Future;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use zenoh::Wait;

use crate::attachment::Attachment;
use crate::msg::ZMessage;

use super::ZAction;
use super::messages::*;
use super::server::{ZActionServer, ExecutingGoal};

/// Runs a minimal driver loop that only handles result requests.
///
/// This allows manual handling of goals via recv_goal() while automatically
/// serving result requests.
pub async fn run_result_handler_loop<A: ZAction>(
    server: Arc<ZActionServer<A>>,
    shutdown: CancellationToken,
) {
    tracing::debug!("Action Server Result Handler Started");

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("Shutdown signal received. Stopping result handler.");
                break;
            }

            res = server.result_server.rx.recv_async() => {
                if let Ok(query) = res {
                    handle_result_request(&server, query).await;
                }
            }
        }
    }

    tracing::debug!("Action Server Result Handler Stopped");
}

/// Runs the unified driver loop for an action server with automatic goal handling.
///
/// This function consolidates all protocol logic into a single event loop,
/// eliminating race conditions and reducing task overhead.
///
/// # Arguments
///
/// * `server` - The action server to drive
/// * `shutdown` - Cancellation token to stop the driver loop
/// * `handler` - Callback to execute goals automatically
pub async fn run_driver_loop<A, F, Fut>(
    server: Arc<ZActionServer<A>>,
    shutdown: CancellationToken,
    handler: F,
) where
    A: ZAction,
    F: Fn(ExecutingGoal<A>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tracing::debug!("Action Server Driver Loop Started");

    loop {
        tokio::select! {
            // 1. Priority: Shutdown
            _ = shutdown.cancelled() => {
                tracing::info!("Shutdown signal received. Stopping driver.");
                break;
            }

            // 2. New Goal Requests
            res = server.goal_server.rx.recv_async() => {
                if let Ok(query) = res {
                    handle_goal_request(&server, query, &handler).await;
                }
            }

            // 3. Cancel Requests
            res = server.cancel_server.rx.recv_async() => {
                if let Ok(query) = res {
                    handle_cancel_request(&server, query).await;
                }
            }

            // 4. Result Requests
            res = server.result_server.rx.recv_async() => {
                if let Ok(query) = res {
                    handle_result_request(&server, query).await;
                }
            }
        }
    }

    tracing::debug!("Action Server Driver Loop Stopped");
}

/// Handles incoming goal requests.
async fn handle_goal_request<A, F, Fut>(
    server: &Arc<ZActionServer<A>>,
    query: zenoh::query::Query,
    handler: &F,
) where
    A: ZAction,
    F: Fn(ExecutingGoal<A>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tracing::debug!("Received goal request");
    let payload = query.payload().unwrap().to_bytes();
    let request = <GoalRequest<A> as ZMessage>::deserialize(&payload);

    let requested = super::server::RequestedGoal {
        goal: request.goal,
        info: super::GoalInfo::new(request.goal_id),
        server: Arc::clone(server),
        query,
    };

    let accepted = requested.accept();
    let executing = accepted.execute();

    // Spawn task to execute the goal
    let fut = handler(executing);
    tokio::spawn(fut);
}

/// Handles incoming cancel requests.
async fn handle_cancel_request<A: ZAction>(
    server: &Arc<ZActionServer<A>>,
    query: zenoh::query::Query,
) {
    tracing::debug!("Received cancel request");
    let payload = query.payload().unwrap().to_bytes();
    let request = <CancelGoalRequest as ZMessage>::deserialize(&payload);

    // Mark goal as canceling using the atomic flag
    let cancelled = server.request_cancel(request.goal_info.goal_id);

    // Send response
    let response = CancelGoalResponse {
        return_code: if cancelled { 1 } else { 0 },
        goals_canceling: if cancelled {
            vec![request.goal_info]
        } else {
            vec![]
        },
    };

    let response_bytes = <CancelGoalResponse as ZMessage>::serialize(&response);
    let attachment: Attachment = query.attachment().unwrap().try_into().unwrap();
    let _ = query.reply(query.key_expr().clone(), response_bytes)
        .attachment(attachment)
        .wait();

    tracing::debug!("Sent cancel response");
}

/// Handles incoming result requests.
async fn handle_result_request<A: ZAction>(
    server: &Arc<ZActionServer<A>>,
    query: zenoh::query::Query,
) {
    tracing::debug!("Received result request");
    let payload = query.payload().unwrap().to_bytes();
    let request = <ResultRequest as ZMessage>::deserialize(&payload);

    // Look up goal result - extract data while holding lock, then release
    let result_data = {
        let manager = server.goal_manager.lock().unwrap();
        if let Some(super::server::ServerGoalState::Terminated { result, status, .. }) =
            manager.goals.get(&request.goal_id)
        {
            Some((result.clone(), *status))
        } else {
            None
        }
    }; // Lock released here

    if let Some((result, status)) = result_data {
        tracing::debug!("Goal {:?} is terminated with status {:?}", request.goal_id, status);

        // Send result response without holding lock
        let response = ResultResponse::<A> {
            status,
            result,
        };
        let response_bytes = <ResultResponse<A> as ZMessage>::serialize(&response);
        let attachment: Attachment = query.attachment().unwrap().try_into().unwrap();
        let _ = query.reply(query.key_expr().clone(), response_bytes)
            .attachment(attachment)
            .wait();
        tracing::debug!("Sent result response");
    } else {
        tracing::warn!("Goal {:?} not found or not terminated yet", request.goal_id);
        // Server doesn't reply if goal is not ready yet
    }
}
