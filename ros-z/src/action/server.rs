//! Action server implementation for ROS 2 actions.
//!
//! This module provides the server-side functionality for ROS 2 actions,
//! allowing nodes to accept goals from action clients, execute them,
//! provide feedback, and return results.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use zenoh::{Result, Wait};

use crate::attachment::Attachment;
use crate::msg::ZMessage;
use crate::topic_name::qualify_topic_name;
use crate::{Builder};

use super::ZAction;
use super::messages::*;
use super::{GoalId, GoalInfo, GoalStatus};
use super::state::{SafeGoalManager, ServerGoalState};

/// Builder for creating an action server.
///
/// The `ZActionServerBuilder` allows you to configure timeouts and QoS settings
/// for different action communication channels before building the server.
///
/// # Examples
///
/// ```no_run
/// # use ros_z::action::*;
/// # use std::time::Duration;
/// # let node = todo!();
/// let server = node.create_action_server::<MyAction>("my_action")
///     .with_result_timeout(Duration::from_secs(30))
///     .build()?;
/// # Ok::<(), zenoh::Error>(())
/// ```
pub struct ZActionServerBuilder<'a, A: ZAction> {
    /// The name of the action.
    pub action_name: String,
    /// Reference to the node that will own this server.
    pub node: &'a crate::node::ZNode,
    /// Timeout for result requests.
    pub result_timeout: Duration,
    /// Optional timeout for goal execution.
    pub goal_timeout: Option<Duration>,
    /// QoS profile for the goal service.
    pub goal_service_qos: Option<crate::qos::QosProfile>,
    /// QoS profile for the result service.
    pub result_service_qos: Option<crate::qos::QosProfile>,
    /// QoS profile for the cancel service.
    pub cancel_service_qos: Option<crate::qos::QosProfile>,
    /// QoS profile for the feedback topic.
    pub feedback_topic_qos: Option<crate::qos::QosProfile>,
    /// QoS profile for the status topic.
    pub status_topic_qos: Option<crate::qos::QosProfile>,
    pub _phantom: std::marker::PhantomData<A>,
}

impl<'a, A: ZAction> ZActionServerBuilder<'a, A> {
    pub fn new(action_name: &str, node: &'a crate::node::ZNode) -> Self {
        Self {
            action_name: action_name.to_string(),
            node,
            result_timeout: Duration::from_secs(10),
            goal_timeout: None,
            goal_service_qos: None,
            result_service_qos: None,
            cancel_service_qos: None,
            feedback_topic_qos: None,
            status_topic_qos: None,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn result_timeout(mut self, timeout: Duration) -> Self {
        self.result_timeout = timeout;
        self
    }

    pub fn goal_timeout(mut self, timeout: Duration) -> Self {
        self.goal_timeout = Some(timeout);
        self
    }

    pub fn with_goal_service_qos(mut self, qos: crate::qos::QosProfile) -> Self {
        self.goal_service_qos = Some(qos);
        self
    }

    pub fn with_result_service_qos(mut self, qos: crate::qos::QosProfile) -> Self {
        self.result_service_qos = Some(qos);
        self
    }

    pub fn with_cancel_service_qos(mut self, qos: crate::qos::QosProfile) -> Self {
        self.cancel_service_qos = Some(qos);
        self
    }

    pub fn with_feedback_topic_qos(mut self, qos: crate::qos::QosProfile) -> Self {
        self.feedback_topic_qos = Some(qos);
        self
    }

    pub fn with_status_topic_qos(mut self, qos: crate::qos::QosProfile) -> Self {
        self.status_topic_qos = Some(qos);
        self
    }
}

// Legacy result handler to preserve original behavior
async fn handle_result_requests_legacy<A: ZAction>(
    result_server: Arc<crate::service::ZServer<ResultService<A>>>,
    goal_manager: Arc<SafeGoalManager<A>>,
) {
    loop {
        if let Ok(query) = result_server.rx.recv_async().await {
            tracing::debug!("Received result request");
            let payload = query.payload().unwrap().to_bytes();
            let request = <ResultRequest as ZMessage>::deserialize(&payload);

            // Look up goal result - extract data while holding lock, then release
            let result_data = goal_manager.read(|manager| {
                if let Some(ServerGoalState::Terminated { result, status, .. }) =
                    manager.goals.get(&request.goal_id)
                {
                    Some((result.clone(), *status))
                } else {
                    None
                }
            }); // Lock released here

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
    }
}

impl<'a, A: ZAction> Builder for ZActionServerBuilder<'a, A> {
    type Output = Arc<ZActionServer<A>>;

    fn build(self) -> Result<Self::Output> {
        // Apply remapping to action name
        let action_name = self.node.remap_rules.apply(&self.action_name);

        // Validate action name
        if action_name.is_empty() {
            return Err(zenoh::Error::from("Action name cannot be empty"));
        }

        // Qualify action name like a topic name
        let qualified_action_name = qualify_topic_name(&action_name, &self.node.entity.namespace, &self.node.entity.name)?;

        // ROS 2 action naming conventions
        let goal_service_name = format!("{}/_action/send_goal", qualified_action_name);
        let result_service_name = format!("{}/_action/get_result", qualified_action_name);
        let cancel_service_name = format!("{}/_action/cancel_goal", qualified_action_name);
        let feedback_topic_name = format!("{}/_action/feedback", qualified_action_name);
        let status_topic_name = format!("{}/_action/status", qualified_action_name);

        // Create goal server using node API for proper graph registration
        let mut goal_server_builder = self.node.create_service_impl::<GoalService<A>>(&goal_service_name, None);
        if let Some(qos) = self.goal_service_qos {
            goal_server_builder.entity.qos = qos;
        }
        let goal_server = goal_server_builder.build()?;

        // Create result server using node API for proper graph registration
        let mut result_server_builder = self.node.create_service_impl::<ResultService<A>>(&result_service_name, None);
        if let Some(qos) = self.result_service_qos {
            result_server_builder.entity.qos = qos;
        }
        let result_server = result_server_builder.build()?;
        tracing::debug!("Created result server for: {}", result_service_name);

        // Create cancel server using node API for proper graph registration
        let mut cancel_server_builder = self.node.create_service_impl::<CancelService>(&cancel_service_name, None);
        if let Some(qos) = self.cancel_service_qos {
            cancel_server_builder.entity.qos = qos;
        }
        let cancel_server = cancel_server_builder.build()?;

        // Create feedback publisher using node API for proper graph registration
        // Use action name as type info for graph introspection
        let feedback_type_info = Some(crate::entity::TypeInfo::new(
            &format!("{}/_FeedbackMessage", A::name()),
            crate::entity::TypeHash::zero(),
        ));
        let mut feedback_pub_builder = self.node.create_pub_impl::<FeedbackMessage<A>>(&feedback_topic_name, feedback_type_info);
        if let Some(qos) = self.feedback_topic_qos {
            feedback_pub_builder.entity.qos = qos;
        }
        feedback_pub_builder.with_attachment = false;
        let feedback_pub = feedback_pub_builder.build()?;

        // Create status publisher using node API for proper graph registration
        let mut status_pub_builder = self.node.create_pub_impl::<StatusMessage>(&status_topic_name, None);
        if let Some(qos) = self.status_topic_qos {
            status_pub_builder.entity.qos = qos;
        }
        status_pub_builder.with_attachment = false;
        let status_pub = status_pub_builder.build()?;

        let goal_manager = Arc::new(SafeGoalManager::new(
            self.result_timeout,
            self.goal_timeout,
        ));

        let cancellation_token = CancellationToken::new();
        let result_handler_token = CancellationToken::new();

        // Spawn background task to handle result requests (default mode for manual goal handling)
        // This task will be cancelled if with_handler() is called
        let result_server_arc = Arc::new(result_server);
        let result_server_clone = result_server_arc.clone();
        let goal_manager_clone = goal_manager.clone();
        let global_shutdown = cancellation_token.clone();
        let handler_token = result_handler_token.clone();

        tokio::spawn(async move {
            // Run until EITHER global shutdown OR handler-specific cancellation
            tokio::select! {
                _ = global_shutdown.cancelled() => {
                    tracing::debug!("Result handler stopping due to global shutdown");
                },
                _ = handler_token.cancelled() => {
                    tracing::debug!("Result handler stopping - switching to full driver mode");
                },
                _ = handle_result_requests_legacy::<A>(result_server_clone, goal_manager_clone) => {},
            }
        });

        // TODO: Add background task for goal expiration checking

        Ok(Arc::new(ZActionServer {
            goal_server: Arc::new(goal_server),
            result_server: result_server_arc,
            cancel_server: Arc::new(cancel_server),
            feedback_pub: Arc::new(feedback_pub),
            status_pub: Arc::new(status_pub),
            goal_manager,
            _cancellation_token: cancellation_token,
            result_handler_token,
        }))
    }
}

pub struct ZActionServer<A: ZAction> {
    pub(crate) goal_server: Arc<crate::service::ZServer<GoalService<A>>>,
    pub(crate) result_server: Arc<crate::service::ZServer<ResultService<A>>>,
    pub(crate) cancel_server: Arc<crate::service::ZServer<CancelService>>,
    pub(crate) feedback_pub: Arc<crate::pubsub::ZPub<FeedbackMessage<A>, <FeedbackMessage<A> as ZMessage>::Serdes>>,
    pub(crate) status_pub: Arc<crate::pubsub::ZPub<StatusMessage, <StatusMessage as ZMessage>::Serdes>>,
    pub(crate) goal_manager: Arc<SafeGoalManager<A>>,
    _cancellation_token: CancellationToken,
    /// Token to cancel the default result handler when switching to full driver mode
    result_handler_token: CancellationToken,
}

impl<A: ZAction> Clone for ZActionServer<A> {
    fn clone(&self) -> Self {
        Self {
            goal_server: self.goal_server.clone(),
            result_server: self.result_server.clone(),
            cancel_server: self.cancel_server.clone(),
            feedback_pub: self.feedback_pub.clone(),
            status_pub: self.status_pub.clone(),
            goal_manager: self.goal_manager.clone(),
            _cancellation_token: self._cancellation_token.clone(),
            result_handler_token: self.result_handler_token.clone(),
        }
    }
}

impl<A: ZAction> Drop for ZActionServer<A> {
    fn drop(&mut self) {
        self._cancellation_token.cancel();
    }
}

impl<A: ZAction> ZActionServer<A> {
    fn publish_status(&self) {
        // Build status list while holding lock, then release before publishing
        let status_list: Vec<GoalStatusInfo> = self.goal_manager.read(|manager| {
            manager.goals.iter().map(|(goal_id, state)| {
                let status = match state {
                    ServerGoalState::Accepted { .. } => GoalStatus::Accepted,
                    ServerGoalState::Executing { .. } => GoalStatus::Executing,
                    ServerGoalState::Canceling { .. } => GoalStatus::Canceling,
                    ServerGoalState::Terminated { status, .. } => *status,
                };
                GoalStatusInfo {
                    goal_info: GoalInfo::new(*goal_id),
                    status,
                }
            }).collect()
        }); // Lock released here

        // Publish without holding lock
        let msg = StatusMessage { status_list };
        let _ = self.status_pub.publish(&msg);
    }

    pub async fn recv_goal(self: &Arc<Self>) -> Result<GoalHandle<A, Requested>> {
        let query = self.goal_server.rx.recv_async().await?;
        let payload = query.payload().unwrap().to_bytes();
        let request = <GoalRequest<A> as ZMessage>::deserialize(&payload);

        Ok(GoalHandle {
            goal: request.goal,
            info: GoalInfo::new(request.goal_id),
            server: Arc::clone(self),
            query: Some(query),
            cancel_flag: None,
            _state: PhantomData,
        })
    }

    pub async fn recv_cancel(&self) -> Result<(CancelGoalRequest, zenoh::query::Query)> {
        let query = self.cancel_server.rx.recv_async().await?;
        let payload = query.payload().unwrap().to_bytes();
        let request = <CancelGoalRequest as ZMessage>::deserialize(&payload);
        Ok((request, query))
    }

    pub fn is_cancel_request_ready(&self) -> bool {
        !self.cancel_server.rx.is_empty()
    }

    /// Marks a goal as canceling by setting its atomic cancel flag.
    /// This is a lock-free operation that can be called from any thread.
    pub fn request_cancel(&self, goal_id: GoalId) -> bool {
        self.goal_manager.read(|manager| {
            if let Some(ServerGoalState::Executing { cancel_flag, .. }) = manager.goals.get(&goal_id) {
                cancel_flag.store(true, Ordering::Relaxed);
                true
            } else {
                false
            }
        })
    }

    pub async fn recv_result_request(&self) -> Result<(GoalId, zenoh::query::Query)> {
        let query = self.result_server.rx.recv_async().await?;
        let payload = query.payload().unwrap().to_bytes();
        let request = <ResultRequest as ZMessage>::deserialize(&payload);
        Ok((request.goal_id, query))
    }

    // Low-level methods for testing
    pub async fn recv_goal_request_low(&self) -> Result<(super::messages::GoalRequest<A>, zenoh::query::Query)> {
        let query = self.goal_server.rx.recv_async().await?;
        let payload = query.payload().unwrap().to_bytes();
        let request = <super::messages::GoalRequest<A> as ZMessage>::deserialize(&payload);
        Ok((request, query))
    }

    pub fn send_goal_response_low(&self, query: &zenoh::query::Query, response: &super::messages::GoalResponse) -> Result<()> {
        let response_bytes = <super::messages::GoalResponse as ZMessage>::serialize(response);
        let attachment: Attachment = query.attachment().unwrap().try_into().unwrap();
        let _ = query.reply(query.key_expr().clone(), response_bytes)
            .attachment(attachment)
            .wait();
        Ok(())
    }

    pub async fn recv_cancel_request_low(&self) -> Result<(super::messages::CancelGoalRequest, zenoh::query::Query)> {
        let query = self.cancel_server.rx.recv_async().await?;
        let payload = query.payload().unwrap().to_bytes();
        let request = <super::messages::CancelGoalRequest as ZMessage>::deserialize(&payload);
        Ok((request, query))
    }

    pub fn send_cancel_response_low(&self, query: &zenoh::query::Query, response: &super::messages::CancelGoalResponse) -> Result<()> {
        let response_bytes = <super::messages::CancelGoalResponse as ZMessage>::serialize(response);
        let attachment: Attachment = query.attachment().unwrap().try_into().unwrap();
        let _ = query.reply(query.key_expr().clone(), response_bytes)
            .attachment(attachment)
            .wait();
        Ok(())
    }

    pub fn send_result_response_low(&self, query: &zenoh::query::Query, response: &super::messages::ResultResponse<A>) -> Result<()> {
        let response_bytes = <super::messages::ResultResponse<A> as ZMessage>::serialize(response);
        let attachment: Attachment = query.attachment().unwrap().try_into().unwrap();
        let _ = query.reply(query.key_expr().clone(), response_bytes)
            .attachment(attachment)
            .wait();
        Ok(())
    }

    /// Attaches an automatic goal handler to the server.
    ///
    /// This method transitions the server from "manual mode" (where you call `recv_goal()`)
    /// to "automatic mode" (where goals are handled by the provided callback).
    ///
    /// **Important**: This method cancels the default result-only handler and starts a full
    /// driver loop that handles all protocol events (goals, cancels, results) automatically.
    ///
    /// # Arguments
    ///
    /// * `handler` - Callback function that will be invoked for each accepted goal
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ros_z::action::*;
    /// # let server = todo!();
    /// let _server = server.with_handler(|executing| async move {
    ///     // Process the goal
    ///     executing.succeed(result).unwrap();
    /// });
    /// ```
    pub fn with_handler<F, Fut>(self: Arc<Self>, handler: F) -> Arc<Self>
    where
        F: Fn(GoalHandle<A, Executing>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        // 1. Stop the default result-only handler to avoid competing for result_server.rx
        tracing::debug!("Cancelling default result handler to switch to full driver mode");
        self.result_handler_token.cancel();

        // 2. Start the full driver loop that handles all protocol events
        let server_clone = self.clone();
        let shutdown = self._cancellation_token.clone();
        tokio::spawn(async move {
            crate::action::driver::run_driver_loop(server_clone, shutdown, handler).await;
        });

        self
    }

    /// Expires terminated goals that are older than the specified maximum age.
    ///
    /// This method removes terminated goals (succeeded, aborted, or canceled)
    /// from the goal manager if they have been in a terminal state longer
    /// than `max_age`. This prevents memory leaks in long-running servers.
    ///
    /// # Arguments
    ///
    /// * `max_age` - The maximum age for terminated goals before expiration
    ///
    /// # Returns
    ///
    /// Returns a vector of `GoalId`s that were expired.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ros_z::action::*;
    /// # use std::time::Duration;
    /// # let server: ros_z::action::server::ZActionServer<MyAction> = todo!();
    /// // Expire goals older than 10 seconds
    /// let expired = server.expire_goals(Duration::from_secs(10));
    /// println!("Expired {} goals", expired.len());
    /// ```
    pub fn expire_goals(&self, max_age: Duration) -> Vec<GoalId> {
        let expired = self.goal_manager.modify(|manager| {
            let now = Instant::now();
            let mut expired = Vec::new();

            // Find terminated goals older than max_age
            manager.goals.retain(|goal_id, state| {
                if let ServerGoalState::Terminated { timestamp, .. } = state
                    && now.duration_since(*timestamp) > max_age {
                        expired.push(*goal_id);
                        return false; // Remove this goal
                    }
                true // Keep this goal
            });

            expired
        }); // Lock released here

        // Publish updated status if any goals were expired
        if !expired.is_empty() {
            self.publish_status();
        }

        expired
    }

    /// Sets the result timeout for this server.
    ///
    /// This configures how long the server will keep terminated goals
    /// before they can be expired. Note: This does not automatically
    /// expire goals - you must call `expire_goals()` periodically.
    ///
    /// # Arguments
    ///
    /// * `timeout` - The result timeout duration
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ros_z::action::*;
    /// # use std::time::Duration;
    /// # let mut server: ros_z::action::server::ZActionServer<MyAction> = todo!();
    /// server.set_result_timeout(Duration::from_secs(30));
    /// ```
    pub fn set_result_timeout(&self, timeout: Duration) {
        self.goal_manager.modify(|manager| {
            manager.result_timeout = timeout;
        });
    }

    /// Gets the current result timeout for this server.
    ///
    /// # Returns
    ///
    /// The result timeout duration
    pub fn result_timeout(&self) -> Duration {
        self.goal_manager.read(|manager| manager.result_timeout)
    }
}

// --- State Markers for Type-State Pattern ---
/// Marker type representing a goal that has been requested but not yet accepted or rejected.
pub struct Requested;

/// Marker type representing a goal that has been accepted but not yet executing.
pub struct Accepted;

/// Marker type representing a goal that is currently executing.
pub struct Executing;

// Type aliases for convenience
/// A goal handle in the "Requested" state.
pub type RequestedGoal<A> = GoalHandle<A, Requested>;

/// A goal handle in the "Accepted" state.
pub type AcceptedGoal<A> = GoalHandle<A, Accepted>;

/// A goal handle in the "Executing" state.
pub type ExecutingGoal<A> = GoalHandle<A, Executing>;

// Type-state pattern for goal lifecycle with PhantomData markers
/// A type-safe goal handle that uses compile-time state tracking.
///
/// The `GoalHandle` is generic over the action type `A` and the state `State`.
/// Different methods are available depending on the current state, enforced at compile time.
///
/// # Type States
///
/// - `GoalHandle<A, Requested>`: Can be accepted or rejected
/// - `GoalHandle<A, Accepted>`: Can be executed
/// - `GoalHandle<A, Executing>`: Can publish feedback and be terminated
///
/// # Examples
///
/// ```no_run
/// # use ros_z::action::*;
/// # let server: std::sync::Arc<server::ZActionServer<MyAction>> = todo!();
/// # async {
/// let requested = server.recv_goal().await?;
/// let accepted = requested.accept();
/// let executing = accepted.execute();
/// executing.succeed(result)?;
/// # Ok::<(), zenoh::Error>(())
/// # };
/// ```
pub struct GoalHandle<A: ZAction, State> {
    /// The goal data.
    pub goal: A::Goal,
    /// The goal metadata.
    pub info: GoalInfo,
    pub(crate) server: Arc<ZActionServer<A>>,
    pub(crate) query: Option<zenoh::query::Query>,
    pub(crate) cancel_flag: Option<Arc<AtomicBool>>,
    pub(crate) _state: PhantomData<State>,
}

// --- State-specific implementations ---

/// Methods available only for goals in the "Requested" state.
impl<A: ZAction> GoalHandle<A, Requested> {
    /// Access the goal data.
    pub fn goal(&self) -> &A::Goal {
        &self.goal
    }

    /// Access the goal info.
    pub fn info(&self) -> &GoalInfo {
        &self.info
    }

    /// Accept this goal and transition to the "Accepted" state.
    ///
    /// This sends an acceptance response to the client and updates the server state.
    pub fn accept(mut self) -> GoalHandle<A, Accepted> {
        // Send acceptance response
        let response = GoalResponse { accepted: true, stamp: self.info.stamp };
        let response_bytes = <GoalResponse as ZMessage>::serialize(&response);

        if let Some(query) = self.query.take() {
            let attachment: Attachment = query.attachment().unwrap().try_into().unwrap();
            let _ = query.reply(query.key_expr().clone(), response_bytes)
                .attachment(attachment)
                .wait();
        }

        // Update server state to ACCEPTED
        self.server.goal_manager.modify(|manager| {
            let expires_at = manager.goal_timeout.map(|timeout| Instant::now() + timeout);
            manager.goals.insert(
                self.info.goal_id,
                ServerGoalState::Accepted {
                    goal: self.goal.clone(),
                    timestamp: Instant::now(),
                    expires_at,
                },
            );
        });

        // Publish status update
        self.server.publish_status();

        GoalHandle {
            goal: self.goal,
            info: self.info,
            server: self.server,
            query: None,
            cancel_flag: None,
            _state: PhantomData,
        }
    }

    /// Reject this goal.
    ///
    /// This sends a rejection response to the client. The goal will not be executed.
    pub fn reject(mut self) -> Result<()> {
        // Send rejection response
        let response = GoalResponse { accepted: false, stamp: 0 };
        let response_bytes = <GoalResponse as ZMessage>::serialize(&response);

        if let Some(query) = self.query.take() {
            let attachment: Attachment = query.attachment().unwrap().try_into().unwrap();
            let _ = query.reply(query.key_expr().clone(), response_bytes)
                .attachment(attachment)
                .wait();
        }
        Ok(())
    }
}

/// Methods available only for goals in the "Accepted" state.
impl<A: ZAction> GoalHandle<A, Accepted> {
    /// Access the goal data.
    pub fn goal(&self) -> &A::Goal {
        &self.goal
    }

    /// Access the goal info.
    pub fn info(&self) -> &GoalInfo {
        &self.info
    }

    /// Begin executing this goal and transition to the "Executing" state.
    ///
    /// This updates the server state to executing and publishes a status update.
    pub fn execute(self) -> GoalHandle<A, Executing> {
        // Create cancel flag
        let cancel_flag = Arc::new(AtomicBool::new(false));

        // Transition to EXECUTING
        self.server.goal_manager.modify(|manager| {
            let expires_at = manager.goal_timeout.map(|timeout| Instant::now() + timeout);
            manager.goals.insert(
                self.info.goal_id,
                ServerGoalState::Executing {
                    goal: self.goal.clone(),
                    cancel_flag: cancel_flag.clone(),
                    expires_at,
                },
            );
        });

        self.server.publish_status();

        GoalHandle {
            goal: self.goal,
            info: self.info,
            server: self.server,
            query: None,
            cancel_flag: Some(cancel_flag),
            _state: PhantomData,
        }
    }
}

/// Methods available only for goals in the "Executing" state.
impl<A: ZAction> GoalHandle<A, Executing> {
    /// Access the goal data.
    pub fn goal(&self) -> &A::Goal {
        &self.goal
    }

    /// Access the goal info.
    pub fn info(&self) -> &GoalInfo {
        &self.info
    }

    /// Publish feedback for this goal.
    ///
    /// Feedback can be published multiple times during goal execution to inform
    /// the client of progress.
    pub fn publish_feedback(&self, feedback: A::Feedback) -> Result<()> {
        let msg = FeedbackMessage {
            goal_id: self.info.goal_id,
            feedback,
        };
        self.server.feedback_pub.publish(&msg)
    }

    /// Check if cancellation has been requested for this goal.
    ///
    /// This is a lock-free operation that can be called frequently from the
    /// goal execution loop.
    ///
    /// # Returns
    ///
    /// `true` if a cancel request has been received, `false` otherwise.
    pub fn is_cancel_requested(&self) -> bool {
        self.cancel_flag.as_ref()
            .map(|flag| flag.load(Ordering::Relaxed))
            .unwrap_or(false)
    }

    /// Mark this goal as succeeded with the given result.
    ///
    /// This transitions the goal to a terminal state and consumes the handle.
    pub fn succeed(self, result: A::Result) -> Result<()> {
        self.terminate(result, GoalStatus::Succeeded)
    }

    /// Mark this goal as aborted with the given result.
    ///
    /// This transitions the goal to a terminal state and consumes the handle.
    pub fn abort(self, result: A::Result) -> Result<()> {
        self.terminate(result, GoalStatus::Aborted)
    }

    /// Mark this goal as canceled with the given result.
    ///
    /// This transitions the goal to a terminal state and consumes the handle.
    pub fn canceled(self, result: A::Result) -> Result<()> {
        self.terminate(result, GoalStatus::Canceled)
    }

    fn terminate(self, result: A::Result, status: GoalStatus) -> Result<()> {
        self.server.goal_manager.modify(|manager| {
            manager.goals.insert(
                self.info.goal_id,
                ServerGoalState::Terminated {
                    result,
                    status,
                    timestamp: Instant::now(),
                },
            );
        }); // Drop the lock before publishing status
        self.server.publish_status();
        Ok(())
    }
}
