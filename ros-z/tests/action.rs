// Action-related integration tests
// This module organizes all action-related tests into a single integration test
// Tests ported from rcl_action/test/rcl_action

mod action {
    mod client;
    mod communication;
    mod goal_handle;
    mod goal_state_machine;
    mod graph;
    mod interaction;
    mod names;
    mod remapping;
    mod server;
    mod types;
    mod wait;
}
