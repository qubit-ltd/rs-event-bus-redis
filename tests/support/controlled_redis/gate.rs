// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! One-shot synchronization point for an applied Redis reply.

use std::future::Future;
use std::pin::Pin;
use std::sync::Condvar;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

#[derive(Default)]
struct GateState {
    armed: bool,
    command: Option<Vec<u8>>,
    new_entries_only: bool,
    reached: bool,
    released: bool,
    discard_reply: bool,
    waker: Option<Waker>,
}

/// One-shot response gate for an already applied Redis command.
#[derive(Default)]
pub struct ReplyGate {
    state: Mutex<GateState>,
    changed: Condvar,
}

impl ReplyGate {
    /// Arms the next matching response.
    pub fn arm(&self) {
        self.arm_for_new_entries();
    }

    /// Arms the next response for `command` after Redis has applied it.
    pub fn arm_for(&self, command: &'static str) {
        let mut state = self.state.lock().expect("reply gate lock is healthy");
        state.armed = true;
        state.command = Some(command.as_bytes().to_ascii_uppercase());
        state.new_entries_only = false;
        state.reached = false;
        state.released = false;
        state.discard_reply = false;
        state.waker = None;
    }

    fn arm_for_new_entries(&self) {
        let mut state = self.state.lock().expect("reply gate lock is healthy");
        state.armed = true;
        state.command = Some(b"XREADGROUP".to_vec());
        state.new_entries_only = true;
        state.reached = false;
        state.released = false;
        state.discard_reply = false;
        state.waker = None;
    }

    /// Waits until Redis has returned the response for the selected command.
    pub fn wait_until_reached(&self, timeout: Duration) -> bool {
        let state = self.state.lock().expect("reply gate lock is healthy");
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| !state.reached)
            .expect("reply gate lock is healthy");
        state.reached
    }

    /// Waits without blocking the executor until the upstream reply is applied.
    pub fn wait_applied(&self) -> WaitApplied<'_> {
        WaitApplied { gate: self }
    }

    /// Allows the held response to continue to the client.
    pub fn release(&self) {
        let mut state = self.state.lock().expect("reply gate lock is healthy");
        state.discard_reply = false;
        state.released = true;
        self.changed.notify_all();
    }

    /// Closes the proxied client connection without delivering the held reply.
    pub fn release_without_reply(&self) {
        let mut state = self.state.lock().expect("reply gate lock is healthy");
        state.discard_reply = true;
        state.released = true;
        self.changed.notify_all();
    }

    pub(super) fn hold_if_armed(&self, command: &[u8], request: &[u8]) -> bool {
        let mut state = self.state.lock().expect("reply gate lock is healthy");
        if !state.armed || state.command.as_deref() != Some(command) {
            return false;
        }
        if state.new_entries_only && !request.contains(&b'>') {
            return false;
        }
        state.armed = false;
        state.reached = true;
        let waker = state.waker.take();
        self.changed.notify_all();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
        let state = self.state.lock().expect("reply gate lock is healthy");
        let _state = self
            .changed
            .wait_while(state, |state| !state.released)
            .expect("reply gate lock is healthy");
        _state.discard_reply
    }
}

/// Future returned by [`ReplyGate::wait_applied`].
pub struct WaitApplied<'a> {
    gate: &'a ReplyGate,
}

impl Future for WaitApplied<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.gate.state.lock().expect("reply gate lock is healthy");
        if state.reached {
            Poll::Ready(())
        } else {
            state.waker = Some(context.waker().clone());
            Poll::Pending
        }
    }
}
