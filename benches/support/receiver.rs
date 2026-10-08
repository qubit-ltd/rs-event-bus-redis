// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Exclusive receiver ownership, polling, and acceptance.

use std::error::Error;
use std::hint::black_box;
use std::time::Duration;

use futures_lite::future::block_on;
use qubit_event_bus::model::EventId;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::ReceiveOutcome;

/// Receiver ownership remains exclusive to its worker.
pub enum Receiver {
    Sync(Box<dyn EventSubscriptionSpi>),
    Async(Box<dyn AsyncEventSubscriptionSpi>),
}

impl Receiver {
    /// Accepts the acknowledged event; recovery can retry receive without
    /// republishing.
    pub fn receive_accept(&mut self, expected_id: &EventId) -> String {
        match self {
            Self::Sync(receiver) => match receiver.receive(Duration::from_secs(3)) {
                Ok(ReceiveOutcome::Message(delivery)) => {
                    if delivery.id() != expected_id {
                        return "event_id_mismatch".into();
                    }
                    black_box(&delivery);
                    match delivery.settlement() {
                        Some(token) => receiver
                            .settle(token, DeliveryDisposition::Accept)
                            .map_or_else(
                                |error| format!("{}:{}", error.operation(), error.kind()),
                                |_| "ok".into(),
                            ),
                        None => "missing_token".into(),
                    }
                }
                Ok(_) => "receive_not_message".into(),
                Err(error) => format!("{}:{}", error.operation(), error.kind()),
            },
            Self::Async(receiver) => block_on(async {
                match receiver.receive(Duration::from_secs(3)).await {
                    Ok(ReceiveOutcome::Message(delivery)) => {
                        if delivery.id() != expected_id {
                            return "event_id_mismatch".into();
                        }
                        black_box(&delivery);
                        match delivery.settlement() {
                            Some(token) => receiver
                                .settle(token, DeliveryDisposition::Accept)
                                .await
                                .map_or_else(
                                    |error| format!("{}:{}", error.operation(), error.kind()),
                                    |_| "ok".into(),
                                ),
                            None => "missing_token".into(),
                        }
                    }
                    Ok(_) => "receive_not_message".into(),
                    Err(error) => format!("{}:{}", error.operation(), error.kind()),
                }
            }),
        }
    }

    /// Measures a one-millisecond idle receive without publishing.
    pub fn poll(&mut self) -> String {
        match self {
            Self::Sync(receiver) => match receiver.receive(Duration::from_millis(1)) {
                Ok(ReceiveOutcome::TimedOut) => "timed_out".into(),
                Ok(_) => "unexpected_idle_result".into(),
                Err(error) => format!("{}:{}", error.operation(), error.kind()),
            },
            Self::Async(receiver) => block_on(async {
                match receiver.receive(Duration::from_millis(1)).await {
                    Ok(ReceiveOutcome::TimedOut) => "timed_out".into(),
                    Ok(_) => "unexpected_idle_result".into(),
                    Err(error) => format!("{}:{}", error.operation(), error.kind()),
                }
            }),
        }
    }

    /// Closes resources after the final observed active-client snapshot.
    pub fn close(&mut self) -> Result<(), Box<dyn Error>> {
        match self {
            Self::Sync(receiver) => Ok(receiver.close()?),
            Self::Async(receiver) => Ok(block_on(receiver.close())?),
        }
    }
}
