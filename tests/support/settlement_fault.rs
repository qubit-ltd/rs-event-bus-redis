// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Observes the real Redis receiver without sharing or replacing its ownership.

use std::io::Error as IoError;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::PublishAcknowledgement;
#[cfg(feature = "async")]
use qubit_event_bus::spi::AsyncEventBusSpi;
#[cfg(feature = "async")]
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventBusCapabilities;
#[cfg(feature = "sync")]
use qubit_event_bus::spi::EventBusSpi;
#[cfg(feature = "sync")]
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
#[cfg(feature = "async")]
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_id::Id;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Attempt {
    pub token_address: usize,
    pub disposition: DeliveryDisposition,
    pub retryable_error: Option<bool>,
    pub succeeded: bool,
}

#[derive(Default)]
pub struct Observation {
    pub attempts: Mutex<Vec<Attempt>>,
    pub underlying_settles: AtomicUsize,
    pub closes: AtomicUsize,
}

impl Observation {
    pub fn settled(&self) -> bool {
        self.attempts
            .lock()
            .expect("settlement observations lock")
            .last()
            .is_some_and(|a| a.succeeded)
    }

    fn record(&self, address: usize, disposition: DeliveryDisposition, result: &Result<(), SpiError>) {
        self.attempts
            .lock()
            .expect("settlement observations lock")
            .push(Attempt {
                token_address: address,
                disposition,
                retryable_error: result.as_ref().err().and_then(SpiError::retryable),
                succeeded: result.is_ok(),
            });
    }
}

fn permanent_failure() -> SpiError {
    SpiError::Operation {
        provider_id: "redis-streams".into(),
        operation: "settle",
        resource: None,
        kind: "injected_before_xack",
        retryable: Some(false),
        source: Box::new(IoError::other("permanent failure before Redis settlement")),
    }
}

#[cfg(feature = "sync")]
pub struct SyncObservedBus {
    pub inner: Arc<dyn EventBusSpi>,
    pub observation: Arc<Observation>,
    pub fail_before_settle: bool,
}

#[cfg(feature = "sync")]
impl EventBusSpi for SyncObservedBus {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.inner.publish(message)
    }
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        let owner = request.subscription_id();
        Ok(Box::new(SyncObservedReceiver {
            inner: self.inner.subscribe(request)?,
            owner,
            observation: self.observation.clone(),
            fail_before_settle: self.fail_before_settle,
        }))
    }
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.inner.shutdown(mode)
    }
}

#[cfg(feature = "sync")]
struct SyncObservedReceiver {
    inner: Box<dyn EventSubscriptionSpi>,
    owner: Id,
    observation: Arc<Observation>,
    fail_before_settle: bool,
}

#[cfg(feature = "sync")]
impl EventSubscriptionSpi for SyncObservedReceiver {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        self.inner.receive(timeout)
    }
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError> {
        assert!(
            token.belongs_to(self.owner),
            "token stays with its original Redis receiver"
        );
        let result = if self.fail_before_settle {
            Err(permanent_failure())
        } else {
            self.observation.underlying_settles.fetch_add(1, Ordering::SeqCst);
            self.inner.settle(token, disposition)
        };
        self.observation
            .record(token as *const SettlementToken as usize, disposition, &result);
        result
    }
    fn close(&mut self) -> Result<(), SpiError> {
        self.inner.close()?;
        self.observation.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(feature = "async")]
pub struct AsyncObservedBus {
    pub inner: Arc<dyn AsyncEventBusSpi>,
    pub observation: Arc<Observation>,
    pub fail_before_settle: bool,
}

#[cfg(feature = "async")]
impl AsyncEventBusSpi for AsyncObservedBus {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.inner.publish(message)
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async move {
            let owner = request.subscription_id();
            let inner = self.inner.subscribe(request).await?;
            Ok(Box::new(AsyncObservedReceiver {
                inner,
                owner,
                observation: self.observation.clone(),
                fail_before_settle: self.fail_before_settle,
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

#[cfg(feature = "async")]
struct AsyncObservedReceiver {
    inner: Box<dyn AsyncEventSubscriptionSpi>,
    owner: Id,
    observation: Arc<Observation>,
    fail_before_settle: bool,
}

#[cfg(feature = "async")]
impl AsyncEventSubscriptionSpi for AsyncObservedReceiver {
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        self.inner.receive(timeout)
    }
    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        assert!(
            token.belongs_to(self.owner),
            "token stays with its original Redis receiver"
        );
        let address = token as *const SettlementToken as usize;
        // Derive owned observation data before returning a future, just like the SPI
        // contract.
        let future = if self.fail_before_settle {
            Box::pin(async { Err(permanent_failure()) }) as SpiFuture<'a, Result<(), SpiError>>
        } else {
            self.observation.underlying_settles.fetch_add(1, Ordering::SeqCst);
            self.inner.settle(token, disposition)
        };
        let observation = self.observation.clone();
        Box::pin(async move {
            let result = future.await;
            observation.record(address, disposition, &result);
            result
        })
    }
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        Box::pin(async move {
            self.inner.close().await?;
            self.observation.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}
