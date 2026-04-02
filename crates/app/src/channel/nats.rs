// SPDX-License-Identifier: MIT
// NATS channel adapter for klawper workflow dispatch.
// Subscribes to NATS subjects, receives workflow step dispatch messages,
// and routes them as inbound messages to loongclaw agents.

use async_trait::async_trait;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

use super::types::{
    ChannelAdapter, ChannelDelivery, ChannelInboundMessage, ChannelOutboundMessage,
    ChannelOutboundTarget, ChannelOutboundTargetKind, ChannelPlatform, ChannelSession,
    ChannelStreamingMode,
};
use crate::CliResult;

/// Dispatch payload from klawper's WorkflowRun controller.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StepDispatchPayload {
    run_name: String,
    step_name: String,
    #[allow(dead_code)]
    agent_role: String,
    #[allow(dead_code)]
    model: String,
    #[allow(dead_code)]
    provider: Option<String>,
    #[allow(dead_code)]
    endpoint: Option<String>,
    input: String,
    #[allow(dead_code)]
    expected_outputs: Vec<String>,
    #[allow(dead_code)]
    timeout: String,
}

/// Result payload sent back to the controller.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StepResultPayload {
    run_name: String,
    step_name: String,
    status: String,
    output: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    duration_ms: i64,
}

pub(super) struct NatsAdapter {
    client: async_nats::Client,
    subscriber: Option<async_nats::Subscriber>,
    #[allow(dead_code)]
    workspace: String,
    #[allow(dead_code)]
    subject_prefix: String,
    #[allow(dead_code)]
    pending_results: Arc<Mutex<Vec<(String, StepResultPayload)>>>,
}

impl NatsAdapter {
    pub(super) async fn new(url: &str, workspace: &str) -> CliResult<Self> {
        let client = async_nats::connect(url)
            .await
            .map_err(|e| format!("failed to connect to NATS at {url}: {e}"))?;

        let subject_prefix = format!("klawper.workspace.{workspace}.workflow");
        let subscribe_subject = format!("{subject_prefix}.*.step.*.dispatch");

        let subscriber = client
            .subscribe(subscribe_subject.clone())
            .await
            .map_err(|e| format!("failed to subscribe to {subscribe_subject}: {e}"))?;

        Ok(Self {
            client,
            subscriber: Some(subscriber),
            workspace: workspace.to_owned(),
            subject_prefix,
            pending_results: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

#[async_trait]
impl ChannelAdapter for NatsAdapter {
    fn name(&self) -> &str {
        "nats"
    }

    async fn receive_batch(&mut self) -> CliResult<Vec<ChannelInboundMessage>> {
        let subscriber = self
            .subscriber
            .as_mut()
            .ok_or_else(|| "NATS subscriber not initialized".to_string())?;

        let mut messages = Vec::new();

        // Non-blocking drain of available messages (up to 10 per batch).
        for _ in 0..10 {
            match tokio::time::timeout(std::time::Duration::from_millis(100), subscriber.next())
                .await
            {
                Ok(Some(msg)) => {
                    let payload: StepDispatchPayload = serde_json::from_slice(&msg.payload)
                        .map_err(|e| format!("invalid dispatch payload: {e}"))?;

                    // Build the result subject for replies.
                    let result_subject = msg.subject.as_str().replace(".dispatch", ".result");

                    let session = ChannelSession::new(
                        ChannelPlatform::Nats,
                        format!("{}.{}", payload.run_name, payload.step_name),
                    );

                    let reply_target = ChannelOutboundTarget::new(
                        ChannelPlatform::Nats,
                        ChannelOutboundTargetKind::Endpoint,
                        result_subject,
                    );

                    messages.push(ChannelInboundMessage {
                        session,
                        reply_target,
                        text: payload.input,
                        delivery: ChannelDelivery {
                            source_message_id: Some(format!(
                                "{}/{}",
                                payload.run_name, payload.step_name
                            )),
                            ..Default::default()
                        },
                    });
                }
                Ok(None) => break, // Subscriber closed
                Err(_) => break,   // Timeout — no more messages
            }
        }

        Ok(messages)
    }

    async fn send_message(
        &self,
        target: &ChannelOutboundTarget,
        message: &ChannelOutboundMessage,
    ) -> CliResult<()> {
        if target.platform != ChannelPlatform::Nats {
            return Err(format!(
                "NATS adapter cannot send to platform {:?}",
                target.platform
            ));
        }

        let text = match message {
            ChannelOutboundMessage::Text(t) => t.clone(),
            ChannelOutboundMessage::MarkdownCard(t) => t.clone(),
            ChannelOutboundMessage::Post(_)
            | ChannelOutboundMessage::Image { .. }
            | ChannelOutboundMessage::File { .. } => {
                return Err("NATS adapter only supports text messages".to_string());
            }
        };

        // The target.id is the NATS result subject.
        let subject = &target.id;

        let result = StepResultPayload {
            run_name: String::new(),
            step_name: String::new(),
            status: "completed".to_string(),
            output: text,
            error: None,
            duration_ms: 0,
        };

        let data =
            serde_json::to_vec(&result).map_err(|e| format!("failed to serialize result: {e}"))?;

        self.client
            .publish(subject.to_string(), data.into())
            .await
            .map_err(|e| format!("failed to publish to {subject}: {e}"))?;

        Ok(())
    }
}
