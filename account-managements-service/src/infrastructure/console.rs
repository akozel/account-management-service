//! Development console output adapters.

use std::io::{self, Write};

use crate::application::email_reservation::tasks::send_verification_code::{VerificationCodeDelivery, VerificationCodeSender};
use async_trait::async_trait;
use cqrs_runtime::task::{data::attempt::TaskAttempt, handling::handler::TaskHandlerError};

/// Development adapter: the only delivery side effect is a line on stdout.
pub struct ConsoleVerificationCodeSender<W = io::Stdout> {
    writer: W,
}

impl<W> ConsoleVerificationCodeSender<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }
}

#[async_trait]
impl<W: Write + Send> VerificationCodeSender for ConsoleVerificationCodeSender<W> {
    async fn send(&mut self, message: VerificationCodeDelivery, delivery: &TaskAttempt) -> Result<(), TaskHandlerError> {
        writeln!(
            self.writer,
            "Verification code {} for account_id {} sent to {}; task_id={}; attempt={}",
            message.verification_code,
            message.account_id,
            message.email,
            delivery.id(),
            delivery.number()
        )
        .and_then(|()| self.writer.flush())
        .map_err(|_| TaskHandlerError::Retry {
            reason: "console write failed".to_owned(),
            retry_after: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::email_reservation::tasks::send_verification_code::{
        send_verification_code_task_adapter, v1::SendVerificationCodeTaskV1,
    };
    use crate::domain::{Email, UserAccountId};
    use cqrs_runtime::task::data::format::TaskFormat;
    use serde_json::json;
    use uuid::Uuid;

    fn delivery() -> TaskAttempt {
        TaskAttempt::new(
            Uuid::from_u128(41), TaskFormat::of::<SendVerificationCodeTaskV1>().unwrap(),
            json!({"email":"alice@example.com", "account_id":"26e91668-f9fc-4be4-bd9d-321fba42e18b", "verification_code":1234567}),
            json!({}), 2,
        ).unwrap()
    }
    use cqrs_runtime::task::handling::handler::TaskHandlerError;
    use std::io::{self, Write};

    #[tokio::test]
    async fn console_propagates_write_and_flush_errors_as_retryable() {
        struct BrokenWriter {
            flush_only: bool,
        }
        impl Write for BrokenWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if self.flush_only {
                    Ok(bytes.len())
                } else {
                    Err(io::Error::other("broken"))
                }
            }
            fn flush(&mut self) -> io::Result<()> {
                Err(io::Error::other("broken"))
            }
        }
        for flush_only in [false, true] {
            let mut handler =
                send_verification_code_task_adapter(Box::new(ConsoleVerificationCodeSender::new(BrokenWriter { flush_only })))
                    .unwrap();
            assert!(matches!(
                handler.invoke(&delivery()).await,
                Err(TaskHandlerError::Retry { .. })
            ));
        }
    }

    #[tokio::test]
    async fn console_preserves_output_with_borrowed_delivery() {
        let mut sender = ConsoleVerificationCodeSender::new(Vec::<u8>::new());
        let task = delivery();
        sender
            .send(
                VerificationCodeDelivery {
                    email: Email::parse("alice@example.com").unwrap(),
                    account_id: "26e91668-f9fc-4be4-bd9d-321fba42e18b".parse::<UserAccountId>().unwrap(),
                    verification_code: 1_234_567,
                },
                &task,
            )
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8(sender.writer).unwrap(),
            "Verification code 1234567 for account_id 26e91668-f9fc-4be4-bd9d-321fba42e18b sent to alice@example.com; task_id=00000000-0000-0000-0000-000000000029; attempt=2\n"
        );
    }
}
