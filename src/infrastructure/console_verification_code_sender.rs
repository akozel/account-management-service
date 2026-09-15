use std::io::{self, Write};

use crate::application::{
    email_reservation::tasks::send_verification_code::{DeliveryContext, VerificationCodeDelivery, VerificationCodeSender},
    outbox::TaskExecutionError,
};
use async_trait::async_trait;

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
    async fn send(&mut self, message: VerificationCodeDelivery, context: DeliveryContext) -> Result<(), TaskExecutionError> {
        writeln!(
            self.writer,
            "Verification code {} for account_id {} sent to {}; task_id={}; attempt={}",
            message.verification_code, message.account_id, message.email, context.task_id, context.attempt
        )
        .and_then(|()| self.writer.flush())
        .map_err(|_| TaskExecutionError::Retry {
            reason: "console write failed".to_owned(),
            retry_after: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::email_reservation::tasks::send_verification_code::send_verification_code_task_handler;
    use crate::application::outbox::{test_support::*, *};
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
                send_verification_code_task_handler(Box::new(ConsoleVerificationCodeSender::new(BrokenWriter { flush_only })));
            assert!(matches!(
                handler.handle(&claimed()).await,
                Err(TaskExecutionError::Retry { .. })
            ));
        }
    }
}
