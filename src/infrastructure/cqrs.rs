pub mod email_reservation_gateway;
pub mod user_account_gateway;

use std::error::Error;

use cqrs_es::AggregateError;

use crate::application::CommandGatewayError;

pub(super) fn map_aggregate_error<D: Error>(error: AggregateError<D>) -> CommandGatewayError<D> {
    match error {
        AggregateError::UserError(error) => CommandGatewayError::Domain(error),
        AggregateError::AggregateConflict => CommandGatewayError::Conflict,
        AggregateError::DatabaseConnectionError(_)
        | AggregateError::DeserializationError(_)
        | AggregateError::UnexpectedError(_) => CommandGatewayError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use crate::domain::{EmailReservationError, UserAccountError};

    #[test]
    fn maps_framework_errors_for_both_aggregates() {
        assert_eq!(
            map_aggregate_error::<EmailReservationError>(AggregateError::AggregateConflict),
            CommandGatewayError::Conflict
        );
        assert_eq!(
            map_aggregate_error(AggregateError::UserError(UserAccountError::EmptyFirstName)),
            CommandGatewayError::Domain(UserAccountError::EmptyFirstName)
        );

        for (error, expected_error) in [
            (
                AggregateError::UserError(EmailReservationError::NotReserved),
                CommandGatewayError::Domain(EmailReservationError::NotReserved),
            ),
            (
                AggregateError::UserError(EmailReservationError::AlreadyVerified),
                CommandGatewayError::Domain(EmailReservationError::AlreadyVerified),
            ),
            (
                AggregateError::DatabaseConnectionError(Box::new(io::Error::other("db"))),
                CommandGatewayError::Unavailable,
            ),
            (
                AggregateError::DeserializationError(Box::new(io::Error::other("json"))),
                CommandGatewayError::Unavailable,
            ),
            (
                AggregateError::UnexpectedError(Box::new(io::Error::other("unexpected"))),
                CommandGatewayError::Unavailable,
            ),
        ] {
            assert_eq!(map_aggregate_error(error), expected_error);
        }
    }
}
