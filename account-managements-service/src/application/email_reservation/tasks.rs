//! Versioned delivery tasks carrying the account/code pair from accepted events.
//!
//! Deliveries may repeat or execute out of issuance order. EmailReservation
//! validates only the latest issued pair; task execution does not advance its
//! phase. A sender-required idempotency key belongs in payload or metadata;
//! the delivery context also exposes the stable task ID for redelivery identity.

pub mod send_verification_code;
