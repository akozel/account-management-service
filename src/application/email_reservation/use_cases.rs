// TODO(registration): Bound verification attempts and code requests. V1
// deliberately permits unrestricted guesses of the seven-digit code, which
// can confirm an email without mailbox access.
const CONFLICT_RETRIES: usize = 3;
pub(super) mod confirm_email;
pub(super) mod request_new_verification_code;
pub(super) mod reserve_email;
