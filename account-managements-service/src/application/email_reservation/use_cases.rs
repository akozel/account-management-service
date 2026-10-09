// TODO(registration): Bound verification attempts and code requests. V1
// deliberately permits unrestricted guesses of the seven-digit code, which
// can confirm an email without mailbox access.
pub(super) mod confirm_email;
pub(super) mod request_new_verification_code;
pub(super) mod reserve_email;
