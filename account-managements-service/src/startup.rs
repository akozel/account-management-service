//! Executable composition and ownership of service resources.
pub(crate) mod environment;
pub(crate) mod error;
pub(crate) mod service;
#[cfg(test)]
pub(crate) mod test_support;
mod wiring;
