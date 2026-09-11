mod aggregate;
mod commands;
mod events;
mod id;
mod value_objects;

pub use aggregate::{UserAccount, UserAccountError};
pub use id::UserAccountId;
pub use value_objects::{Email, InvalidEmail};
