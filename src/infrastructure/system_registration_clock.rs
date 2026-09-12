use chrono::{DateTime, Utc};

use crate::application::user_account::service::RegistrationClock;

pub struct SystemRegistrationClock;

impl RegistrationClock for SystemRegistrationClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}
