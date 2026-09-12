use cqrs_es::{Aggregate, event_sink::EventSink};

use super::{CreatedAccount, UserAccount, UserAccountCommand, UserAccountError, UserAccountEvent};

impl Aggregate for UserAccount {
    const TYPE: &'static str = "user_account";
    type Command = UserAccountCommand;
    type Event = UserAccountEvent;
    type Error = UserAccountError;
    type Services = ();

    async fn handle(
        &mut self,
        command: Self::Command,
        _service: &Self::Services,
        sink: &EventSink<Self>,
    ) -> Result<(), Self::Error> {
        match command {
            UserAccountCommand::CreateAccount {
                account_id,
                email,
                first_name,
                last_name,
                date_of_birth,
                validation_date,
                created_at,
            } => {
                self.handle_create_account(
                    account_id,
                    email,
                    first_name,
                    last_name,
                    date_of_birth,
                    validation_date,
                    created_at,
                    sink,
                )
                .await
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        let UserAccountEvent::AccountCreated {
            account_id,
            email,
            first_name,
            last_name,
            date_of_birth,
            validation_date,
            created_at,
        } = event;
        self.created = Some(CreatedAccount {
            account_id,
            email,
            first_name,
            last_name,
            date_of_birth,
            validation_date,
            created_at,
        });
    }
}
