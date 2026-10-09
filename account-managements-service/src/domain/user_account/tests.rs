use cqrs_es::{Aggregate, DomainEvent, test::TestFramework};

use super::*;

fn date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).unwrap()
}

fn timestamp(day: u32) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(&format!("2026-09-{day:02}T12:00:00Z"))
        .unwrap()
        .with_timezone(&Utc)
}

fn id(value: u128) -> UserAccountId {
    UserAccountId::from_uuid(uuid::Uuid::from_u128(value))
}

fn email(value: &str) -> Email {
    Email::parse(value).unwrap()
}

fn command() -> UserAccountCommand {
    UserAccountCommand::CreateAccount {
        account_id: id(1),
        email: email("Alice@Example.com"),
        first_name: " Alice ".into(),
        last_name: " Smith ".into(),
        date_of_birth: date(1990, 5, 12),
        validation_date: date(2026, 9, 14),
        created_at: timestamp(14),
    }
}

fn created_event() -> UserAccountEvent {
    UserAccountEvent::AccountCreated {
        account_id: id(1),
        email: email("alice@example.com"),
        first_name: "Alice".into(),
        last_name: "Smith".into(),
        date_of_birth: date(1990, 5, 12),
        validation_date: date(2026, 9, 14),
        created_at: timestamp(14),
    }
}

#[test]
fn first_creation_records_normalized_profile_and_fixed_time() {
    assert!(UserAccount::default().created().is_none());
    TestFramework::<UserAccount>::with(())
        .given_no_previous_events()
        .when(command())
        .then_expect_events(vec![created_event()]);
    let event = created_event();
    assert_eq!(event.event_type(), "account_created");
    assert_eq!(event.event_version(), "1");
    assert_eq!(
        serde_json::from_value::<UserAccountEvent>(serde_json::to_value(&event).unwrap()).unwrap(),
        event
    );
}

#[test]
fn first_creation_rejects_invalid_profile_without_event() {
    let mut blank_first = command();
    let UserAccountCommand::CreateAccount { first_name, .. } = &mut blank_first;
    *first_name = " \t ".into();
    TestFramework::<UserAccount>::with(())
        .given_no_previous_events()
        .when(blank_first)
        .then_expect_error(UserAccountError::EmptyFirstName);

    let mut blank_last = command();
    let UserAccountCommand::CreateAccount { last_name, .. } = &mut blank_last;
    *last_name = "  ".into();
    TestFramework::<UserAccount>::with(())
        .given_no_previous_events()
        .when(blank_last)
        .then_expect_error(UserAccountError::EmptyLastName);

    let mut future_birth = command();
    let UserAccountCommand::CreateAccount { date_of_birth, .. } = &mut future_birth;
    *date_of_birth = date(2026, 9, 15);
    TestFramework::<UserAccount>::with(())
        .given_no_previous_events()
        .when(future_birth)
        .then_expect_error(UserAccountError::FutureDateOfBirth {
            date_of_birth: date(2026, 9, 15),
            validation_date: date(2026, 9, 14),
        });

    let mut today_birth = command();
    let UserAccountCommand::CreateAccount { date_of_birth, .. } = &mut today_birth;
    *date_of_birth = date(2026, 9, 14);
    let mut expected = created_event();
    let UserAccountEvent::AccountCreated { date_of_birth, .. } = &mut expected;
    *date_of_birth = date(2026, 9, 14);
    TestFramework::<UserAccount>::with(())
        .given_no_previous_events()
        .when(today_birth)
        .then_expect_events(vec![expected]);
}

#[test]
fn exact_retry_ignores_new_created_at_and_conflicting_intent_fails() {
    let mut retry = command();
    let UserAccountCommand::CreateAccount { created_at, .. } = &mut retry;
    *created_at = timestamp(15);
    TestFramework::<UserAccount>::with(())
        .given(vec![created_event()])
        .when(retry)
        .then_expect_error(UserAccountError::AlreadyCreated);

    let mut changes = Vec::new();
    let mut changed_id = command();
    let UserAccountCommand::CreateAccount { account_id, .. } = &mut changed_id;
    *account_id = id(2);
    changes.push(changed_id);
    let mut changed_email = command();
    let UserAccountCommand::CreateAccount { email: value, .. } = &mut changed_email;
    *value = email("bob@example.com");
    changes.push(changed_email);
    let mut changed_first = command();
    let UserAccountCommand::CreateAccount { first_name, .. } = &mut changed_first;
    *first_name = "Alicia".into();
    changes.push(changed_first);
    let mut changed_last = command();
    let UserAccountCommand::CreateAccount { last_name, .. } = &mut changed_last;
    *last_name = "Jones".into();
    changes.push(changed_last);
    let mut changed_birth = command();
    let UserAccountCommand::CreateAccount { date_of_birth, .. } = &mut changed_birth;
    *date_of_birth = date(1991, 5, 12);
    changes.push(changed_birth);
    let mut changed_validation = command();
    let UserAccountCommand::CreateAccount { validation_date, .. } = &mut changed_validation;
    *validation_date = date(2026, 9, 15);
    changes.push(changed_validation);
    for change in changes {
        TestFramework::<UserAccount>::with(())
            .given(vec![created_event()])
            .when(change)
            .then_expect_error(UserAccountError::AlreadyExistsWithDifferentData);
    }
}

#[test]
fn replay_and_snapshot_preserve_original_creation() {
    let mut account = UserAccount::default();
    account.apply(created_event());
    let created = account.created().unwrap();
    assert_eq!(created.account_id(), id(1));
    assert_eq!(created.email().as_str(), "alice@example.com");
    assert_eq!(created.first_name(), "Alice");
    assert_eq!(created.last_name(), "Smith");
    assert_eq!(created.date_of_birth(), date(1990, 5, 12));
    assert_eq!(created.validation_date(), date(2026, 9, 14));
    assert_eq!(created.created_at(), timestamp(14));
    assert_eq!(
        serde_json::from_value::<UserAccount>(serde_json::to_value(&account).unwrap()).unwrap(),
        account
    );
}
