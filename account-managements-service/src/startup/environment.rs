//! Explicit dotenv loading; only a missing file is optional.
use super::error::ServiceError;
use std::path::PathBuf;
pub(crate) fn load_dotenv() -> Result<(), ServiceError> {
    load_dotenv_with(dotenvy::dotenv)
}

fn load_dotenv_with(load: impl FnOnce() -> Result<PathBuf, dotenvy::Error>) -> Result<(), ServiceError> {
    match load() {
        Ok(_) => Ok(()),
        Err(error) if error.not_found() => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{error::Error, io};
    #[test]
    fn dotenv_absence_is_optional_other_failures_are_typed_and_redacted() {
        assert!(load_dotenv_with(|| Ok(PathBuf::from(".env"))).is_ok());
        assert!(load_dotenv_with(|| Err(dotenvy::Error::Io(io::ErrorKind::NotFound.into()))).is_ok());
        for source in [
            dotenvy::Error::Io(io::Error::new(io::ErrorKind::PermissionDenied, "private_path")),
            dotenvy::Error::LineParse("DATABASE_URL=private_password".into(), 4),
        ] {
            let error = load_dotenv_with(|| Err(source)).unwrap_err();
            assert!(matches!(error, ServiceError::Dotenv(_)));
            assert!(error.source().is_some());
            assert!(!format!("{error:?} {error}").contains("private_"));
        }
    }
}
