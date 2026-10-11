//! Shared integer parsing; each configuration owns its units, validation and errors.

pub(crate) fn read_u64(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
) -> Result<u64, std::num::ParseIntError> {
    lookup(name)
        .map(|value| value.parse())
        .transpose()
        .map(|value| value.unwrap_or(default))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_parsing_preserves_absence_aliases_and_overflow_behavior() {
        assert_eq!(read_u64(&mut |_| None, "KEY", 42).unwrap(), 42);
        for value in ["0", "+0", "18446744073709551615"] {
            assert_eq!(
                read_u64(&mut |_| Some(value.into()), "KEY", 42).unwrap(),
                value.parse::<u64>().unwrap()
            );
        }
        for value in ["", "-1", " 1", "garbage", "18446744073709551616"] {
            assert!(read_u64(&mut |_| Some(value.into()), "KEY", 42).is_err());
        }
    }
}
