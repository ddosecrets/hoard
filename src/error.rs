use std::error::Error;
use std::fmt;

// because `std::error::Error` isn't implemented for `anyhow::Error`
pub struct GenericError(String);

impl GenericError {
    pub fn new(string: String) -> Self {
        Self(string)
    }
}

impl Error for GenericError {}

impl fmt::Debug for GenericError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for GenericError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
