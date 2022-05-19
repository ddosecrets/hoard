use chrono::{DateTime, Utc};
use rusqlite::types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef};
use rusqlite::ToSql;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timestamp(DateTime<Utc>);

impl Timestamp {
    const FMT_STR: &'static str = "%FT%T%.3fZ";

    pub fn now() -> Self {
        Self(Utc::now())
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.format(Self::FMT_STR))
    }
}

// rusqlite's `ToSql` impl for `DateTime<Utc>` uses the string `"%F %T%.f%:z"` which isn't an
// ISO-8601 string (' ' instad of 'T') and has excessive precision on the nanos.
impl ToSql for Timestamp {
    #[inline]
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let date_str = self.0.format(Self::FMT_STR).to_string();
        Ok(ToSqlOutput::from(date_str))
    }
}

impl FromSql for Timestamp {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let dt: DateTime<Utc> = FromSql::column_result(value)?;
        Ok(Self(dt))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_sql() {
        let ts = Timestamp::now();
        ts.to_sql().unwrap();
    }

    #[test]
    fn from_sql() {
        Timestamp::column_result(ValueRef::Text(b"2022-01-01T00:00:00.000Z")).unwrap();
    }
}
