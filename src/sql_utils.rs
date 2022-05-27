use rusqlite::ToSql;
use std::iter::ExactSizeIterator;

/// Helper function to allow doing SQL queries like:
///
/// ```txt
/// SELECT * FROM foo WHERE bar in (?, ?, ...);
/// ```
///
/// when the elements being compared is unknown at compile time.
pub fn add_array_to_query<'a, I, II, T: 'a + ToSql>(
    sql: &mut String,
    params: &mut Vec<&'a dyn ToSql>,
    elems: II,
) where
    I: ExactSizeIterator<Item = &'a T>,
    II: IntoIterator<Item = &'a T, IntoIter = I>,
{
    let elems = elems.into_iter();
    let last_idx = elems.len() - 1;
    sql.push('(');
    for (idx, elem) in elems.enumerate() {
        params.push(elem);
        sql.push('?');
        if idx != last_idx {
            sql.push(',');
        }
    }
    sql.push(')');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_empty_array_to_query() {
        let mut sql = String::new();
        let mut params = Vec::<&dyn ToSql>::new();
        let elems: &[u32] = &[1, 2, 3];
        add_array_to_query(&mut sql, &mut params, elems.iter());
        assert_eq!(sql, "(?,?,?)");
        assert_eq!(params.len(), elems.len());
    }
}
