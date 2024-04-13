use aws_sdk_athena::types::ResultSet;
use std::collections::HashMap;

extern crate from_athena_derive;
pub use from_athena_derive::FromAthena;

/// A trait for converting data from an Athena query result into a specified type.
///
/// This trait defines a method `from_athena` which converts a HashMap of string
/// key-value pairs representing data retrieved from an Athena query into an instance
/// of the implementing type. The conversion process may involve parsing, validation,
/// or any other necessary transformation.
///
/// # Errors
///
/// If the conversion process fails due to invalid or missing data, an error is returned.
///
/// # Examples
///
/// Implementing `FromAthena` for a custom struct:
pub trait FromAthena: Sized {
    /// Converts a HashMap of string key-value pairs into an instance of the implementing type.
    ///
    /// # Arguments
    ///
    /// * `values` - A HashMap containing the data to be converted.
    ///
    /// # Returns
    ///
    /// Result containing the converted instance of the implementing type or an error if conversion fails.
    fn from_athena(values: HashMap<String, String>) -> anyhow::Result<Self, anyhow::Error>;
}

/// Builds a vector of hash maps representing the rows of the given ResultSet.
///
/// This function takes a ResultSet as input and returns a vector of hash maps,
/// where each hash map represents a row in the ResultSet. If the ResultSet contains
/// no data or metadata, an empty vector is returned.
///
/// # Arguments
///
/// * `result_set` - A ResultSet containing the data to be converted into hash maps.
///
/// # Returns
///
/// A vector of hash maps, where each hash map represents a row in the ResultSet.
///
/// # Examples
///
/// ```
/// use some_library::{ResultSet, build_map};
///
/// let result_set = ResultSet::new(/* Some initialization */);
/// let mapped_data = build_map(result_set);
/// // Use mapped_data for further processing
/// ```
pub fn build_map(result_set: ResultSet) -> Vec<HashMap<String, String>> {
    if let Some(meta) = result_set.result_set_metadata() {
        let columns: Vec<String> = meta
            .column_info()
            .iter()
            .map(|c| c.name().to_string())
            .collect();

        let rows: Vec<HashMap<String, String>> = result_set
            .rows()
            .iter()
            .map(|r| {
                r.data()
                    .iter()
                    .map(|d| d.var_char_value().unwrap_or("").to_string())
                    .zip(columns.iter())
                    .map(|(val, col)| (col.clone(), val.clone()))
                    .collect::<HashMap<String, String>>()
            })
            .collect();

        rows
    } else {
        vec![]
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use aws_sdk_athena::types::{ColumnInfo, Datum, ResultSetMetadata, Row};

    #[derive(from_athena_derive::FromAthena)]
    struct Testing {
        pub test: i64,
    }

    #[derive(from_athena_derive::FromAthena)]
    #[allow(dead_code)]
    struct BadTesting {
        pub no_exist: String,
    }

    #[derive(from_athena_derive::FromAthena)]
    struct LargeStruct {
        pub test1: i64,
        pub test2: i32,
        pub test3: String,
        pub test4: String,
        pub test5: f64,
        pub test6: bool,
    }

    #[test]
    fn convert_result_set_to_map() {
        let column = ColumnInfo::builder()
            .name("test")
            .r#type("bigint")
            .build()
            .unwrap();
        let metadata = ResultSetMetadata::builder().column_info(column).build();
        let data = Datum::builder()
            .set_var_char_value(Some("100".to_string()))
            .build();
        let row = Row::builder().set_data(Some(vec![data])).build();
        let result_set = ResultSet::builder()
            .result_set_metadata(metadata)
            .set_rows(Some(vec![row]))
            .build();

        let res = build_map(result_set);
        assert!(res.len() == 1);
        assert!(res[0].get("test").is_some());
        assert_eq!(res[0].get("test").unwrap(), "100");
    }

    #[test]
    fn converted_results_to_struct() {
        let column = ColumnInfo::builder()
            .name("test")
            .r#type("bigint")
            .build()
            .unwrap();
        let metadata = ResultSetMetadata::builder().column_info(column).build();
        let data = Datum::builder()
            .set_var_char_value(Some("100".to_string()))
            .build();
        let row = Row::builder().set_data(Some(vec![data])).build();
        let result_set = ResultSet::builder()
            .result_set_metadata(metadata)
            .set_rows(Some(vec![row]))
            .build();

        let res: Vec<Testing> = build_map(result_set)
            .iter()
            .flat_map(|x| Testing::from_athena(x.clone()))
            .collect();

        assert_eq!(res[0].test, 100);
    }

    #[test]
    fn converted_results_to_large_struct() {
        let columns = [
            ("test1", "bigint"),
            ("test2", "integer"),
            ("test3", "varchar"),
            ("test4", "varchar"),
            ("test5", "double"),
            ("test6", "boolean"),
        ]
        .iter()
        .map(|i| {
            ColumnInfo::builder()
                .name(i.0.to_string())
                .r#type(i.1.to_string())
                .build()
                .unwrap()
        })
        .collect();

        let metadata = ResultSetMetadata::builder()
            .set_column_info(Some(columns))
            .build();

        let data: Vec<Datum> = ["1000", "100", "test", "test", "100.0", "true"]
            .iter()
            .map(|v| {
                Datum::builder()
                    .set_var_char_value(Some(v.to_string()))
                    .build()
            })
            .collect();

        let row = Row::builder().set_data(Some(data)).build();

        let result_set = ResultSet::builder()
            .result_set_metadata(metadata)
            .set_rows(Some(vec![row]))
            .build();

        let res: Vec<Result<LargeStruct, anyhow::Error>> = build_map(result_set)
            .iter()
            .map(|x| LargeStruct::from_athena(x.clone()))
            .collect();

        for res in res {
            match res {
                Ok(r) => {
                    assert_eq!(r.test1, 1000);
                    assert_eq!(r.test2, 100);
                    assert_eq!(r.test3, "test".to_string());
                    assert_eq!(r.test4, "test");
                    assert_eq!(r.test5, 100.0);
                    assert!(r.test6);
                }
                Err(e) => {
                    panic!("{:#}", e);
                }
            }
        }
    }

    #[test]
    fn error_convert_results_to_invalid_struct() {
        let column = ColumnInfo::builder()
            .name("test")
            .r#type("bigint")
            .build()
            .unwrap();
        let metadata = ResultSetMetadata::builder().column_info(column).build();
        let data = Datum::builder()
            .set_var_char_value(Some("100".to_string()))
            .build();
        let row = Row::builder().set_data(Some(vec![data])).build();
        let result_set = ResultSet::builder()
            .result_set_metadata(metadata)
            .set_rows(Some(vec![row]))
            .build();

        let res: Vec<Result<BadTesting, anyhow::Error>> = build_map(result_set)
            .iter()
            .map(|x| BadTesting::from_athena(x.clone()))
            .collect();

        assert!(res[0].is_err());
        assert_eq!(
            res[0].as_ref().err().unwrap().to_string(),
            "Missing field within result set. `no_exist` was not found!".to_string()
        );
    }
}
