use std::collections::HashMap;

use aws_sdk_athena::types::ResultSet;

pub trait FromAthena: Sized {
    fn from_athena(values: HashMap<String, String>) -> anyhow::Result<Self, anyhow::Error>;
}

fn build_map(result_set: ResultSet) -> Option<Vec<HashMap<String, String>>> {
    if let Some(meta) = result_set.result_set_metadata() {
        let columns: Vec<(String, String)> = meta
            .column_info()
            .iter()
            .map(|c| (c.name().to_string(), c.r#type.to_string()))
            .collect();

        let rows: Vec<Vec<String>> = result_set
            .rows()
            .iter()
            .map(|r| {
                r.data()
                    .iter()
                    .map(|d| d.var_char_value().unwrap_or("").to_string())
                    .collect::<Vec<String>>()
            })
            .collect();

        let combined: Vec<HashMap<String, String>> = rows
            .iter()
            .map(|row| {
                columns
                    .iter()
                    .flat_map(|(col, _)| row.iter().map(|val| (col.clone(), val.clone())))
                    .collect::<HashMap<String, String>>()
            })
            .collect();

        Some(combined)
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use aws_sdk_athena::types::{ColumnInfo, Datum, ResultSetMetadata, Row};
    use parser_macro::FromAthena;

    #[derive(FromAthena)]
    struct Testing {
        pub test: i64,
    }

    #[derive(FromAthena)]
    struct BadTesting {
        pub no_exist: String,
    }

    #[test]
    fn convert_result_set_to_tups() {
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

        let res = build_map(result_set).unwrap();
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
            .unwrap()
            .iter()
            .flat_map(|x| Testing::from_athena(x.clone()))
            .collect();

        assert_eq!(res[0].test, 100);
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
            .unwrap()
            .iter()
            .map(|x| BadTesting::from_athena(x.clone()))
            .collect();

        assert!(res[0].is_err());
    }
}
