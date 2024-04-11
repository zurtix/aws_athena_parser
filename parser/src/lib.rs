use aws_sdk_athena::types::ResultSet;

pub trait FromAthena: Sized {
    fn from_athena(values: Vec<(String, String, String)>) -> anyhow::Result<Self, anyhow::Error>;
}

enum AthenaTypes {
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Integer(i32),
    Int(i32),
    Bigint(i64),
    Double(f64),
    Float(f32),
    Decimal(f64),
    Char(u8),
    VarChar(String),
    String(String),
    IPAddr(String),
    Binary(Vec<u8>),
    Date(String),
    TimeStamp(String),
    //    Array(Vec<AthenaTypes>), // todo
    //    Map(HashMap<String, AthenaTypes>), // todo
    //    Struct(HashMap<String, AthenaTypes>), // todo
}

type TupType = Vec<(String, String, String)>;

fn from_type(ty: &str, val: String) {
    match ty {
        "boolean" => AthenaTypes::Boolean(val.parse().unwrap()),
        "tinyint" => AthenaTypes::TinyInt(val.parse().unwrap()),
        "smallint" => AthenaTypes::SmallInt(val.parse().unwrap()),
        "integer" => AthenaTypes::Integer(val.parse().unwrap()),
        "int" => AthenaTypes::Int(val.parse().unwrap()),
        "bigint" => AthenaTypes::Bigint(val.parse().unwrap()),
        "double" => AthenaTypes::Double(val.parse().unwrap()),
        "float" => AthenaTypes::Float(val.parse().unwrap()),
        "decimal" => AthenaTypes::Decimal(val.parse().unwrap()),
        "char" => AthenaTypes::Char(val.parse().unwrap()),
        "varchar" => AthenaTypes::VarChar(val),
        "string" => AthenaTypes::String(val),
        "ipaddr" => AthenaTypes::IPAddr(val),
        "binary" => AthenaTypes::Binary(val.into_bytes()),
        "date" => AthenaTypes::Date(val),
        "timestamp" => AthenaTypes::TimeStamp(val),
        _ => AthenaTypes::String(val),
    };
}

fn build_tups(result_set: ResultSet) -> Option<Vec<TupType>> {
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

        let combined: Vec<TupType> = rows
            .iter()
            .map(|row| {
                columns
                    .iter()
                    .flat_map(|(col, ty)| {
                        row.iter().map(|val| (col.clone(), ty.clone(), val.clone()))
                    })
                    .collect::<TupType>()
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

        let res = build_tups(result_set).unwrap();
        assert_eq!(res[0][0].0, "test");
        assert_eq!(res[0][0].1, "bigint");
        assert_eq!(res[0][0].2, "100");
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

        let res: Vec<Testing> = build_tups(result_set)
            .unwrap()
            .iter()
            .flat_map(|x| Testing::from_athena(x.clone()))
            .collect();

        assert_eq!(res[0].test, 100);
    }
}
