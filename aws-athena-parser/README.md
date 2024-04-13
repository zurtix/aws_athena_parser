# Athena Parser

## Overview

The purpose of this package is to provide an easy method of turning the external `aws_sdk_athena::types::ResultSet` into a user defined struct of various types.

## Usage

Ensure that named values within your struct correspond to the column names of the Athena query result set.


```rust
use aws_athena_pasrer::{FromAthena, build_map};

#[derive(FromAthena)]
struct MyStruct {
  my_value: String
}

pub fn main() {
      let result_set = ...; // Athena response
      let res: Vec<MyStruct> = build_map(result_set)
            .iter()
            .flat_map(|x| MyStruct::from_athena(x.clone()))
            .collect();
      // ( use the res for your purposes )
}
```

