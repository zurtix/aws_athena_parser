extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Fields};

/// Converts data from an Athena query result into a struct implementing the `FromAthena` trait.
///
/// This function takes a TokenStream representing the input Rust code and generates
/// the necessary implementation of the `FromAthena` trait for the specified struct.
///
/// # Arguments
///
/// * `input` - A TokenStream representing the input Rust code to derive `FromAthena`.
///
/// # Returns
///
/// A TokenStream containing the generated implementation of the `FromAthena` trait for the specified struct.
/// If the input struct does not have named fields, an error TokenStream is returned.
///
/// # Examples
///
/// ```
/// use aws_athena_parser::{from_athena, FromAthena};
///
/// #[derive(FromAthena)]
/// struct MyStruct {
///     field1: String,
///     field2: i32,
/// }
/// ```
#[proc_macro_derive(FromAthena)]
pub fn from_athena(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    if let syn::Data::Struct(ref data) = input.data {
        if let Fields::Named(ref fields) = data.fields {
            let field_vals = fields.named.iter().enumerate().map(|(_, field)| {
                let name = &field.ident;
                let ty = &field.ty;

                quote!(#name: row.get(stringify!(#name))
                    .ok_or(anyhow::Error::msg(format!("Missing field within result set. `{}` was not found!", stringify!(#name))))?
                .parse::<#ty>()?)
            });

            let name = input.ident;

            return TokenStream::from(quote!(
            impl FromAthena for #name {
                fn from_athena(row: HashMap<String, String>) -> Result<Self, anyhow::Error> {
                    Ok(Self {
                        #(#field_vals),*
                    })
                }
            }));
        }
    }

    TokenStream::from(
        syn::Error::new(
            input.ident.span(),
            "Only structs with named fields can derive `FromAthena`",
        )
        .to_compile_error(),
    )
}
