extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

#[proc_macro_derive(FromAthena)]
pub fn from_athena(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    if let syn::Data::Struct(ref data) = input.data {
        if let Fields::Named(ref fields) = data.fields {
            let field_vals = fields.named.iter().enumerate().map(|(_, field)| {
                let name = &field.ident;
                let ty = &field.ty;

                quote!(#name: row[0].2.parse::<#ty>()?)
            });

            let name = input.ident;

            return TokenStream::from(quote!(
            impl FromAthena for #name {
                fn from_athena(row: Vec<(String,String,String)>) -> Result<Self, anyhow::Error> {
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
            "Only structs with named fields can derive `FromRow`",
        )
        .to_compile_error(),
    ) // Return the generated code as a TokenStream
}
