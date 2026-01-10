// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Derive macro for the [`HeapSize`] trait.
//!
//! This crate provides a `#[derive(HeapSize)]` macro that automatically
//! implements the `HeapSize` trait for structs and enums.
//!
//! # Example
//!
//! ```rust,ignore
//! use arrow_memory_size::HeapSize;
//! use arrow_memory_size_derive::HeapSize;
//!
//! #[derive(HeapSize)]
//! struct MyStruct {
//!     name: String,
//!     data: Vec<u8>,
//!     count: i32,
//! }
//! ```
//!
//! # Field Attributes
//!
//! The derive macro supports several attributes to customize behavior:
//!
//! ## `#[heap_size(ignore)]`
//!
//! Skip this field entirely (contributes 0 to heap size).
//!
//! ```rust,ignore
//! #[derive(HeapSize)]
//! struct MyStruct {
//!     data: Vec<u8>,
//!     #[heap_size(ignore)]
//!     cached_hash: u64,  // Not counted
//! }
//! ```
//!
//! ## `#[heap_size(size = N)]`
//!
//! Use a constant value instead of calling `heap_size()`.
//!
//! ```rust,ignore
//! #[derive(HeapSize)]
//! struct MyStruct {
//!     #[heap_size(size = 1024)]
//!     fixed_buffer: *const u8,  // Known to be 1KB
//! }
//! ```
//!
//! ## `#[heap_size(size_fn = path)]`
//!
//! Call a custom function to compute the heap size.
//! The function must have signature `fn(&FieldType) -> usize`.
//!
//! ```rust,ignore
//! fn custom_size(data: &ExternalType) -> usize {
//!     data.len() * 8
//! }
//!
//! #[derive(HeapSize)]
//! struct MyStruct {
//!     #[heap_size(size_fn = custom_size)]
//!     external: ExternalType,
//! }
//! ```
//!
//! # Restrictions
//!
//! This macro will emit a compile error if any field contains `Arc` or `Rc`
//! types (unless the field is ignored), as the semantics for shared references
//! are complex and should be handled manually.
//!
//! [`HeapSize`]: arrow_memory_size::HeapSize

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/arrow-rs/refs/heads/main/docs/source/_static/images/Arrow-logo_hex_black-txt_transparent-bg.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/arrow-rs/refs/heads/main/docs/source/_static/images/Arrow-logo_hex_black-txt_transparent-bg.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

extern crate proc_macro;

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Data, DataEnum, DataStruct, DeriveInput, Expr, Fields, GenericParam, Lit, Path, Type,
    parse_macro_input,
};

/// Field attribute configuration parsed from `#[heap_size(...)]`
#[derive(Default)]
struct FieldAttr {
    /// Skip this field (return 0)
    ignore: bool,
    /// Use a constant size value
    size: Option<usize>,
    /// Use a custom function to compute size
    size_fn: Option<Path>,
}

impl FieldAttr {
    fn parse(field: &syn::Field) -> Result<Self, syn::Error> {
        let mut attr = FieldAttr::default();

        for a in &field.attrs {
            if !a.path().is_ident("heap_size") {
                continue;
            }

            a.parse_nested_meta(|meta| {
                if meta.path.is_ident("ignore") {
                    attr.ignore = true;
                    Ok(())
                } else if meta.path.is_ident("size") {
                    let value: Expr = meta.value()?.parse()?;
                    if let Expr::Lit(expr_lit) = &value {
                        if let Lit::Int(lit_int) = &expr_lit.lit {
                            attr.size = Some(lit_int.base10_parse()?);
                            return Ok(());
                        }
                    }
                    Err(meta.error("expected integer literal for `size`"))
                } else if meta.path.is_ident("size_fn") {
                    let value: Expr = meta.value()?.parse()?;
                    if let Expr::Path(expr_path) = value {
                        attr.size_fn = Some(expr_path.path);
                        return Ok(());
                    }
                    Err(meta.error("expected path for `size_fn`"))
                } else {
                    Err(meta.error("unknown heap_size attribute"))
                }
            })?;
        }

        // Validate that only one option is set
        let count = attr.ignore as u8 + attr.size.is_some() as u8 + attr.size_fn.is_some() as u8;
        if count > 1 {
            return Err(syn::Error::new_spanned(
                field,
                "only one of `ignore`, `size`, or `size_fn` can be specified",
            ));
        }

        Ok(attr)
    }
}

/// Derive [`HeapSize`] implementations for structs and enums.
///
/// This macro generates an implementation of the `HeapSize` trait that
/// calculates heap memory usage by summing the `heap_size()` of all fields.
///
/// # Supported Types
///
/// - **Structs with named fields**: sums heap size of all fields
/// - **Tuple structs**: sums heap size of all tuple elements
/// - **Unit structs**: returns 0
/// - **Enums**: matches on variants and sums heap size of variant fields
///
/// # Field Attributes
///
/// - `#[heap_size(ignore)]` - Skip this field (contributes 0)
/// - `#[heap_size(size = N)]` - Use constant value N
/// - `#[heap_size(size_fn = path)]` - Call custom function
///
/// # Restrictions
///
/// This macro will emit a compile error if any field contains `Arc` or `Rc`
/// types (unless ignored), as the semantics for shared references are complex
/// and should be handled manually.
///
/// # Example
///
/// ```rust,ignore
/// use arrow_memory_size::HeapSize;
/// use arrow_memory_size_derive::HeapSize;
///
/// #[derive(HeapSize)]
/// struct MyStruct {
///     name: String,
///     data: Vec<u8>,
///     #[heap_size(ignore)]
///     cached: u64,
/// }
///
/// let s = MyStruct {
///     name: "test".to_string(),
///     data: vec![1, 2, 3],
///     cached: 0,
/// };
/// println!("Heap size: {} bytes", s.heap_size());
/// ```
///
/// [`HeapSize`]: arrow_memory_size::HeapSize
#[proc_macro_derive(HeapSize, attributes(heap_size))]
pub fn heap_size_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: DeriveInput = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Check for Arc/Rc in non-ignored fields and emit error if found
    if let Err(err) = check_no_arc_rc(&input.data) {
        return err.to_compile_error().into();
    }

    // Build the generics with HeapSize bounds
    let generics = add_heap_size_bounds(&input.generics);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let heap_size_body = match &input.data {
        Data::Struct(data) => match generate_struct_heap_size(data) {
            Ok(body) => body,
            Err(err) => return err.to_compile_error().into(),
        },
        Data::Enum(data) => match generate_enum_heap_size(data) {
            Ok(body) => body,
            Err(err) => return err.to_compile_error().into(),
        },
        Data::Union(_) => {
            return syn::Error::new_spanned(&input, "HeapSize cannot be derived for unions")
                .to_compile_error()
                .into();
        }
    };

    let expanded = quote! {
        impl #impl_generics ::arrow_memory_size::HeapSize for #name #ty_generics #where_clause {
            fn heap_size(&self) -> usize {
                #heap_size_body
            }
        }
    };

    expanded.into()
}

/// Check that no non-ignored fields contain Arc or Rc types
fn check_no_arc_rc(data: &Data) -> Result<(), syn::Error> {
    match data {
        Data::Struct(data) => check_fields_no_arc_rc(&data.fields),
        Data::Enum(data) => {
            for variant in &data.variants {
                check_fields_no_arc_rc(&variant.fields)?;
            }
            Ok(())
        }
        Data::Union(_) => Ok(()),
    }
}

/// Check that non-ignored fields don't contain Arc/Rc
fn check_fields_no_arc_rc(fields: &Fields) -> Result<(), syn::Error> {
    for field in fields {
        // Parse attributes to check if field is ignored
        let attr = FieldAttr::parse(field)?;
        if attr.ignore {
            continue; // Skip Arc/Rc check for ignored fields
        }

        if contains_arc_or_rc(&field.ty) {
            return Err(syn::Error::new_spanned(
                &field.ty,
                "HeapSize cannot be derived for types containing Arc or Rc. \
                 Use #[heap_size(ignore)] to skip this field, or implement HeapSize manually.",
            ));
        }
    }
    Ok(())
}

/// Recursively check if a type contains Arc or Rc
fn contains_arc_or_rc(ty: &Type) -> bool {
    match ty {
        Type::Path(type_path) => {
            // Check the last segment of the path for Arc or Rc
            if let Some(segment) = type_path.path.segments.last() {
                let ident = segment.ident.to_string();
                if ident == "Arc" || ident == "Rc" {
                    return true;
                }
                // Check generic arguments recursively
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    for arg in &args.args {
                        if let syn::GenericArgument::Type(inner_ty) = arg {
                            if contains_arc_or_rc(inner_ty) {
                                return true;
                            }
                        }
                    }
                }
            }
            // Check all segments for fully qualified paths like std::sync::Arc
            for segment in &type_path.path.segments {
                let ident = segment.ident.to_string();
                if ident == "Arc" || ident == "Rc" {
                    return true;
                }
            }
            false
        }
        Type::Tuple(tuple) => tuple.elems.iter().any(contains_arc_or_rc),
        Type::Array(array) => contains_arc_or_rc(&array.elem),
        Type::Slice(slice) => contains_arc_or_rc(&slice.elem),
        Type::Reference(reference) => contains_arc_or_rc(&reference.elem),
        Type::Paren(paren) => contains_arc_or_rc(&paren.elem),
        Type::Group(group) => contains_arc_or_rc(&group.elem),
        _ => false,
    }
}

/// Add HeapSize bounds to generic parameters
fn add_heap_size_bounds(generics: &syn::Generics) -> syn::Generics {
    let mut generics = generics.clone();
    for param in &mut generics.params {
        if let GenericParam::Type(type_param) = param {
            type_param
                .bounds
                .push(syn::parse_quote!(::arrow_memory_size::HeapSize));
        }
    }
    generics
}

/// Generate the size expression for a single field
fn generate_field_size_expr(
    field: &syn::Field,
    accessor: TokenStream,
) -> Result<TokenStream, syn::Error> {
    let attr = FieldAttr::parse(field)?;

    if attr.ignore {
        return Ok(quote! { 0 });
    }

    if let Some(size) = attr.size {
        return Ok(quote! { #size });
    }

    if let Some(size_fn) = attr.size_fn {
        return Ok(quote! { #size_fn(&#accessor) });
    }

    // Default: call heap_size()
    Ok(quote! { ::arrow_memory_size::HeapSize::heap_size(&#accessor) })
}

/// Generate heap_size() body for structs
fn generate_struct_heap_size(data: &DataStruct) -> Result<TokenStream, syn::Error> {
    match &data.fields {
        Fields::Named(fields) => {
            if fields.named.is_empty() {
                Ok(quote! { 0 })
            } else {
                let mut field_sizes = Vec::new();
                for f in &fields.named {
                    let name = &f.ident;
                    let accessor = quote! { self.#name };
                    field_sizes.push(generate_field_size_expr(f, accessor)?);
                }
                Ok(quote! { #(#field_sizes)+* })
            }
        }
        Fields::Unnamed(fields) => {
            if fields.unnamed.is_empty() {
                Ok(quote! { 0 })
            } else {
                let mut field_sizes = Vec::new();
                for (i, f) in fields.unnamed.iter().enumerate() {
                    let index = syn::Index::from(i);
                    let accessor = quote! { self.#index };
                    field_sizes.push(generate_field_size_expr(f, accessor)?);
                }
                Ok(quote! { #(#field_sizes)+* })
            }
        }
        Fields::Unit => Ok(quote! { 0 }),
    }
}

/// Generate heap_size() body for enums
fn generate_enum_heap_size(data: &DataEnum) -> Result<TokenStream, syn::Error> {
    if data.variants.is_empty() {
        return Ok(quote! { 0 });
    }

    let mut match_arms = Vec::new();

    for variant in &data.variants {
        let variant_name = &variant.ident;
        let arm = match &variant.fields {
            Fields::Named(fields) => {
                let field_names: Vec<_> = fields
                    .named
                    .iter()
                    .map(|f| f.ident.as_ref().unwrap())
                    .collect();
                if field_names.is_empty() {
                    quote! { Self::#variant_name {} => 0 }
                } else {
                    let mut field_sizes = Vec::new();
                    for f in &fields.named {
                        let name = f.ident.as_ref().unwrap();
                        let accessor = quote! { *#name };
                        field_sizes.push(generate_field_size_expr(f, accessor)?);
                    }
                    quote! {
                        Self::#variant_name { #(#field_names),* } => {
                            #(#field_sizes)+*
                        }
                    }
                }
            }
            Fields::Unnamed(fields) => {
                let field_names: Vec<_> = (0..fields.unnamed.len())
                    .map(|i| syn::Ident::new(&format!("f{}", i), proc_macro2::Span::call_site()))
                    .collect();
                if field_names.is_empty() {
                    quote! { Self::#variant_name() => 0 }
                } else {
                    let mut field_sizes = Vec::new();
                    for (i, f) in fields.unnamed.iter().enumerate() {
                        let name =
                            syn::Ident::new(&format!("f{}", i), proc_macro2::Span::call_site());
                        let accessor = quote! { *#name };
                        field_sizes.push(generate_field_size_expr(f, accessor)?);
                    }
                    quote! {
                        Self::#variant_name(#(#field_names),*) => {
                            #(#field_sizes)+*
                        }
                    }
                }
            }
            Fields::Unit => quote! { Self::#variant_name => 0 },
        };
        match_arms.push(arm);
    }

    Ok(quote! {
        match self {
            #(#match_arms),*
        }
    })
}
