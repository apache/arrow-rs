use heck::{ToSnakeCase, ToUpperCamelCase};
use proc_macro::TokenStream;
use quote::{format_ident, quote, ToTokens};
use syn::{
    parse_macro_input, punctuated::Punctuated, spanned::Spanned, token::Comma, Token, DeriveInput, Error, Field, GenericParam, Generics, Ident, ImplGenerics, Index, Lifetime, LifetimeParam, Type, TypeGenerics, TypeParamBound, TypeReference, Variant, WhereClause, TraitBound, TraitBoundModifier
};

struct FieldData<'a> {
    ty: &'a Type,
    index: Index,
    ident: Option<&'a Ident>,
    name: Option<String>,
    get: Ident,
    get_opt: Ident,
    is_valid: Ident,
    is_null: Ident,
    ident_or_index: Box<dyn ToTokens>,
}

macro_rules! field {
    ($vec:ident.$field:ident) => {
        $vec.iter().map(|f| &f.$field).collect::<Vec<_>>()
    };
}

fn fields_data<'a>(fields: &'a Punctuated<Field, Comma>, generics: &Generics) -> Vec<FieldData<'a>> {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            FieldData {
                ty: &field.ty,
                index: index.into(),
                ident: field.ident.as_ref(),
                name: field.ident.as_ref().map(ToString::to_string),
                get: field.ident.as_ref().map_or_else(
                    || format_ident!("_{}", index, span = field.span()),
                    |ident| {
                        format_ident!(
                            "r#{}",
                            &ident.to_string().to_snake_case(),
                            span = field.span()
                        )
                    },
                ),
                get_opt: field.ident.as_ref().map_or_else(
                    || format_ident!("_{}_opt", index, span = field.span()),
                    |ident| {
                        format_ident!(
                            "{}_opt",
                            &ident.to_string().to_snake_case(),
                            span = field.span()
                        )
                    },
                ),
                is_valid: field.ident.as_ref().map_or_else(
                    || format_ident!("is_{}_valid", index, span = field.span()),
                    |ident| {
                        format_ident!(
                            "is_{}_valid",
                            ident.to_string().to_snake_case(),
                            span = field.span()
                        )
                    },
                ),
                is_null: field.ident.as_ref().map_or_else(
                    || format_ident!("is_{}_null", index, span = field.span()),
                    |ident| {
                        format_ident!(
                            "is_{}_null",
                            ident.to_string().to_snake_case(),
                            span = field.span()
                        )
                    },
                ),
                ident_or_index: field.ident.as_ref().map_or_else(
                    || Box::new(Index::from(index)) as Box<dyn ToTokens>,
                    |ident| Box::new(ident.clone()) as Box<dyn ToTokens>,
                ),
            }
        })
        .collect()
}

#[proc_macro_derive(Struct, attributes(by_value))]
pub fn struct_array(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    
    let ident = &input.ident;
    let ident_name = ident.to_string();

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let expanded = match &input.data {
        syn::Data::Struct(struct_) => {
            let fields = match &struct_.fields {
                syn::Fields::Named(named) => &named.named,
                syn::Fields::Unnamed(unnamed) => &unnamed.unnamed,
                syn::Fields::Unit => {
                    return Error::new(input.span(), "Unit structs aren't supported")
                        .into_compile_error()
                        .into()
                }
            };

            let accessor_item = format_ident!("{}AccessorItem", ident);

            let data = fields_data(fields, &input.generics);

            let field_name = field!(data.name);
            let field_ident = field!(data.ident);
            let field_index = field!(data.index);
            let field_type = field!(data.ty);
            let get = field!(data.get);
            let get_opt = field!(data.get_opt);
            let is_valid = field!(data.is_valid);
            let is_null = field!(data.is_null);
            let ident_or_index = field!(data.ident_or_index);

            let fields = if matches!(struct_.fields, syn::Fields::Named(_)) {
                quote! {

                    let fields = value.fields();

                    #( let mut #field_ident = ::std::option::Option::None; )*

                    for (index, field) in fields.iter().enumerate() {
                        match field.name().as_str() {
                            #( #field_name => { #field_ident = ::std::option::Option::Some(value.column(index).as_ref().try_into()?); } )*
                            _ => {}
                        }
                    }

                    Self {
                        #( #field_ident: #field_ident.ok_or_else(|| ::arrow::error::ArrowError::InvalidArgumentError(format!("Field {} not found on StructArray {:?}", #ident_name, fields)))?, )*
                    }

                }
            } else {
                quote! { Self ( #( value.column(#field_index).as_ref().try_into()?,)* ) }
            };

            quote! {
                impl #impl_generics TryFrom<&'a ::arrow::array::StructArray> for #ident #ty_generics #where_clause {
                    type Error = ::arrow::error::ArrowError;
                    fn try_from(value: &'a ::arrow::array::StructArray) -> ::std::result::Result<Self, Self::Error> {
                        ::std::result::Result::Ok({#fields})
                    }
                }

                impl #impl_generics ::arrow::array::TypedStructInnerAccessor<'a> for #ident #ty_generics #where_clause {
                    type Item = #accessor_item #ty_generics;
                }

                #[derive(Debug)]
                struct #accessor_item #impl_generics #where_clause {
                    index: ::std::primitive::usize,
                    struct_: ::arrow::array::TypedStructArray<'a, #ident #ty_generics>,
                }

                impl #impl_generics ::std::clone::Clone for #accessor_item #ty_generics #where_clause {
                    fn clone(&self) -> Self {
                        *self
                    }
                }

                impl #impl_generics ::std::marker::Copy for #accessor_item #ty_generics #where_clause {}

                impl #impl_generics ::std::convert::From<(::arrow::array::TypedStructArray<'a, #ident #ty_generics>, usize)> for #accessor_item #ty_generics {
                    fn from(value: (::arrow::array::TypedStructArray<'a, #ident #ty_generics>, usize)) -> Self {
                        Self{
                            index: value.1,
                            struct_: value.0
                        }
                    }
                }

                impl #impl_generics #accessor_item #ty_generics {
                    #[inline]
                    fn is_valid(&self) -> ::std::primitive::bool {
                        self.struct_.is_valid(self.index)
                    }

                    #[inline]
                    fn is_null(&self) -> ::std::primitive::bool {
                        self.struct_.is_null(self.index)
                    }

                    #(
                        #[inline]
                        fn #get(&self) -> <#field_type as ::arrow::array::ArrayAccessor>::Item {
                            (&self.struct_.fields().#ident_or_index).value(self.index)
                        }

                        #[inline]
                        fn #get_opt(&self) -> ::std::option::Option<<#field_type as ::arrow::array::ArrayAccessor>::Item> {
                            if (&self.struct_.fields().#ident_or_index).is_valid(self.index) {
                                Some((&self.struct_.fields().#ident_or_index).value(self.index))
                            } else {
                                None
                            }
                        }

                        #[inline]
                        fn #is_valid(&self) -> ::std::primitive::bool {
                            self.struct_.fields().#ident_or_index.is_valid(self.index)
                        }

                        #[inline]
                        fn #is_null(&self) -> ::std::primitive::bool {
                            self.struct_.fields().#ident_or_index.is_null(self.index)
                        }
                    )*
                }

            }
        }
        _ => Error::new(input.span(), "Only structs are supported").into_compile_error(),
    };

    TokenStream::from(expanded)
}
