#![feature(prelude_import)]
#[prelude_import]
use std::prelude::rust_2021::*;
#[macro_use]
extern crate std;
use std::sync::Arc;
use arrow::array::{
    Array, ArrayAccessor, ArrayIter, ArrayRef, Int32Array, StringArray, StructArray,
    TypedStructArray,
};
extern crate arrow_array_derive;
struct NestedStructArray<'a> {
    inner: TypedStructArray<'a, SimpleStructArray<'a>>,
}
#[automatically_derived]
impl<'a> ::core::clone::Clone for NestedStructArray<'a> {
    #[inline]
    fn clone(&self) -> NestedStructArray<'a> {
        let _: ::core::clone::AssertParamIsClone<
            TypedStructArray<'a, SimpleStructArray<'a>>,
        >;
        *self
    }
}
#[automatically_derived]
impl<'a> ::core::marker::Copy for NestedStructArray<'a> {}
#[automatically_derived]
impl<'a> ::core::fmt::Debug for NestedStructArray<'a> {
    #[inline]
    fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
        ::core::fmt::Formatter::debug_struct_field1_finish(
            f,
            "NestedStructArray",
            "inner",
            &&self.inner,
        )
    }
}
impl<'a> TryFrom<&'a ::arrow::array::StructArray> for NestedStructArray<'a> {
    type Error = ::arrow::error::ArrowError;
    fn try_from(
        value: &'a ::arrow::array::StructArray,
    ) -> ::std::result::Result<Self, Self::Error> {
        ::std::result::Result::Ok({
            let fields = value.fields();
            let mut inner = ::std::option::Option::None;
            for (index, field) in fields.iter().enumerate() {
                match field.name().as_str() {
                    "inner" => {
                        inner = ::std::option::Option::Some(
                            value.column(index).as_ref().try_into()?,
                        );
                    }
                    _ => {}
                }
            }
            Self {
                inner: inner
                    .ok_or_else(|| ::arrow::error::ArrowError::InvalidArgumentError({
                        let res = ::alloc::fmt::format(
                            format_args!(
                                "Field {0} not found on StructArray {1:?}",
                                "NestedStructArray",
                                fields,
                            ),
                        );
                        res
                    }))?,
            }
        })
    }
}
impl<'a> ::arrow::array::TypedStructInnerAccessor<'a> for NestedStructArray<'a> {
    type Item = NestedStructArrayAccessorItem<'a>;
}
struct NestedStructArrayAccessorItem<'a> {
    index: ::std::primitive::usize,
    struct_: ::arrow::array::TypedStructArray<'a, NestedStructArray<'a>>,
}
#[automatically_derived]
impl<'a> ::core::fmt::Debug for NestedStructArrayAccessorItem<'a> {
    #[inline]
    fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
        ::core::fmt::Formatter::debug_struct_field2_finish(
            f,
            "NestedStructArrayAccessorItem",
            "index",
            &self.index,
            "struct_",
            &&self.struct_,
        )
    }
}
impl<'a> ::std::clone::Clone for NestedStructArrayAccessorItem<'a> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<'a> ::std::marker::Copy for NestedStructArrayAccessorItem<'a> {}
impl<
    'a,
> ::std::convert::From<
    (::arrow::array::TypedStructArray<'a, NestedStructArray<'a>>, usize),
> for NestedStructArrayAccessorItem<'a> {
    fn from(
        value: (::arrow::array::TypedStructArray<'a, NestedStructArray<'a>>, usize),
    ) -> Self {
        Self {
            index: value.1,
            struct_: value.0,
        }
    }
}
impl<'a> NestedStructArrayAccessorItem<'a> {
    #[inline]
    fn is_valid(&self) -> ::std::primitive::bool {
        self.struct_.is_valid(self.index)
    }
    #[inline]
    fn is_null(&self) -> ::std::primitive::bool {
        self.struct_.is_null(self.index)
    }
    #[inline]
    fn inner(
        &self,
    ) -> <TypedStructArray<
        'a,
        SimpleStructArray<'a>,
    > as ::arrow::array::ArrayAccessor>::Item {
        (&self.struct_.fields().inner).value(self.index)
    }
    #[inline]
    fn inner_opt(
        &self,
    ) -> ::std::option::Option<
        <TypedStructArray<
            'a,
            SimpleStructArray<'a>,
        > as ::arrow::array::ArrayAccessor>::Item,
    > {
        if (&self.struct_.fields().inner).is_valid(self.index) {
            Some((&self.struct_.fields().inner).value(self.index))
        } else {
            None
        }
    }
    #[inline]
    fn is_inner_valid(&self) -> ::std::primitive::bool {
        self.struct_.fields().inner.is_valid(self.index)
    }
    #[inline]
    fn is_inner_null(&self) -> ::std::primitive::bool {
        self.struct_.fields().inner.is_null(self.index)
    }
}
struct SimpleStructArray<'a> {
    text: &'a StringArray,
    number: &'a Int32Array,
}
#[automatically_derived]
impl<'a> ::core::clone::Clone for SimpleStructArray<'a> {
    #[inline]
    fn clone(&self) -> SimpleStructArray<'a> {
        let _: ::core::clone::AssertParamIsClone<&'a StringArray>;
        let _: ::core::clone::AssertParamIsClone<&'a Int32Array>;
        *self
    }
}
#[automatically_derived]
impl<'a> ::core::marker::Copy for SimpleStructArray<'a> {}
#[automatically_derived]
impl<'a> ::core::fmt::Debug for SimpleStructArray<'a> {
    #[inline]
    fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
        ::core::fmt::Formatter::debug_struct_field2_finish(
            f,
            "SimpleStructArray",
            "text",
            &self.text,
            "number",
            &&self.number,
        )
    }
}
impl<'a> TryFrom<&'a ::arrow::array::StructArray> for SimpleStructArray<'a> {
    type Error = ::arrow::error::ArrowError;
    fn try_from(
        value: &'a ::arrow::array::StructArray,
    ) -> ::std::result::Result<Self, Self::Error> {
        ::std::result::Result::Ok({
            let fields = value.fields();
            let mut text = ::std::option::Option::None;
            let mut number = ::std::option::Option::None;
            for (index, field) in fields.iter().enumerate() {
                match field.name().as_str() {
                    "text" => {
                        text = ::std::option::Option::Some(
                            value.column(index).as_ref().try_into()?,
                        );
                    }
                    "number" => {
                        number = ::std::option::Option::Some(
                            value.column(index).as_ref().try_into()?,
                        );
                    }
                    _ => {}
                }
            }
            Self {
                text: text
                    .ok_or_else(|| ::arrow::error::ArrowError::InvalidArgumentError({
                        let res = ::alloc::fmt::format(
                            format_args!(
                                "Field {0} not found on StructArray {1:?}",
                                "SimpleStructArray",
                                fields,
                            ),
                        );
                        res
                    }))?,
                number: number
                    .ok_or_else(|| ::arrow::error::ArrowError::InvalidArgumentError({
                        let res = ::alloc::fmt::format(
                            format_args!(
                                "Field {0} not found on StructArray {1:?}",
                                "SimpleStructArray",
                                fields,
                            ),
                        );
                        res
                    }))?,
            }
        })
    }
}
impl<'a> ::arrow::array::TypedStructInnerAccessor<'a> for SimpleStructArray<'a> {
    type Item = SimpleStructArrayAccessorItem<'a>;
}
struct SimpleStructArrayAccessorItem<'a> {
    index: ::std::primitive::usize,
    struct_: ::arrow::array::TypedStructArray<'a, SimpleStructArray<'a>>,
}
#[automatically_derived]
impl<'a> ::core::fmt::Debug for SimpleStructArrayAccessorItem<'a> {
    #[inline]
    fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
        ::core::fmt::Formatter::debug_struct_field2_finish(
            f,
            "SimpleStructArrayAccessorItem",
            "index",
            &self.index,
            "struct_",
            &&self.struct_,
        )
    }
}
impl<'a> ::std::clone::Clone for SimpleStructArrayAccessorItem<'a> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<'a> ::std::marker::Copy for SimpleStructArrayAccessorItem<'a> {}
impl<
    'a,
> ::std::convert::From<
    (::arrow::array::TypedStructArray<'a, SimpleStructArray<'a>>, usize),
> for SimpleStructArrayAccessorItem<'a> {
    fn from(
        value: (::arrow::array::TypedStructArray<'a, SimpleStructArray<'a>>, usize),
    ) -> Self {
        Self {
            index: value.1,
            struct_: value.0,
        }
    }
}
impl<'a> SimpleStructArrayAccessorItem<'a> {
    #[inline]
    fn is_valid(&self) -> ::std::primitive::bool {
        self.struct_.is_valid(self.index)
    }
    #[inline]
    fn is_null(&self) -> ::std::primitive::bool {
        self.struct_.is_null(self.index)
    }
    #[inline]
    fn text(&self) -> <&'a StringArray as ::arrow::array::ArrayAccessor>::Item {
        (&self.struct_.fields().text).value(self.index)
    }
    #[inline]
    fn text_opt(
        &self,
    ) -> ::std::option::Option<
        <&'a StringArray as ::arrow::array::ArrayAccessor>::Item,
    > {
        if (&self.struct_.fields().text).is_valid(self.index) {
            Some((&self.struct_.fields().text).value(self.index))
        } else {
            None
        }
    }
    #[inline]
    fn is_text_valid(&self) -> ::std::primitive::bool {
        self.struct_.fields().text.is_valid(self.index)
    }
    #[inline]
    fn is_text_null(&self) -> ::std::primitive::bool {
        self.struct_.fields().text.is_null(self.index)
    }
    #[inline]
    fn number(&self) -> <&'a Int32Array as ::arrow::array::ArrayAccessor>::Item {
        (&self.struct_.fields().number).value(self.index)
    }
    #[inline]
    fn number_opt(
        &self,
    ) -> ::std::option::Option<<&'a Int32Array as ::arrow::array::ArrayAccessor>::Item> {
        if (&self.struct_.fields().number).is_valid(self.index) {
            Some((&self.struct_.fields().number).value(self.index))
        } else {
            None
        }
    }
    #[inline]
    fn is_number_valid(&self) -> ::std::primitive::bool {
        self.struct_.fields().number.is_valid(self.index)
    }
    #[inline]
    fn is_number_null(&self) -> ::std::primitive::bool {
        self.struct_.fields().number.is_null(self.index)
    }
}
fn main() {
    let strings: ArrayRef = Arc::new(
        StringArray::from(
            <[_]>::into_vec(
                #[rustc_box]
                ::alloc::boxed::Box::new([Some("joe"), None, None, Some("mark")]),
            ),
        ),
    );
    let ints: ArrayRef = Arc::new(
        Int32Array::from(
            <[_]>::into_vec(
                #[rustc_box]
                ::alloc::boxed::Box::new([Some(1), Some(2), None, Some(4)]),
            ),
        ),
    );
    let inner_arr: ArrayRef = Arc::new(
        StructArray::try_from(
                <[_]>::into_vec(
                    #[rustc_box]
                    ::alloc::boxed::Box::new([
                        ("text", strings.clone()),
                        ("number", ints.clone()),
                    ]),
                ),
            )
            .unwrap(),
    );
    let arr = StructArray::try_from(
            <[_]>::into_vec(
                #[rustc_box]
                ::alloc::boxed::Box::new([("inner", inner_arr.clone())]),
            ),
        )
        .unwrap();
    let typed = <TypedStructArray<NestedStructArray>>::try_from(&arr as &dyn Array)
        .unwrap();
    let iter = ArrayIter::new(typed);
    for item in iter.flatten() {
        item.is_valid();
        item.is_null();
        item.is_inner_null();
        item.is_inner_valid();
        let nested = item.inner();
        let nested_opt = item.inner_opt();
        let text: &str = nested.text();
        let text: Option<&str> = nested.text_opt();
        let number: i32 = nested.number();
        let number: Option<i32> = nested.number_opt();
    }
}
