use std::{cmp::Ordering, sync::Arc};

use arrow_array::{
    builder::PrimitiveBuilder,
    cast::AsArray,
    types::{ArrowPrimitiveType, Decimal128Type, Decimal256Type, DecimalType},
    Array, ArrayRef, ArrowNativeTypeOp, PrimitiveArray,
};
use arrow_buffer::i256;
use arrow_buffer::ArrowNativeType;
use arrow_schema::ArrowError;
use num::{traits::AsPrimitive, NumCast, ToPrimitive};

use crate::CastOptions;

pub(crate) fn cast_integer_to_decimal<
    T: ArrowPrimitiveType,
    D: DecimalType + ArrowPrimitiveType<Native = M>,
    M,
>(
    array: &PrimitiveArray<T>,
    precision: u8,
    scale: i8,
    base: M,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    <T as ArrowPrimitiveType>::Native: AsPrimitive<M>,
    M: ArrowNativeTypeOp,
{
    let scale_factor = base.pow_checked(scale.unsigned_abs() as u32).map_err(|_| {
        ArrowError::CastError(format!(
            "Cannot cast to {:?}({}, {}). The scale causes overflow.",
            D::PREFIX,
            precision,
            scale,
        ))
    })?;

    let array = if scale < 0 {
        match cast_options.safe {
            true => array.unary_opt::<_, D>(|v| {
                v.as_().div_checked(scale_factor).ok().and_then(|v| {
                    (D::validate_decimal_precision(v, precision).is_ok()).then_some(v)
                })
            }),
            false => array.try_unary::<_, D, _>(|v| {
                v.as_()
                    .div_checked(scale_factor)
                    .and_then(|v| D::validate_decimal_precision(v, precision).map(|_| v))
            })?,
        }
    } else {
        match cast_options.safe {
            true => array.unary_opt::<_, D>(|v| {
                v.as_().mul_checked(scale_factor).ok().and_then(|v| {
                    (D::validate_decimal_precision(v, precision).is_ok()).then_some(v)
                })
            }),
            false => array.try_unary::<_, D, _>(|v| {
                v.as_()
                    .mul_checked(scale_factor)
                    .and_then(|v| D::validate_decimal_precision(v, precision).map(|_| v))
            })?,
        }
    };

    Ok(Arc::new(array.with_precision_and_scale(precision, scale)?))
}

pub(crate) fn cast_floating_point_to_decimal128<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    <T as ArrowPrimitiveType>::Native: AsPrimitive<f64>,
{
    let mul = 10_f64.powi(scale as i32);

    if cast_options.safe {
        array
            .unary_opt::<_, Decimal128Type>(|v| {
                (mul * v.as_())
                    .round()
                    .to_i128()
                    .filter(|v| Decimal128Type::validate_decimal_precision(*v, precision).is_ok())
            })
            .with_precision_and_scale(precision, scale)
            .map(|a| Arc::new(a) as ArrayRef)
    } else {
        array
            .try_unary::<_, Decimal128Type, _>(|v| {
                (mul * v.as_())
                    .round()
                    .to_i128()
                    .ok_or_else(|| {
                        ArrowError::CastError(format!(
                            "Cannot cast to {}({}, {}). Overflowing on {:?}",
                            Decimal128Type::PREFIX,
                            precision,
                            scale,
                            v
                        ))
                    })
                    .and_then(|v| {
                        Decimal128Type::validate_decimal_precision(v, precision).map(|_| v)
                    })
            })?
            .with_precision_and_scale(precision, scale)
            .map(|a| Arc::new(a) as ArrayRef)
    }
}

pub(crate) fn cast_floating_point_to_decimal256<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    <T as ArrowPrimitiveType>::Native: AsPrimitive<f64>,
{
    let mul = 10_f64.powi(scale as i32);

    if cast_options.safe {
        array
            .unary_opt::<_, Decimal256Type>(|v| {
                i256::from_f64((v.as_() * mul).round())
                    .filter(|v| Decimal256Type::validate_decimal_precision(*v, precision).is_ok())
            })
            .with_precision_and_scale(precision, scale)
            .map(|a| Arc::new(a) as ArrayRef)
    } else {
        array
            .try_unary::<_, Decimal256Type, _>(|v| {
                i256::from_f64((v.as_() * mul).round())
                    .ok_or_else(|| {
                        ArrowError::CastError(format!(
                            "Cannot cast to {}({}, {}). Overflowing on {:?}",
                            Decimal256Type::PREFIX,
                            precision,
                            scale,
                            v
                        ))
                    })
                    .and_then(|v| {
                        Decimal256Type::validate_decimal_precision(v, precision).map(|_| v)
                    })
            })?
            .with_precision_and_scale(precision, scale)
            .map(|a| Arc::new(a) as ArrayRef)
    }
}

pub(crate) fn cast_decimal_to_integer<D, T>(
    array: &dyn Array,
    base: D::Native,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    T: ArrowPrimitiveType,
    <T as ArrowPrimitiveType>::Native: NumCast,
    D: DecimalType + ArrowPrimitiveType,
    <D as ArrowPrimitiveType>::Native: ArrowNativeTypeOp + ToPrimitive,
{
    let array = array.as_primitive::<D>();

    let div: D::Native = base.pow_checked(scale as u32).map_err(|_| {
        ArrowError::CastError(format!(
            "Cannot cast to {:?}. The scale {} causes overflow.",
            D::PREFIX,
            scale,
        ))
    })?;

    let mut value_builder = PrimitiveBuilder::<T>::with_capacity(array.len());

    if cast_options.safe {
        for i in 0..array.len() {
            if array.is_null(i) {
                value_builder.append_null();
            } else {
                let v = array
                    .value(i)
                    .div_checked(div)
                    .ok()
                    .and_then(<T::Native as NumCast>::from::<D::Native>);

                value_builder.append_option(v);
            }
        }
    } else {
        for i in 0..array.len() {
            if array.is_null(i) {
                value_builder.append_null();
            } else {
                let v = array.value(i).div_checked(div)?;

                let value = <T::Native as NumCast>::from::<D::Native>(v).ok_or_else(|| {
                    ArrowError::CastError(format!(
                        "value of {:?} is out of range {}",
                        v,
                        T::DATA_TYPE
                    ))
                })?;

                value_builder.append_value(value);
            }
        }
    }
    Ok(Arc::new(value_builder.finish()))
}

// cast the decimal array to floating-point array
pub(crate) fn cast_decimal_to_float<D: DecimalType, T: ArrowPrimitiveType, F>(
    array: &dyn Array,
    op: F,
) -> Result<ArrayRef, ArrowError>
where
    F: Fn(D::Native) -> T::Native,
{
    let array = array.as_primitive::<D>();
    let array = array.unary::<_, T>(op);
    Ok(Arc::new(array))
}

/// A utility trait that provides checked conversions between
/// decimal types inspired by [`NumCast`]
pub(crate) trait DecimalCast: Sized {
    fn to_i128(self) -> Option<i128>;

    fn to_i256(self) -> Option<i256>;

    fn from_decimal<T: DecimalCast>(n: T) -> Option<Self>;
}

impl DecimalCast for i128 {
    fn to_i128(self) -> Option<i128> {
        Some(self)
    }

    fn to_i256(self) -> Option<i256> {
        Some(i256::from_i128(self))
    }

    fn from_decimal<T: DecimalCast>(n: T) -> Option<Self> {
        n.to_i128()
    }
}

impl DecimalCast for i256 {
    fn to_i128(self) -> Option<i128> {
        self.to_i128()
    }

    fn to_i256(self) -> Option<i256> {
        Some(self)
    }

    fn from_decimal<T: DecimalCast>(n: T) -> Option<Self> {
        n.to_i256()
    }
}

pub(crate) fn cast_decimal_to_decimal_error<I, O>(
    output_precision: u8,
    output_scale: i8,
) -> impl Fn(<I as ArrowPrimitiveType>::Native) -> ArrowError
where
    I: DecimalType,
    O: DecimalType,
    I::Native: DecimalCast + ArrowNativeTypeOp,
    O::Native: DecimalCast + ArrowNativeTypeOp,
{
    move |x: I::Native| {
        ArrowError::CastError(format!(
            "Cannot cast to {}({}, {}). Overflowing on {:?}",
            O::PREFIX,
            output_precision,
            output_scale,
            x
        ))
    }
}

pub(crate) fn convert_to_smaller_scale_decimal<I, O>(
    array: &PrimitiveArray<I>,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<O>, ArrowError>
where
    I: DecimalType,
    O: DecimalType,
    I::Native: DecimalCast + ArrowNativeTypeOp,
    O::Native: DecimalCast + ArrowNativeTypeOp,
{
    let error = cast_decimal_to_decimal_error::<I, O>(output_precision, output_scale);
    let div = I::Native::from_decimal(10_i128)
        .unwrap()
        .pow_checked((input_scale - output_scale) as u32)?;

    let half = div.div_wrapping(I::Native::from_usize(2).unwrap());
    let half_neg = half.neg_wrapping();

    let f = |x: I::Native| {
        // div is >= 10 and so this cannot overflow
        let d = x.div_wrapping(div);
        let r = x.mod_wrapping(div);

        // Round result
        let adjusted = match x >= I::Native::ZERO {
            true if r >= half => d.add_wrapping(I::Native::ONE),
            false if r <= half_neg => d.sub_wrapping(I::Native::ONE),
            _ => d,
        };
        O::Native::from_decimal(adjusted)
    };

    Ok(match cast_options.safe {
        true => array.unary_opt(f),
        false => array.try_unary(|x| f(x).ok_or_else(|| error(x)))?,
    })
}

pub(crate) fn convert_to_bigger_or_equal_scale_decimal<I, O>(
    array: &PrimitiveArray<I>,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<O>, ArrowError>
where
    I: DecimalType,
    O: DecimalType,
    I::Native: DecimalCast + ArrowNativeTypeOp,
    O::Native: DecimalCast + ArrowNativeTypeOp,
{
    let error = cast_decimal_to_decimal_error::<I, O>(output_precision, output_scale);
    let mul = O::Native::from_decimal(10_i128)
        .unwrap()
        .pow_checked((output_scale - input_scale) as u32)?;

    let f = |x| O::Native::from_decimal(x).and_then(|x| x.mul_checked(mul).ok());

    Ok(match cast_options.safe {
        true => array.unary_opt(f),
        false => array.try_unary(|x| f(x).ok_or_else(|| error(x)))?,
    })
}

// Only support one type of decimal cast operations
pub(crate) fn cast_decimal_to_decimal_same_type<T>(
    array: &PrimitiveArray<T>,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    T: DecimalType,
    T::Native: DecimalCast + ArrowNativeTypeOp,
{
    let array: PrimitiveArray<T> = match input_scale.cmp(&output_scale) {
        Ordering::Equal => {
            // the scale doesn't change, the native value don't need to be changed
            array.clone()
        }
        Ordering::Greater => convert_to_smaller_scale_decimal::<T, T>(
            array,
            input_scale,
            output_precision,
            output_scale,
            cast_options,
        )?,
        Ordering::Less => {
            // input_scale < output_scale
            convert_to_bigger_or_equal_scale_decimal::<T, T>(
                array,
                input_scale,
                output_precision,
                output_scale,
                cast_options,
            )?
        }
    };

    Ok(Arc::new(array.with_precision_and_scale(
        output_precision,
        output_scale,
    )?))
}

// Support two different types of decimal cast operations
pub(crate) fn cast_decimal_to_decimal<I, O>(
    array: &PrimitiveArray<I>,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    I: DecimalType,
    O: DecimalType,
    I::Native: DecimalCast + ArrowNativeTypeOp,
    O::Native: DecimalCast + ArrowNativeTypeOp,
{
    let array: PrimitiveArray<O> = if input_scale > output_scale {
        convert_to_smaller_scale_decimal::<I, O>(
            array,
            input_scale,
            output_precision,
            output_scale,
            cast_options,
        )?
    } else {
        convert_to_bigger_or_equal_scale_decimal::<I, O>(
            array,
            input_scale,
            output_precision,
            output_scale,
            cast_options,
        )?
    };

    Ok(Arc::new(array.with_precision_and_scale(
        output_precision,
        output_scale,
    )?))
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::display::FormatOptions;
    use crate::*;
    use arrow_array::*;
    use arrow_schema::DataType;

    macro_rules! generate_cast_test_case {
        ($INPUT_ARRAY: expr, $OUTPUT_TYPE_ARRAY: ident, $OUTPUT_TYPE: expr, $OUTPUT_VALUES: expr) => {
            let output =
                $OUTPUT_TYPE_ARRAY::from($OUTPUT_VALUES).with_data_type($OUTPUT_TYPE.clone());

            // assert cast type
            let input_array_type = $INPUT_ARRAY.data_type();
            assert!(can_cast_types(input_array_type, $OUTPUT_TYPE));
            let result = cast($INPUT_ARRAY, $OUTPUT_TYPE).unwrap();
            assert_eq!($OUTPUT_TYPE, result.data_type());
            assert_eq!(result.as_ref(), &output);

            let cast_option = CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            };
            let result = cast_with_options($INPUT_ARRAY, $OUTPUT_TYPE, &cast_option).unwrap();
            assert_eq!($OUTPUT_TYPE, result.data_type());
            assert_eq!(result.as_ref(), &output);
        };
    }

    fn create_decimal_array(
        array: Vec<Option<i128>>,
        precision: u8,
        scale: i8,
    ) -> Result<Decimal128Array, ArrowError> {
        array
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(precision, scale)
    }

    fn create_decimal256_array(
        array: Vec<Option<i256>>,
        precision: u8,
        scale: i8,
    ) -> Result<Decimal256Array, ArrowError> {
        array
            .into_iter()
            .collect::<Decimal256Array>()
            .with_precision_and_scale(precision, scale)
    }

    #[test]
    #[cfg(not(feature = "force_validate"))]
    #[should_panic(
        expected = "Cannot cast to Decimal128(20, 3). Overflowing on 57896044618658097711785492504343953926634992332820282019728792003956564819967"
    )]
    fn test_cast_decimal_to_decimal_round_with_error() {
        // decimal256 to decimal128 overflow

        use arrow_schema::DataType;
        let array = vec![
            Some(i256::from_i128(1123454)),
            Some(i256::from_i128(2123456)),
            Some(i256::from_i128(-3123453)),
            Some(i256::from_i128(-3123456)),
            None,
            Some(i256::MAX),
            Some(i256::MIN),
        ];
        let input_decimal_array = create_decimal256_array(array, 76, 4).unwrap();
        let array = Arc::new(input_decimal_array) as ArrayRef;
        let input_type = DataType::Decimal256(76, 4);
        let output_type = DataType::Decimal128(20, 3);
        assert!(can_cast_types(&input_type, &output_type));
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(112345_i128),
                Some(212346_i128),
                Some(-312345_i128),
                Some(-312346_i128),
                None,
                None,
                None,
            ]
        );
    }

    #[test]
    #[cfg(not(feature = "force_validate"))]
    fn test_cast_decimal_to_decimal_round() {
        use arrow_schema::DataType;

        let array = vec![
            Some(1123454),
            Some(2123456),
            Some(-3123453),
            Some(-3123456),
            None,
        ];
        let array = create_decimal_array(array, 20, 4).unwrap();
        // decimal128 to decimal128
        let input_type = DataType::Decimal128(20, 4);
        let output_type = DataType::Decimal128(20, 3);
        assert!(can_cast_types(&input_type, &output_type));
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(112345_i128),
                Some(212346_i128),
                Some(-312345_i128),
                Some(-312346_i128),
                None
            ]
        );

        // decimal128 to decimal256
        let input_type = DataType::Decimal128(20, 4);
        let output_type = DataType::Decimal256(20, 3);
        assert!(can_cast_types(&input_type, &output_type));
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &output_type,
            vec![
                Some(i256::from_i128(112345_i128)),
                Some(i256::from_i128(212346_i128)),
                Some(i256::from_i128(-312345_i128)),
                Some(i256::from_i128(-312346_i128)),
                None
            ]
        );

        // decimal256
        let array = vec![
            Some(i256::from_i128(1123454)),
            Some(i256::from_i128(2123456)),
            Some(i256::from_i128(-3123453)),
            Some(i256::from_i128(-3123456)),
            None,
        ];
        let array = create_decimal256_array(array, 20, 4).unwrap();

        // decimal256 to decimal256
        let input_type = DataType::Decimal256(20, 4);
        let output_type = DataType::Decimal256(20, 3);
        assert!(can_cast_types(&input_type, &output_type));
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &output_type,
            vec![
                Some(i256::from_i128(112345_i128)),
                Some(i256::from_i128(212346_i128)),
                Some(i256::from_i128(-312345_i128)),
                Some(i256::from_i128(-312346_i128)),
                None
            ]
        );
        // decimal256 to decimal128
        let input_type = DataType::Decimal256(20, 4);
        let output_type = DataType::Decimal128(20, 3);
        assert!(can_cast_types(&input_type, &output_type));
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(112345_i128),
                Some(212346_i128),
                Some(-312345_i128),
                Some(-312346_i128),
                None
            ]
        );
    }

    #[test]
    fn test_cast_decimal128_to_decimal128() {
        let input_type = DataType::Decimal128(20, 3);
        let output_type = DataType::Decimal128(20, 4);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
        let array = create_decimal_array(array, 20, 3).unwrap();
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(11234560_i128),
                Some(21234560_i128),
                Some(31234560_i128),
                None
            ]
        );
        // negative test
        let array = vec![Some(123456), None];
        let array = create_decimal_array(array, 10, 0).unwrap();
        let result = cast(&array, &DataType::Decimal128(2, 2));
        assert!(result.is_ok());
        let array = result.unwrap();
        let array: &Decimal128Array = array.as_primitive();
        let err = array.validate_decimal_precision(2);
        assert_eq!("Invalid argument error: 12345600 is too large to store in a Decimal128 of precision 2. Max is 99",
               err.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_decimal128_to_decimal128_overflow() {
        let input_type = DataType::Decimal128(38, 3);
        let output_type = DataType::Decimal128(38, 38);
        assert!(can_cast_types(&input_type, &output_type));

        let array = vec![Some(i128::MAX)];
        let array = create_decimal_array(array, 38, 3).unwrap();
        let result = cast_with_options(
            &array,
            &output_type,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!("Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 170141183460469231731687303715884105727",
               result.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_decimal128_to_decimal256_overflow() {
        let input_type = DataType::Decimal128(38, 3);
        let output_type = DataType::Decimal256(76, 76);
        assert!(can_cast_types(&input_type, &output_type));

        let array = vec![Some(i128::MAX)];
        let array = create_decimal_array(array, 38, 3).unwrap();
        let result = cast_with_options(
            &array,
            &output_type,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!("Cast error: Cannot cast to Decimal256(76, 76). Overflowing on 170141183460469231731687303715884105727",
               result.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_decimal128_to_decimal256() {
        let input_type = DataType::Decimal128(20, 3);
        let output_type = DataType::Decimal256(20, 4);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
        let array = create_decimal_array(array, 20, 3).unwrap();
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &output_type,
            vec![
                Some(i256::from_i128(11234560_i128)),
                Some(i256::from_i128(21234560_i128)),
                Some(i256::from_i128(31234560_i128)),
                None
            ]
        );
    }

    #[test]
    fn test_cast_decimal256_to_decimal128_overflow() {
        let input_type = DataType::Decimal256(76, 5);
        let output_type = DataType::Decimal128(38, 7);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(i256::from_i128(i128::MAX))];
        let array = create_decimal256_array(array, 76, 5).unwrap();
        let result = cast_with_options(
            &array,
            &output_type,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!("Cast error: Cannot cast to Decimal128(38, 7). Overflowing on 170141183460469231731687303715884105727",
               result.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_decimal256_to_decimal256_overflow() {
        let input_type = DataType::Decimal256(76, 5);
        let output_type = DataType::Decimal256(76, 55);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(i256::from_i128(i128::MAX))];
        let array = create_decimal256_array(array, 76, 5).unwrap();
        let result = cast_with_options(
            &array,
            &output_type,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!("Cast error: Cannot cast to Decimal256(76, 55). Overflowing on 170141183460469231731687303715884105727",
               result.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_decimal256_to_decimal128() {
        let input_type = DataType::Decimal256(20, 3);
        let output_type = DataType::Decimal128(20, 4);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![
            Some(i256::from_i128(1123456)),
            Some(i256::from_i128(2123456)),
            Some(i256::from_i128(3123456)),
            None,
        ];
        let array = create_decimal256_array(array, 20, 3).unwrap();
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(11234560_i128),
                Some(21234560_i128),
                Some(31234560_i128),
                None
            ]
        );
    }

    #[test]
    fn test_cast_decimal256_to_decimal256() {
        let input_type = DataType::Decimal256(20, 3);
        let output_type = DataType::Decimal256(20, 4);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![
            Some(i256::from_i128(1123456)),
            Some(i256::from_i128(2123456)),
            Some(i256::from_i128(3123456)),
            None,
        ];
        let array = create_decimal256_array(array, 20, 3).unwrap();
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &output_type,
            vec![
                Some(i256::from_i128(11234560_i128)),
                Some(i256::from_i128(21234560_i128)),
                Some(i256::from_i128(31234560_i128)),
                None
            ]
        );
    }

    #[test]
    fn test_cast_decimal_to_numeric() {
        let value_array: Vec<Option<i128>> = vec![Some(125), Some(225), Some(325), None, Some(525)];
        let array = create_decimal_array(value_array, 38, 2).unwrap();
        // u8
        generate_cast_test_case!(
            &array,
            UInt8Array,
            &DataType::UInt8,
            vec![Some(1_u8), Some(2_u8), Some(3_u8), None, Some(5_u8)]
        );
        // u16
        generate_cast_test_case!(
            &array,
            UInt16Array,
            &DataType::UInt16,
            vec![Some(1_u16), Some(2_u16), Some(3_u16), None, Some(5_u16)]
        );
        // u32
        generate_cast_test_case!(
            &array,
            UInt32Array,
            &DataType::UInt32,
            vec![Some(1_u32), Some(2_u32), Some(3_u32), None, Some(5_u32)]
        );
        // u64
        generate_cast_test_case!(
            &array,
            UInt64Array,
            &DataType::UInt64,
            vec![Some(1_u64), Some(2_u64), Some(3_u64), None, Some(5_u64)]
        );
        // i8
        generate_cast_test_case!(
            &array,
            Int8Array,
            &DataType::Int8,
            vec![Some(1_i8), Some(2_i8), Some(3_i8), None, Some(5_i8)]
        );
        // i16
        generate_cast_test_case!(
            &array,
            Int16Array,
            &DataType::Int16,
            vec![Some(1_i16), Some(2_i16), Some(3_i16), None, Some(5_i16)]
        );
        // i32
        generate_cast_test_case!(
            &array,
            Int32Array,
            &DataType::Int32,
            vec![Some(1_i32), Some(2_i32), Some(3_i32), None, Some(5_i32)]
        );
        // i64
        generate_cast_test_case!(
            &array,
            Int64Array,
            &DataType::Int64,
            vec![Some(1_i64), Some(2_i64), Some(3_i64), None, Some(5_i64)]
        );
        // f32
        generate_cast_test_case!(
            &array,
            Float32Array,
            &DataType::Float32,
            vec![
                Some(1.25_f32),
                Some(2.25_f32),
                Some(3.25_f32),
                None,
                Some(5.25_f32)
            ]
        );
        // f64
        generate_cast_test_case!(
            &array,
            Float64Array,
            &DataType::Float64,
            vec![
                Some(1.25_f64),
                Some(2.25_f64),
                Some(3.25_f64),
                None,
                Some(5.25_f64)
            ]
        );

        // overflow test: out of range of max u8
        let value_array: Vec<Option<i128>> = vec![Some(51300)];
        let array = create_decimal_array(value_array, 38, 2).unwrap();
        let casted_array = cast_with_options(
            &array,
            &DataType::UInt8,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!(
            "Cast error: value of 513 is out of range UInt8".to_string(),
            casted_array.unwrap_err().to_string()
        );

        let casted_array = cast_with_options(
            &array,
            &DataType::UInt8,
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        // overflow test: out of range of max i8
        let value_array: Vec<Option<i128>> = vec![Some(24400)];
        let array = create_decimal_array(value_array, 38, 2).unwrap();
        let casted_array = cast_with_options(
            &array,
            &DataType::Int8,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!(
            "Cast error: value of 244 is out of range Int8".to_string(),
            casted_array.unwrap_err().to_string()
        );

        let casted_array = cast_with_options(
            &array,
            &DataType::Int8,
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        // loss the precision: convert decimal to f32、f64
        // f32
        // 112345678_f32 and 112345679_f32 are same, so the 112345679_f32 will lose precision.
        let value_array: Vec<Option<i128>> = vec![
            Some(125),
            Some(225),
            Some(325),
            None,
            Some(525),
            Some(112345678),
            Some(112345679),
        ];
        let array = create_decimal_array(value_array, 38, 2).unwrap();
        generate_cast_test_case!(
            &array,
            Float32Array,
            &DataType::Float32,
            vec![
                Some(1.25_f32),
                Some(2.25_f32),
                Some(3.25_f32),
                None,
                Some(5.25_f32),
                Some(1_123_456.7_f32),
                Some(1_123_456.7_f32)
            ]
        );

        // f64
        // 112345678901234568_f64 and 112345678901234560_f64 are same, so the 112345678901234568_f64 will lose precision.
        let value_array: Vec<Option<i128>> = vec![
            Some(125),
            Some(225),
            Some(325),
            None,
            Some(525),
            Some(112345678901234568),
            Some(112345678901234560),
        ];
        let array = create_decimal_array(value_array, 38, 2).unwrap();
        generate_cast_test_case!(
            &array,
            Float64Array,
            &DataType::Float64,
            vec![
                Some(1.25_f64),
                Some(2.25_f64),
                Some(3.25_f64),
                None,
                Some(5.25_f64),
                Some(1_123_456_789_012_345.6_f64),
                Some(1_123_456_789_012_345.6_f64),
            ]
        );
    }

    #[test]
    fn test_cast_decimal256_to_numeric() {
        let value_array: Vec<Option<i256>> = vec![
            Some(i256::from_i128(125)),
            Some(i256::from_i128(225)),
            Some(i256::from_i128(325)),
            None,
            Some(i256::from_i128(525)),
        ];
        let array = create_decimal256_array(value_array, 38, 2).unwrap();
        // u8
        generate_cast_test_case!(
            &array,
            UInt8Array,
            &DataType::UInt8,
            vec![Some(1_u8), Some(2_u8), Some(3_u8), None, Some(5_u8)]
        );
        // u16
        generate_cast_test_case!(
            &array,
            UInt16Array,
            &DataType::UInt16,
            vec![Some(1_u16), Some(2_u16), Some(3_u16), None, Some(5_u16)]
        );
        // u32
        generate_cast_test_case!(
            &array,
            UInt32Array,
            &DataType::UInt32,
            vec![Some(1_u32), Some(2_u32), Some(3_u32), None, Some(5_u32)]
        );
        // u64
        generate_cast_test_case!(
            &array,
            UInt64Array,
            &DataType::UInt64,
            vec![Some(1_u64), Some(2_u64), Some(3_u64), None, Some(5_u64)]
        );
        // i8
        generate_cast_test_case!(
            &array,
            Int8Array,
            &DataType::Int8,
            vec![Some(1_i8), Some(2_i8), Some(3_i8), None, Some(5_i8)]
        );
        // i16
        generate_cast_test_case!(
            &array,
            Int16Array,
            &DataType::Int16,
            vec![Some(1_i16), Some(2_i16), Some(3_i16), None, Some(5_i16)]
        );
        // i32
        generate_cast_test_case!(
            &array,
            Int32Array,
            &DataType::Int32,
            vec![Some(1_i32), Some(2_i32), Some(3_i32), None, Some(5_i32)]
        );
        // i64
        generate_cast_test_case!(
            &array,
            Int64Array,
            &DataType::Int64,
            vec![Some(1_i64), Some(2_i64), Some(3_i64), None, Some(5_i64)]
        );
        // f32
        generate_cast_test_case!(
            &array,
            Float32Array,
            &DataType::Float32,
            vec![
                Some(1.25_f32),
                Some(2.25_f32),
                Some(3.25_f32),
                None,
                Some(5.25_f32)
            ]
        );
        // f64
        generate_cast_test_case!(
            &array,
            Float64Array,
            &DataType::Float64,
            vec![
                Some(1.25_f64),
                Some(2.25_f64),
                Some(3.25_f64),
                None,
                Some(5.25_f64)
            ]
        );

        // overflow test: out of range of max i8
        let value_array: Vec<Option<i256>> = vec![Some(i256::from_i128(24400))];
        let array = create_decimal256_array(value_array, 38, 2).unwrap();
        let casted_array = cast_with_options(
            &array,
            &DataType::Int8,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!(
            "Cast error: value of 244 is out of range Int8".to_string(),
            casted_array.unwrap_err().to_string()
        );

        let casted_array = cast_with_options(
            &array,
            &DataType::Int8,
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        // loss the precision: convert decimal to f32、f64
        // f32
        // 112345678_f32 and 112345679_f32 are same, so the 112345679_f32 will lose precision.
        let value_array: Vec<Option<i256>> = vec![
            Some(i256::from_i128(125)),
            Some(i256::from_i128(225)),
            Some(i256::from_i128(325)),
            None,
            Some(i256::from_i128(525)),
            Some(i256::from_i128(112345678)),
            Some(i256::from_i128(112345679)),
        ];
        let array = create_decimal256_array(value_array, 76, 2).unwrap();
        generate_cast_test_case!(
            &array,
            Float32Array,
            &DataType::Float32,
            vec![
                Some(1.25_f32),
                Some(2.25_f32),
                Some(3.25_f32),
                None,
                Some(5.25_f32),
                Some(1_123_456.7_f32),
                Some(1_123_456.7_f32)
            ]
        );

        // f64
        // 112345678901234568_f64 and 112345678901234560_f64 are same, so the 112345678901234568_f64 will lose precision.
        let value_array: Vec<Option<i256>> = vec![
            Some(i256::from_i128(125)),
            Some(i256::from_i128(225)),
            Some(i256::from_i128(325)),
            None,
            Some(i256::from_i128(525)),
            Some(i256::from_i128(112345678901234568)),
            Some(i256::from_i128(112345678901234560)),
        ];
        let array = create_decimal256_array(value_array, 76, 2).unwrap();
        generate_cast_test_case!(
            &array,
            Float64Array,
            &DataType::Float64,
            vec![
                Some(1.25_f64),
                Some(2.25_f64),
                Some(3.25_f64),
                None,
                Some(5.25_f64),
                Some(1_123_456_789_012_345.6_f64),
                Some(1_123_456_789_012_345.6_f64),
            ]
        );
    }

    #[test]
    fn test_cast_numeric_to_decimal128() {
        let decimal_type = DataType::Decimal128(38, 6);
        // u8, u16, u32, u64
        let input_datas = vec![
            Arc::new(UInt8Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u8
            Arc::new(UInt16Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u16
            Arc::new(UInt32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u32
            Arc::new(UInt64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u64
        ];

        for array in input_datas {
            generate_cast_test_case!(
                &array,
                Decimal128Array,
                &decimal_type,
                vec![
                    Some(1000000_i128),
                    Some(2000000_i128),
                    Some(3000000_i128),
                    None,
                    Some(5000000_i128)
                ]
            );
        }

        // i8, i16, i32, i64
        let input_datas = vec![
            Arc::new(Int8Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i8
            Arc::new(Int16Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i16
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i32
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i64
        ];
        for array in input_datas {
            generate_cast_test_case!(
                &array,
                Decimal128Array,
                &decimal_type,
                vec![
                    Some(1000000_i128),
                    Some(2000000_i128),
                    Some(3000000_i128),
                    None,
                    Some(5000000_i128)
                ]
            );
        }

        // test u8 to decimal type with overflow the result type
        // the 100 will be converted to 1000_i128, but it is out of range for max value in the precision 3.
        let array = UInt8Array::from(vec![1, 2, 3, 4, 100]);
        let casted_array = cast(&array, &DataType::Decimal128(3, 1));
        assert!(casted_array.is_ok());
        let array = casted_array.unwrap();
        let array: &Decimal128Array = array.as_primitive();
        assert!(array.is_null(4));

        // test i8 to decimal type with overflow the result type
        // the 100 will be converted to 1000_i128, but it is out of range for max value in the precision 3.
        let array = Int8Array::from(vec![1, 2, 3, 4, 100]);
        let casted_array = cast(&array, &DataType::Decimal128(3, 1));
        assert!(casted_array.is_ok());
        let array = casted_array.unwrap();
        let array: &Decimal128Array = array.as_primitive();
        assert!(array.is_null(4));

        // test f32 to decimal type
        let array = Float32Array::from(vec![
            Some(1.1),
            Some(2.2),
            Some(4.4),
            None,
            Some(1.123_456_4), // round down
            Some(1.123_456_7), // round up
        ]);
        let array = Arc::new(array) as ArrayRef;
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &decimal_type,
            vec![
                Some(1100000_i128),
                Some(2200000_i128),
                Some(4400000_i128),
                None,
                Some(1123456_i128), // round down
                Some(1123457_i128), // round up
            ]
        );

        // test f64 to decimal type
        let array = Float64Array::from(vec![
            Some(1.1),
            Some(2.2),
            Some(4.4),
            None,
            Some(1.123_456_489_123_4),     // round up
            Some(1.123_456_789_123_4),     // round up
            Some(1.123_456_489_012_345_6), // round down
            Some(1.123_456_789_012_345_6), // round up
        ]);
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &decimal_type,
            vec![
                Some(1100000_i128),
                Some(2200000_i128),
                Some(4400000_i128),
                None,
                Some(1123456_i128), // round down
                Some(1123457_i128), // round up
                Some(1123456_i128), // round down
                Some(1123457_i128), // round up
            ]
        );
    }

    #[test]
    fn test_cast_numeric_to_decimal256() {
        let decimal_type = DataType::Decimal256(76, 6);
        // u8, u16, u32, u64
        let input_datas = vec![
            Arc::new(UInt8Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u8
            Arc::new(UInt16Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u16
            Arc::new(UInt32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u32
            Arc::new(UInt64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u64
        ];

        for array in input_datas {
            generate_cast_test_case!(
                &array,
                Decimal256Array,
                &decimal_type,
                vec![
                    Some(i256::from_i128(1000000_i128)),
                    Some(i256::from_i128(2000000_i128)),
                    Some(i256::from_i128(3000000_i128)),
                    None,
                    Some(i256::from_i128(5000000_i128))
                ]
            );
        }

        // i8, i16, i32, i64
        let input_datas = vec![
            Arc::new(Int8Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i8
            Arc::new(Int16Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i16
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i32
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i64
        ];
        for array in input_datas {
            generate_cast_test_case!(
                &array,
                Decimal256Array,
                &decimal_type,
                vec![
                    Some(i256::from_i128(1000000_i128)),
                    Some(i256::from_i128(2000000_i128)),
                    Some(i256::from_i128(3000000_i128)),
                    None,
                    Some(i256::from_i128(5000000_i128))
                ]
            );
        }

        // test i8 to decimal type with overflow the result type
        // the 100 will be converted to 1000_i128, but it is out of range for max value in the precision 3.
        let array = Int8Array::from(vec![1, 2, 3, 4, 100]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast(&array, &DataType::Decimal256(3, 1));
        assert!(casted_array.is_ok());
        let array = casted_array.unwrap();
        let array: &Decimal256Array = array.as_primitive();
        assert!(array.is_null(4));

        // test f32 to decimal type
        let array = Float32Array::from(vec![
            Some(1.1),
            Some(2.2),
            Some(4.4),
            None,
            Some(1.123_456_4), // round down
            Some(1.123_456_7), // round up
        ]);
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &decimal_type,
            vec![
                Some(i256::from_i128(1100000_i128)),
                Some(i256::from_i128(2200000_i128)),
                Some(i256::from_i128(4400000_i128)),
                None,
                Some(i256::from_i128(1123456_i128)), // round down
                Some(i256::from_i128(1123457_i128)), // round up
            ]
        );

        // test f64 to decimal type
        let array = Float64Array::from(vec![
            Some(1.1),
            Some(2.2),
            Some(4.4),
            None,
            Some(1.123_456_489_123_4),     // round down
            Some(1.123_456_789_123_4),     // round up
            Some(1.123_456_489_012_345_6), // round down
            Some(1.123_456_789_012_345_6), // round up
        ]);
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &decimal_type,
            vec![
                Some(i256::from_i128(1100000_i128)),
                Some(i256::from_i128(2200000_i128)),
                Some(i256::from_i128(4400000_i128)),
                None,
                Some(i256::from_i128(1123456_i128)), // round down
                Some(i256::from_i128(1123457_i128)), // round up
                Some(i256::from_i128(1123456_i128)), // round down
                Some(i256::from_i128(1123457_i128)), // round up
            ]
        );
    }

    #[test]
    fn test_cast_decimal128_to_decimal128_negative_scale() {
        let input_type = DataType::Decimal128(20, 0);
        let output_type = DataType::Decimal128(20, -1);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(1123450), Some(2123455), Some(3123456), None];
        let input_decimal_array = create_decimal_array(array, 20, 0).unwrap();
        let array = Arc::new(input_decimal_array) as ArrayRef;
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(112345_i128),
                Some(212346_i128),
                Some(312346_i128),
                None
            ]
        );

        let casted_array = cast(&array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("1123450", decimal_arr.value_as_string(0));
        assert_eq!("2123460", decimal_arr.value_as_string(1));
        assert_eq!("3123460", decimal_arr.value_as_string(2));
    }

    #[test]
    fn test_cast_numeric_to_decimal128_negative() {
        let decimal_type = DataType::Decimal128(38, -1);
        let array = Arc::new(Int32Array::from(vec![
            Some(1123456),
            Some(2123456),
            Some(3123456),
        ])) as ArrayRef;

        let casted_array = cast(&array, &decimal_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("1123450", decimal_arr.value_as_string(0));
        assert_eq!("2123450", decimal_arr.value_as_string(1));
        assert_eq!("3123450", decimal_arr.value_as_string(2));

        let array = Arc::new(Float32Array::from(vec![
            Some(1123.456),
            Some(2123.456),
            Some(3123.456),
        ])) as ArrayRef;

        let casted_array = cast(&array, &decimal_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("1120", decimal_arr.value_as_string(0));
        assert_eq!("2120", decimal_arr.value_as_string(1));
        assert_eq!("3120", decimal_arr.value_as_string(2));
    }

    #[test]
    fn test_cast_decimal128_to_decimal128_negative() {
        let input_type = DataType::Decimal128(10, -1);
        let output_type = DataType::Decimal128(10, -2);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(123)];
        let input_decimal_array = create_decimal_array(array, 10, -1).unwrap();
        let array = Arc::new(input_decimal_array) as ArrayRef;
        generate_cast_test_case!(&array, Decimal128Array, &output_type, vec![Some(12_i128),]);

        let casted_array = cast(&array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("1200", decimal_arr.value_as_string(0));

        let array = vec![Some(125)];
        let input_decimal_array = create_decimal_array(array, 10, -1).unwrap();
        let array = Arc::new(input_decimal_array) as ArrayRef;
        generate_cast_test_case!(&array, Decimal128Array, &output_type, vec![Some(13_i128),]);

        let casted_array = cast(&array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("1300", decimal_arr.value_as_string(0));
    }

    #[test]
    fn test_cast_decimal128_to_decimal256_negative() {
        let input_type = DataType::Decimal128(10, 3);
        let output_type = DataType::Decimal256(10, 5);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(i128::MAX), Some(i128::MIN)];
        let input_decimal_array = create_decimal_array(array, 10, 3).unwrap();
        let array = Arc::new(input_decimal_array) as ArrayRef;

        let hundred = i256::from_i128(100);
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &output_type,
            vec![
                Some(i256::from_i128(i128::MAX).mul_wrapping(hundred)),
                Some(i256::from_i128(i128::MIN).mul_wrapping(hundred))
            ]
        );
    }

    #[test]
    fn test_cast_f64_to_decimal128() {
        // to reproduce https://github.com/apache/arrow-rs/issues/2997

        let decimal_type = DataType::Decimal128(18, 2);
        let array = Float64Array::from(vec![
            Some(0.0699999999),
            Some(0.0659999999),
            Some(0.0650000000),
            Some(0.0649999999),
        ]);
        let array = Arc::new(array) as ArrayRef;
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &decimal_type,
            vec![
                Some(7_i128), // round up
                Some(7_i128), // round up
                Some(7_i128), // round up
                Some(6_i128), // round down
            ]
        );

        let decimal_type = DataType::Decimal128(18, 3);
        let array = Float64Array::from(vec![
            Some(0.0699999999),
            Some(0.0659999999),
            Some(0.0650000000),
            Some(0.0649999999),
        ]);
        let array = Arc::new(array) as ArrayRef;
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &decimal_type,
            vec![
                Some(70_i128), // round up
                Some(66_i128), // round up
                Some(65_i128), // round down
                Some(65_i128), // round up
            ]
        );
    }

    #[test]
    fn test_cast_numeric_to_decimal128_overflow() {
        let array = Int64Array::from(vec![i64::MAX]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(38, 30),
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(38, 30),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_err());
    }

    #[test]
    fn test_cast_numeric_to_decimal256_overflow() {
        let array = Int64Array::from(vec![i64::MAX]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(76, 76),
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(76, 76),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_err());
    }

    #[test]
    fn test_cast_floating_point_to_decimal128_precision_overflow() {
        let array = Float64Array::from(vec![1.1]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(2, 2),
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(2, 2),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        let err = casted_array.unwrap_err().to_string();
        let expected_error = "Invalid argument error: 110 is too large to store in a Decimal128 of precision 2. Max is 99";
        assert!(
            err.contains(expected_error),
            "did not find expected error '{expected_error}' in actual error '{err}'"
        );
    }

    #[test]
    fn test_cast_floating_point_to_decimal256_precision_overflow() {
        let array = Float64Array::from(vec![1.1]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(2, 2),
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(2, 2),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        let err = casted_array.unwrap_err().to_string();
        let expected_error = "Invalid argument error: 110 is too large to store in a Decimal256 of precision 2. Max is 99";
        assert!(
            err.contains(expected_error),
            "did not find expected error '{expected_error}' in actual error '{err}'"
        );
    }

    #[test]
    fn test_cast_floating_point_to_decimal128_overflow() {
        let array = Float64Array::from(vec![f64::MAX]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(38, 30),
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(38, 30),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        let err = casted_array.unwrap_err().to_string();
        let expected_error = "Cast error: Cannot cast to Decimal128(38, 30)";
        assert!(
            err.contains(expected_error),
            "did not find expected error '{expected_error}' in actual error '{err}'"
        );
    }

    #[test]
    fn test_cast_floating_point_to_decimal256_overflow() {
        let array = Float64Array::from(vec![f64::MAX]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(76, 50),
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(76, 50),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        let err = casted_array.unwrap_err().to_string();
        let expected_error = "Cast error: Cannot cast to Decimal256(76, 50)";
        assert!(
            err.contains(expected_error),
            "did not find expected error '{expected_error}' in actual error '{err}'"
        );
    }

    #[test]
    fn test_cast_decimal_to_utf8() {
        fn test_decimal_to_string<IN: ArrowPrimitiveType, OffsetSize: OffsetSizeTrait>(
            output_type: DataType,
            array: PrimitiveArray<IN>,
        ) {
            let b = cast(&array, &output_type).unwrap();

            assert_eq!(b.data_type(), &output_type);
            let c = b.as_string::<OffsetSize>();

            assert_eq!("1123.454", c.value(0));
            assert_eq!("2123.456", c.value(1));
            assert_eq!("-3123.453", c.value(2));
            assert_eq!("-3123.456", c.value(3));
            assert_eq!("0.000", c.value(4));
            assert_eq!("0.123", c.value(5));
            assert_eq!("1234.567", c.value(6));
            assert_eq!("-1234.567", c.value(7));
            assert!(c.is_null(8));
        }
        let array128: Vec<Option<i128>> = vec![
            Some(1123454),
            Some(2123456),
            Some(-3123453),
            Some(-3123456),
            Some(0),
            Some(123),
            Some(123456789),
            Some(-123456789),
            None,
        ];

        let array256: Vec<Option<i256>> = array128.iter().map(|v| v.map(i256::from_i128)).collect();

        test_decimal_to_string::<arrow_array::types::Decimal128Type, i32>(
            DataType::Utf8,
            create_decimal_array(array128.clone(), 7, 3).unwrap(),
        );
        test_decimal_to_string::<arrow_array::types::Decimal128Type, i64>(
            DataType::LargeUtf8,
            create_decimal_array(array128, 7, 3).unwrap(),
        );
        test_decimal_to_string::<arrow_array::types::Decimal256Type, i32>(
            DataType::Utf8,
            create_decimal256_array(array256.clone(), 7, 3).unwrap(),
        );
        test_decimal_to_string::<arrow_array::types::Decimal256Type, i64>(
            DataType::LargeUtf8,
            create_decimal256_array(array256, 7, 3).unwrap(),
        );
    }
}
