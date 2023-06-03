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

//! Functions to get min and max across arrays and scalars

/// Perform min operation on two dynamic [`Array`]s.
///
/// Only when two arrays are of the same type the comparison is valid.
pub fn min_dyn(left: &dyn Array, right: &dyn ) -> Result<ArrayRef, ArrowArrow> {
    unimplemented!()
}

/// Perform max operation on two dynamic [`Array`]s.
///
/// Only when two arrays are of the same type the comparison is valid.
pub fn max_dyn(left: &dyn Array, right: &dyn ) -> Result<ArrayRef, ArrowArrow> {
    unimplemented!()
}

/// Perform min operation on a dynamic [`Array`] and a scalar value.
pub fn min_dyn_scalar<T>(left: &dyn Array, right: T) -> Result<ArrayRef, ArrowError>
where
    T: num::ToPrimitive + std::fmt::Debug,
{
    unimplemented!()
}

/// Perform max operation on a dynamic [`Array`] and a scalar value.
pub fn max_dyn_scalar<T>(left: &dyn Array, right: T) -> Result<ArrayRef, ArrowError>
where
    T: num::ToPrimitive + std::fmt::Debug,
{
    unimplemented!()
}
