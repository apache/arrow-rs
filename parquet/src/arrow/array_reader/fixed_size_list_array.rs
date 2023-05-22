use std::cmp::Ordering;
use std::sync::Arc;

use crate::arrow::array_reader::ArrayReader;
use crate::errors::ParquetError;
use crate::errors::Result;
use arrow_array::FixedSizeListArray;
use arrow_array::{builder::BooleanBufferBuilder, new_empty_array, Array, ArrayRef};
use arrow_data::{transform::MutableArrayData, ArrayData};
use arrow_schema::DataType as ArrowType;

/// Implementation of fixed-size list array reader.
pub struct FixedSizeListArrayReader {
    item_reader: Box<dyn ArrayReader>,
    /// The number of child items in each row of the list array
    fixed_size: usize,
    data_type: ArrowType,
    /// The definition level at which this list is not null
    def_level: i16,
    /// The repetition level that corresponds to a new value in this array
    rep_level: i16,
    /// If the list is nullable
    nullable: bool,
}

impl FixedSizeListArrayReader {
    /// Construct fixed-size list array reader.
    pub fn new(
        item_reader: Box<dyn ArrayReader>,
        fixed_size: usize,
        data_type: ArrowType,
        def_level: i16,
        rep_level: i16,
        nullable: bool,
    ) -> Self {
        Self {
            item_reader,
            fixed_size,
            data_type,
            def_level,
            rep_level,
            nullable,
        }
    }
}

impl ArrayReader for FixedSizeListArrayReader {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        let size = self.item_reader.read_records(batch_size)?;
        Ok(size)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let next_batch_array = self.item_reader.consume_batch()?;
        if next_batch_array.len() == 0 {
            return Ok(new_empty_array(&self.data_type));
        }

        let def_levels = self
            .get_def_levels()
            .ok_or_else(|| general_err!("item_reader def levels are None"))?;
        let rep_levels = self
            .get_rep_levels()
            .ok_or_else(|| general_err!("item_reader rep levels are None"))?;

        if !rep_levels.is_empty() && rep_levels[0] != 0 {
            // This implies either the source data was invalid, or the leaf column
            // reader did not correctly delimit semantic records
            return Err(general_err!("first repetition level of batch must be 0"));
        }

        let mut validity = self
            .nullable
            .then(|| BooleanBufferBuilder::new(next_batch_array.len()));

        let data = next_batch_array.to_data();
        let mut child_data_builder =
            MutableArrayData::new(vec![&data], false, next_batch_array.len());

        // The number of seen child array items
        let mut child_items = 0;
        // The number of seen child array items that haven't been added to the data builder
        let mut pending_items = 0;
        // The total number of rows (valid and invalid) in the list array
        let mut list_len = 0;

        def_levels.iter().zip(rep_levels).try_for_each(|(d, r)| {
            match r.cmp(&self.rep_level) {
                Ordering::Greater => {
                    // Repetition level greater than current => already handled by inner array
                    if *d < self.def_level {
                        return Err(general_err!(
                            "Encountered repetition level too large for definition level"
                        ));
                    }
                }
                Ordering::Equal => {
                    // Item inside of the current list
                    child_items += 1;
                    pending_items += 1;
                }
                Ordering::Less => {
                    // Start of new list row
                    list_len += 1;

                    // Verify new row is aligned to the fixed list size
                    if child_items % self.fixed_size != 0 {
                        return Err(general_err!("Misaligned fixed-size list entries"));
                    }

                    if *d >= self.def_level {
                        // Valid list entry
                        if let Some(validity) = validity.as_mut() {
                            validity.append(true);
                        }
                        child_items += 1;
                        pending_items += 1;
                    } else {
                        // Null list entry

                        if pending_items > 0 {
                            // Flush pending child items
                            child_data_builder.extend(
                                0,
                                child_items - pending_items,
                                child_items,
                            );
                            pending_items = 0;
                        }
                        child_data_builder.extend_nulls(self.fixed_size);
                        child_items += self.fixed_size;

                        if let Some(validity) = validity.as_mut() {
                            validity.append(false);
                        }
                    }
                }
            }
            Ok(())
        })?;

        let child_data = if pending_items == child_items {
            // No null entries - can reuse original array
            next_batch_array.to_data()
        } else {
            if pending_items > 0 {
                child_data_builder.extend(0, child_items - pending_items, child_items);
            }
            child_data_builder.freeze()
        };

        let mut list_builder = ArrayData::builder(self.get_data_type().clone())
            .len(list_len)
            .add_child_data(child_data);

        if let Some(builder) = validity {
            list_builder = list_builder.null_bit_buffer(Some(builder.into()));
        }

        let list_data = unsafe { list_builder.build_unchecked() };

        let result_array = FixedSizeListArray::from(list_data);
        Ok(Arc::new(result_array))
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        self.item_reader.skip_records(num_records)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.item_reader.get_def_levels()
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.item_reader.get_rep_levels()
    }
}
