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

//! The Arrow tree a top-level array's Parquet leaves are generated from.
//!
//! One [`LevelTree`] resolves a schema tree once: every node appears exactly
//! once and owns the buffers the walk reads, and every subtree owns a
//! contiguous range of leaf outputs. A range walk descends
//! the tree and fans each run out to the leaves below it. Branches through run
//! ends or non-leaf dictionary values cannot be walked in ranges, so they are
//! projected back out into a linear view and visited one row at a time.

use super::{
    FieldContract, LevelContext, LevelData, is_leaf, leaf_types_compatible, normalized,
    plan::{LeafBatch, ValueSelection},
};
use crate::arrow::schema::validate_map_key_type;
use crate::column::value_selection::{
    DictionaryKeys, GroupedSelectionRef, RunEnds, ValueSelectionRef,
};
use crate::column::writer::LevelDataRef;
use crate::errors::{ParquetError, Result};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef};
use arrow_buffer::{ArrowNativeType, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use std::ops::Range;

/// One top-level Arrow array resolved into the tree its Parquet leaves are
/// generated from.
///
/// Every schema node appears exactly once and owns the Arrow buffers the walk
/// reads. A node's immediate children occupy a contiguous block of
/// [`Self::nodes`], and leaves are numbered in Parquet order, so every subtree
/// owns a contiguous range of [`Self::leaves`]. That is what lets the walk fan
/// a run out to "every leaf below this node" with a slice index rather than a
/// search, and what lets one structural traversal feed several column writers.
#[derive(Debug)]
pub(crate) struct LevelTree {
    nodes: Box<[TreeNode]>,
    leaves: Box<[TreeLeaf]>,
    root: ArrayRef,
    /// `None` means every leaf should retain its independent fast path.
    write_windows: Option<Box<[Range<u32>]>>,
}

/// One node of the resolved tree.
#[derive(Debug)]
struct TreeNode {
    kind: TreeKind,
    /// The array at this node, retained for the indexed walker: it binds a
    /// linear view of one branch when its cursor is created.
    array: ArrayRef,
    /// Logical validity, bound only for nodes a range walk can reach. Nodes at
    /// or below a run-end or dictionary node are left `None`; expanding their
    /// logical validity would cost a full-length bitmap, and the indexed walker
    /// reads the physical buffers instead.
    nulls: Option<NullBuffer>,
    nullable: bool,
    name: Box<str>,
    /// Child nodes, contiguous in [`LevelTree::nodes`].
    children: Range<u32>,
    /// Leaves below this node, contiguous in [`LevelTree::leaves`].
    leaves: Range<u32>,
    /// Every leaf below this node can be traversed in ranges.
    direct: bool,
}

/// One Parquet leaf: the output stream a walk feeds.
#[derive(Debug)]
struct TreeLeaf {
    /// The values array a selection indexes into.
    terminal: ArrayRef,
    max_def_level: i16,
    max_rep_level: i16,
    /// Preceding leaves that own identical level streams. These stay empty for
    /// a tree whose leaves all take independent paths.
    level_links: LevelGroups,
    /// Node indices from the root down to this leaf. Only the indexed walker
    /// needs it; a range walk descends the tree itself.
    branch: Box<[u32]>,
    /// The branch passes through a run-end or non-leaf dictionary node, so it
    /// is walked one row at a time rather than in ranges.
    indexed: bool,
}

/// What a node contributes to the walk.
#[derive(Debug)]
enum TreeKind {
    /// A primitive leaf, an all-null array, or a dictionary whose values are a
    /// leaf. A range walk treats all three the same: the row index is the
    /// physical value position and the node's validity decides emission.
    Leaf,
    Null,
    DictionaryLeaf,
    /// A dictionary over a non-leaf. Indexed traversal only.
    Dictionary,
    /// Run-end encoded. Indexed traversal only.
    RunEndEncoded,
    Struct,
    List(ListBounds),
}

/// A list node's row bounds, owned by the tree.
///
/// Maps share the offset layout of a 32-bit list and are stored as one; only
/// the child array differs, and that is already a separate node.
#[derive(Debug)]
enum ListBounds {
    Offsets32(OffsetBuffer<i32>),
    Offsets64(OffsetBuffer<i64>),
    Fixed(usize),
    View32 {
        offsets: ScalarBuffer<i32>,
        sizes: ScalarBuffer<i32>,
    },
    View64 {
        offsets: ScalarBuffer<i64>,
        sizes: ScalarBuffer<i64>,
    },
}

impl LevelTree {
    /// Resolve `array` against `field` into the tree its leaves are written from.
    pub(crate) fn build(field: &Field, array: &ArrayRef) -> Result<Self> {
        validate_map_key_type(field.data_type())?;
        let mut builder = TreeBuilder {
            nodes: vec![None],
            leaves: Vec::new(),
            branch: Vec::new(),
        };
        builder.fill(
            0,
            array.clone(),
            normalized(field),
            LevelContext::default(),
            false,
            LevelGroups::default(),
        )?;
        let TreeBuilder {
            nodes,
            leaves,
            branch: _,
        } = builder;
        let nodes: Box<[TreeNode]> = nodes
            .into_iter()
            .map(|node| node.expect("every reserved node is filled"))
            .collect();
        let mut tree = Self {
            nodes,
            leaves: leaves.into_boxed_slice(),
            root: array.clone(),
            write_windows: None,
        };
        if tree.leaves.len() > 1 && tree.has_shared_window(0, false) {
            // Immediate predecessor links let any contiguous write window
            // select its first matching leaf as owner without a search or a
            // hash table.
            let mut previous = vec![(None, None); tree.nodes.len()];
            for (index, leaf) in tree.leaves.iter_mut().enumerate() {
                leaf.level_links.definition = leaf
                    .level_links
                    .definition
                    .and_then(|group| previous[group as usize].0.replace(index as u32));
                leaf.level_links.repetition = leaf
                    .level_links
                    .repetition
                    .and_then(|group| previous[group as usize].1.replace(index as u32));
            }
            let mut windows = Vec::new();
            tree.collect_write_windows(0, false, &mut windows);
            tree.write_windows = Some(windows.into_boxed_slice());
        } else {
            for leaf in &mut tree.leaves {
                leaf.level_links = LevelGroups::default();
            }
        }
        Ok(tree)
    }

    pub(crate) fn leaf_count(&self) -> usize {
        self.leaves.len()
    }

    /// The values array leaf `leaf`'s selection indexes into.
    pub(crate) fn terminal(&self, leaf: u32) -> &(dyn Array + 'static) {
        self.leaves[leaf as usize].terminal.as_ref()
    }

    /// Partition the leaves into maximal windows that should share a walk.
    ///
    /// Lists always have row structure worth sharing. A struct is worth
    /// sharing when its validity has both null and valid rows; otherwise it
    /// contributes no scan, or collapses to one constant run. An indexed node
    /// is a local barrier, so disjoint direct descendants form their own
    /// windows and retain any structural work shared above the barrier.
    /// Returns `None` when every leaf should use its independent fast path.
    pub(crate) fn write_windows(&self) -> Option<&[Range<u32>]> {
        self.write_windows.as_deref()
    }

    fn has_shared_window(&self, index: u32, shared_prefix: bool) -> bool {
        let node = &self.nodes[index as usize];
        let shares_work = self.shares_work(node, shared_prefix);
        (node.direct && node.leaves.len() > 1 && shares_work)
            || node
                .children
                .clone()
                .any(|child| self.has_shared_window(child, shares_work))
    }

    fn collect_write_windows(
        &self,
        index: u32,
        shared_prefix: bool,
        windows: &mut Vec<Range<u32>>,
    ) {
        let node = &self.nodes[index as usize];
        let shares_work = self.shares_work(node, shared_prefix);
        if node.direct && node.leaves.len() > 1 && shares_work {
            windows.push(node.leaves.clone());
            return;
        }
        if node.children.is_empty() {
            windows.push(node.leaves.clone());
            return;
        }
        for child in node.children.clone() {
            self.collect_write_windows(child, shares_work, windows);
        }
    }

    fn shares_work(&self, node: &TreeNode, shared_prefix: bool) -> bool {
        shared_prefix
            || match node.kind {
                TreeKind::List(_) => true,
                TreeKind::Struct => node
                    .nulls
                    .as_ref()
                    .is_some_and(|nulls| nulls.null_count() < nulls.len()),
                _ => false,
            }
    }

    /// A cursor producing tiles for the contiguous leaf window `window`.
    pub(crate) fn cursor(
        &self,
        window: Range<u32>,
        target_slots: usize,
        target_rows: usize,
    ) -> Result<LeafCursor<'_>> {
        debug_assert!(!window.is_empty());
        debug_assert!(window.end as usize <= self.leaves.len());
        let leaves = &self.leaves[window.start as usize..window.end as usize];
        debug_assert!(leaves.len() == 1 || leaves.iter().all(|leaf| !leaf.indexed));
        // Indexed traversal visits one row per call, so its branch is bound to
        // the Arrow buffers once here rather than re-resolved for every row.
        // One leaf has no fan-out to share, so its stored branch is walked
        // directly, the way the indexed branch is.
        let branch = if leaves.len() == 1 && !leaves[0].indexed {
            Some(match leaves[0].branch.as_ref() {
                &[index] => DirectBranch::One(&self.nodes[index as usize]),
                indices => DirectBranch::Many(
                    indices
                        .iter()
                        .map(|&index| &self.nodes[index as usize])
                        .collect(),
                ),
            })
        } else {
            None
        };
        let indexed = leaves[0]
            .indexed
            .then(|| -> Result<_> {
                debug_assert_eq!(leaves.len(), 1);
                Ok(IndexedState {
                    bound: bind_indexed_branch(self, &leaves[0])?,
                })
            })
            .transpose()?;
        let tiles = if leaves.len() == 1 {
            let leaf = &leaves[0];
            vec![LeafTile::new(
                leaf.max_def_level,
                leaf.max_rep_level,
                leaf.indexed,
                None,
                None,
            )]
            .into_boxed_slice()
        } else {
            let mut tiles: Vec<LeafTile> = Vec::with_capacity(leaves.len());
            for leaf in leaves {
                let def_owner = leaf
                    .level_links
                    .definition
                    .filter(|&owner| owner >= window.start)
                    .map(|owner| {
                        let predecessor = (owner - window.start) as usize;
                        tiles[predecessor].def_owner.unwrap_or(predecessor as u32)
                    });
                let rep_owner = leaf
                    .level_links
                    .repetition
                    .filter(|&owner| owner >= window.start)
                    .map(|owner| {
                        let predecessor = (owner - window.start) as usize;
                        tiles[predecessor].rep_owner.unwrap_or(predecessor as u32)
                    });
                tiles.push(LeafTile::new(
                    leaf.max_def_level,
                    leaf.max_rep_level,
                    leaf.indexed,
                    def_owner,
                    rep_owner,
                ));
            }
            tiles.into_boxed_slice()
        };
        Ok(LeafCursor {
            tree: self,
            window,
            next_row: 0,
            target_slots: target_slots.max(1),
            // A lone range walk with one slot per row needs no tiling. Grouped
            // walks keep the row limit to bound all live leaf tiles together;
            // indexed walks need it to admit a new data page.
            target_rows: if indexed.is_none() && leaves.len() == 1 && leaves[0].max_rep_level == 0 {
                usize::MAX
            } else {
                target_rows.max(1)
            },
            tiles,
            indexed,
            branch,
        })
    }
}

/// Per-cursor state for a branch that must be walked one row at a time.
struct IndexedState<'a> {
    bound: Box<[BoundNode<'a>]>,
}

/// A resolved direct branch. Flat leaves stay inline; nested branches pay one
/// allocation to avoid indexing the node arena inside repeated run walks.
enum DirectBranch<'a> {
    One(&'a TreeNode),
    Many(Box<[&'a TreeNode]>),
}

impl<'a> DirectBranch<'a> {
    fn as_slice(&self) -> &[&'a TreeNode] {
        match self {
            Self::One(node) => std::slice::from_ref(node),
            Self::Many(nodes) => nodes,
        }
    }
}

struct TreeBuilder {
    /// Reserved slots; a node is filled after its descendants are appended.
    nodes: Vec<Option<TreeNode>>,
    leaves: Vec<TreeLeaf>,
    /// Node indices from the root to the node being filled.
    branch: Vec<u32>,
}

#[derive(Debug, Clone, Copy, Default)]
struct LevelGroups {
    definition: Option<u32>,
    repetition: Option<u32>,
}

impl TreeBuilder {
    /// Fill the reserved slot `me`, appending its descendants after it.
    fn fill(
        &mut self,
        me: u32,
        array: ArrayRef,
        contract: FieldContract<'_>,
        ctx: LevelContext,
        indexed: bool,
        groups: LevelGroups,
    ) -> Result<()> {
        let classified = classify_node(array.as_ref(), contract)?;
        // Required structs and leaves cannot alter a definition stream, so
        // their descendants retain the last nullable or repeated node as a
        // proof that their streams are identical.
        let groups = if contract.nullable || matches!(classified, NodeKind::List(_)) {
            LevelGroups {
                definition: Some(me),
                ..groups
            }
        } else {
            groups
        };
        self.branch.push(me);
        let leaf_start = self.leaves.len() as u32;

        // Children are reserved as one block before any of them is filled, so
        // that a node's children stay contiguous however deep they recurse.
        let children_start = self.nodes.len() as u32;
        let mut child_count = 0;
        let kind = match classified {
            NodeKind::Null | NodeKind::Leaf | NodeKind::DictionaryLeaf => {
                self.leaves.push(TreeLeaf {
                    terminal: array.clone(),
                    max_def_level: ctx.def_level + contract.nullable as i16,
                    max_rep_level: ctx.rep_level,
                    level_links: groups,
                    branch: self.branch.clone().into_boxed_slice(),
                    indexed,
                });
                match classified {
                    NodeKind::Null => TreeKind::Null,
                    NodeKind::DictionaryLeaf => TreeKind::DictionaryLeaf,
                    _ => TreeKind::Leaf,
                }
            }
            NodeKind::Struct => {
                let DataType::Struct(fields) = contract.data_type else {
                    unreachable!("struct contract was validated during planning")
                };
                let columns = array.as_struct().columns();
                child_count = columns.len() as u32;
                self.nodes
                    .resize_with(self.nodes.len() + columns.len(), || None);
                let child_ctx = LevelContext {
                    def_level: ctx.def_level + contract.nullable as i16,
                    ..ctx
                };
                for (ordinal, (child, child_field)) in columns.iter().zip(fields).enumerate() {
                    let slot = children_start + ordinal as u32;
                    self.fill(
                        slot,
                        child.clone(),
                        normalized(child_field),
                        child_ctx,
                        indexed,
                        groups,
                    )?;
                }
                TreeKind::Struct
            }
            NodeKind::List(list_kind) => {
                let (bounds, child) = bind_list_bounds(list_kind, array.as_ref());
                child_count = 1;
                self.nodes.push(None);
                self.fill(
                    children_start,
                    child,
                    normalized(list_kind.field(contract)),
                    LevelContext {
                        def_level: ctx.def_level + contract.nullable as i16 + 1,
                        rep_level: ctx.rep_level + 1,
                    },
                    indexed,
                    LevelGroups {
                        repetition: Some(me),
                        ..groups
                    },
                )?;
                TreeKind::List(bounds)
            }
            NodeKind::RunEndEncoded => {
                let (_, _, values) = super::super::run_ends_of(array.as_ref())?;
                let values = values.clone();
                child_count = 1;
                self.nodes.push(None);
                self.fill(children_start, values, contract, ctx, true, groups)?;
                TreeKind::RunEndEncoded
            }
            NodeKind::Dictionary => {
                let values = array.as_any_dictionary().values().clone();
                child_count = 1;
                self.nodes.push(None);
                self.fill(children_start, values, contract, ctx, true, groups)?;
                TreeKind::Dictionary
            }
        };

        // A range walk never reaches a run-end or dictionary node, and asking
        // one for its logical validity would expand a bitmap it will not read.
        let range_walked =
            !indexed && !matches!(kind, TreeKind::RunEndEncoded | TreeKind::Dictionary);
        let nulls = range_walked
            .then(|| array.logical_nulls())
            .flatten()
            .filter(|nulls| nulls.null_count() != 0);

        let direct = self.leaves[leaf_start as usize..]
            .iter()
            .all(|leaf| !leaf.indexed);
        self.nodes[me as usize] = Some(TreeNode {
            kind,
            array,
            nulls,
            nullable: contract.nullable,
            name: contract.name.into(),
            children: children_start..children_start + child_count,
            leaves: leaf_start..self.leaves.len() as u32,
            direct,
        });
        self.branch.pop();
        Ok(())
    }
}

/// Resolve a list node's row bounds and its child array.
fn bind_list_bounds(kind: ListKind, array: &dyn Array) -> (ListBounds, ArrayRef) {
    match kind {
        ListKind::List => {
            let list = array.as_list::<i32>();
            (
                ListBounds::Offsets32(list.offsets().clone()),
                list.values().clone(),
            )
        }
        ListKind::LargeList => {
            let list = array.as_list::<i64>();
            (
                ListBounds::Offsets64(list.offsets().clone()),
                list.values().clone(),
            )
        }
        ListKind::FixedSizeList => {
            let list = array.as_fixed_size_list();
            (
                ListBounds::Fixed(list.value_length() as usize),
                list.values().clone(),
            )
        }
        ListKind::ListView => {
            let list = array.as_list_view::<i32>();
            (
                ListBounds::View32 {
                    offsets: list.offsets().clone(),
                    sizes: list.sizes().clone(),
                },
                list.values().clone(),
            )
        }
        ListKind::LargeListView => {
            let list = array.as_list_view::<i64>();
            (
                ListBounds::View64 {
                    offsets: list.offsets().clone(),
                    sizes: list.sizes().clone(),
                },
                list.values().clone(),
            )
        }
        ListKind::Map => {
            let map = array.as_map();
            (
                ListBounds::Offsets32(map.offsets().clone()),
                std::sync::Arc::new(map.entries().clone()),
            )
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ListKind {
    List,
    LargeList,
    FixedSizeList,
    ListView,
    LargeListView,
    Map,
}

impl ListKind {
    fn try_new(actual: &DataType, contract: FieldContract<'_>) -> Result<Self> {
        Ok(match (actual, contract.data_type) {
            (DataType::List(_), DataType::List(_)) => Self::List,
            (DataType::LargeList(_), DataType::LargeList(_)) => Self::LargeList,
            (DataType::FixedSizeList(_, a), DataType::FixedSizeList(_, e)) if a == e => {
                Self::FixedSizeList
            }
            (DataType::ListView(_), DataType::ListView(_)) => Self::ListView,
            (DataType::LargeListView(_), DataType::LargeListView(_)) => Self::LargeListView,
            (DataType::Map(_, a), DataType::Map(_, e)) if a == e => Self::Map,
            _ => return Err(incompatible(contract, actual)),
        })
    }

    fn field(self, contract: FieldContract<'_>) -> &Field {
        match (self, contract.data_type) {
            (Self::List, DataType::List(field))
            | (Self::LargeList, DataType::LargeList(field))
            | (Self::FixedSizeList, DataType::FixedSizeList(field, _))
            | (Self::ListView, DataType::ListView(field))
            | (Self::LargeListView, DataType::LargeListView(field))
            | (Self::Map, DataType::Map(field, _)) => field,
            _ => unreachable!("list kind was validated during planning"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum NodeKind {
    RunEndEncoded,
    Dictionary,
    Null,
    Leaf,
    DictionaryLeaf,
    Struct,
    List(ListKind),
}

fn classify_node(array: &dyn Array, contract: FieldContract<'_>) -> Result<NodeKind> {
    Ok(match array.data_type() {
        DataType::RunEndEncoded(_, _) => NodeKind::RunEndEncoded,
        DataType::Dictionary(_, value) if is_leaf(value) => {
            if !leaf_types_compatible(contract.data_type, value) {
                return Err(incompatible(contract, value));
            }
            NodeKind::DictionaryLeaf
        }
        DataType::Dictionary(_, _) => NodeKind::Dictionary,
        actual if is_leaf(actual) => {
            if !leaf_types_compatible(contract.data_type, actual) {
                return Err(incompatible(contract, actual));
            }
            if matches!(actual, DataType::Null) {
                NodeKind::Null
            } else {
                NodeKind::Leaf
            }
        }
        DataType::Struct(actual) => {
            let DataType::Struct(expected) = contract.data_type else {
                return Err(incompatible(contract, array.data_type()));
            };
            if actual.len() != expected.len() {
                return Err(arrow_err!(
                    "Incompatible struct field '{}': expected {} children, got {}",
                    contract.name,
                    expected.len(),
                    actual.len()
                ));
            }
            NodeKind::Struct
        }
        actual if actual.is_list() || matches!(actual, DataType::Map(_, _)) => {
            NodeKind::List(ListKind::try_new(actual, contract)?)
        }
        actual => {
            return Err(nyi_err!(format!(
                "Datatype {actual} is not supported by recursive leaf cursor"
            )));
        }
    })
}

/// Compact level storage for indexed traversal, which appends one slot at a time.
#[derive(Debug)]
struct ScalarLevels {
    enabled: bool,
    uniform: Option<i16>,
    len: usize,
    values: Vec<i16>,
}

impl ScalarLevels {
    fn new(enabled: bool) -> Self {
        Self {
            enabled,
            uniform: None,
            len: 0,
            values: Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.uniform = None;
        self.len = 0;
        self.values.clear();
    }

    #[inline]
    fn push(&mut self, value: i16) {
        if !self.enabled {
            return;
        }
        match self.uniform {
            None if self.len == 0 => self.uniform = Some(value),
            Some(uniform) if uniform == value => {}
            Some(uniform) => {
                self.values.resize(self.len, uniform);
                self.values.push(value);
                self.uniform = None;
            }
            None => self.values.push(value),
        }
        self.len += 1;
    }

    fn len(&self) -> usize {
        usize::from(self.enabled) * self.len
    }

    fn as_ref(&self) -> LevelDataRef<'_> {
        if !self.enabled {
            LevelDataRef::Absent
        } else if let Some(value) = self.uniform {
            LevelDataRef::Uniform {
                value,
                count: self.len,
            }
        } else {
            LevelDataRef::Materialized(&self.values)
        }
    }
}

/// One reusable portion of a leaf stream. Indexed traversal accumulates
/// terminal indices, while direct traversal can emit contiguous ranges.
/// Repeated paths are bounded on record boundaries, and the cursor reuses the
/// tile between calls.
#[derive(Debug)]
pub(crate) struct LeafTile {
    slots: usize,
    /// The owning leaf's maximum repetition level, used by the list patch to
    /// tell a leaf that emits one slot per element from one that does not.
    max_rep_level: i16,
    /// An earlier tile whose definition stream this tile shares. `None` means
    /// this tile materializes its own stream.
    def_owner: Option<u32>,
    /// An earlier tile whose repetition stream this tile shares. `None` means
    /// this tile materializes its own stream.
    rep_owner: Option<u32>,
    def_levels: ScalarLevels,
    rep_levels: ScalarLevels,
    direct: DirectTile,
    indexed_traversal: bool,
    value_indices: Vec<usize>,
    value_ends: Vec<usize>,
    // Last physical run at each REE depth; retained across tile clears.
    ree_runs: Vec<usize>,
    ree_depth: usize,
}

#[derive(Debug)]
struct DirectTile {
    def_levels: LevelData,
    rep_levels: LevelData,
    values: ValueSelection,
}

impl LeafTile {
    fn new(
        max_def_level: i16,
        max_rep_level: i16,
        indexed_traversal: bool,
        def_owner: Option<u32>,
        rep_owner: Option<u32>,
    ) -> Self {
        let owns_def_levels = def_owner.is_none();
        let owns_rep_levels = rep_owner.is_none();
        Self {
            slots: 0,
            max_rep_level,
            def_owner,
            rep_owner,
            def_levels: ScalarLevels::new(
                indexed_traversal && max_def_level != 0 && owns_def_levels,
            ),
            rep_levels: ScalarLevels::new(
                indexed_traversal && max_rep_level != 0 && owns_rep_levels,
            ),
            direct: DirectTile {
                def_levels: LevelData::new(
                    !indexed_traversal && max_def_level != 0 && owns_def_levels,
                ),
                rep_levels: LevelData::new(
                    !indexed_traversal && max_rep_level != 0 && owns_rep_levels,
                ),
                values: ValueSelection::Empty,
            },
            indexed_traversal,
            value_indices: Vec::new(),
            value_ends: Vec::new(),
            ree_runs: Vec::new(),
            ree_depth: 0,
        }
    }

    fn clear(&mut self) {
        self.slots = 0;
        if self.indexed_traversal {
            self.def_levels.clear();
            self.rep_levels.clear();
        } else {
            let direct = &mut self.direct;
            direct.def_levels.clear();
            direct.rep_levels.clear();
            direct.values.clear();
        }
        self.value_indices.clear();
        self.value_ends.clear();
    }

    fn push_level(&mut self, def: i16, rep: i16) {
        debug_assert!(self.indexed_traversal);
        self.slots += 1;
        self.def_levels.push(def);
        self.rep_levels.push(rep);
    }

    fn push_level_run(&mut self, def: i16, rep: i16, count: usize) {
        debug_assert!(!self.indexed_traversal);
        self.slots += count;
        let direct = &mut self.direct;
        direct.def_levels.append_run(def, count);
        direct.rep_levels.append_dense_run(rep, count);
    }

    fn push_value_range(&mut self, def: i16, rep: i16, range: std::ops::Range<usize>) {
        debug_assert!(!self.indexed_traversal);
        let len = range.len();
        self.push_level_run(def, rep, len);
        self.direct.values.append_range(range);
    }

    fn push_value(&mut self, def: i16, rep: i16, index: usize) {
        debug_assert!(self.indexed_traversal);
        self.push_level(def, rep);
        self.push_group(index, 1);
    }

    fn push_group(&mut self, index: usize, len: usize) {
        let end = self.value_ends.last().copied().unwrap_or(0) + len;
        if self.value_indices.last() == Some(&index) {
            *self.value_ends.last_mut().unwrap() = end;
        } else {
            self.value_indices.push(index);
            self.value_ends.push(end);
        }
    }

    fn batch<'a>(
        &'a self,
        terminal: &'a (dyn Array + 'static),
        def_levels: LevelDataRef<'a>,
        rep_levels: LevelDataRef<'a>,
    ) -> LeafBatch<'a> {
        let values = if !self.indexed_traversal {
            self.direct.values.as_ref()
        } else if self.value_indices.is_empty() {
            ValueSelectionRef::Empty
        } else if self.value_indices.len() == self.value_ends.last().copied().unwrap_or(0) {
            ValueSelectionRef::Sparse(&self.value_indices)
        } else {
            ValueSelectionRef::Grouped(GroupedSelectionRef::new(
                &self.value_indices,
                &self.value_ends,
            ))
        };
        LeafBatch::new(terminal, def_levels, rep_levels, values)
    }

    fn def_levels_len(&self) -> usize {
        if self.indexed_traversal {
            self.def_levels.len()
        } else {
            self.direct.def_levels.len()
        }
    }

    fn rep_levels_len(&self) -> usize {
        if self.indexed_traversal {
            self.rep_levels.len()
        } else {
            self.direct.rep_levels.len()
        }
    }
}

/// A pull cursor over a contiguous window of a tree's leaves.
///
/// One cursor serves both shapes. A window of every leaf shares the structural
/// walk across all of them; a window of one leaf walks the single branch that
/// reaches it, because a struct descends only into children whose leaves the
/// window still wants. The returned tiles are reused by the next call.
pub(crate) struct LeafCursor<'a> {
    tree: &'a LevelTree,
    window: Range<u32>,
    next_row: usize,
    target_slots: usize,
    target_rows: usize,
    tiles: Box<[LeafTile]>,
    indexed: Option<IndexedState<'a>>,
    /// A lone direct leaf's branch. `None` when the walk fans out to more than
    /// one leaf.
    branch: Option<DirectBranch<'a>>,
}

pub(crate) struct CursorBatch<'a> {
    tiles: &'a [LeafTile],
}

impl<'a> CursorBatch<'a> {
    pub(crate) fn len(&self) -> usize {
        self.tiles.len()
    }

    pub(crate) fn leaf(&self, index: usize, terminal: &'a (dyn Array + 'static)) -> LeafBatch<'a> {
        let tile = &self.tiles[index];
        let def_owner = &self.tiles[tile.def_owner.map_or(index, |owner| owner as usize)];
        let def_levels = if def_owner.indexed_traversal {
            def_owner.def_levels.as_ref()
        } else {
            def_owner.direct.def_levels.as_ref()
        };
        let rep_owner = &self.tiles[tile.rep_owner.map_or(index, |owner| owner as usize)];
        let rep_levels = if rep_owner.indexed_traversal {
            rep_owner.rep_levels.as_ref()
        } else {
            rep_owner.direct.rep_levels.as_ref()
        };
        tile.batch(terminal, def_levels, rep_levels)
    }
}

impl LeafCursor<'_> {
    /// The next record-aligned tile for every leaf in the window, in order.
    pub(crate) fn next_tiles(&mut self) -> Result<Option<CursorBatch<'_>>> {
        let len = self.tree.root.len();
        if self.next_row == len {
            return Ok(None);
        }
        for tile in &mut self.tiles {
            tile.clear();
        }

        let first_row = self.next_row;
        let rows_to_boundary = self.target_rows - first_row % self.target_rows;

        match self.indexed.as_mut() {
            None => {
                let end = first_row.saturating_add(rows_to_boundary).min(len);
                if self.branch.is_none() {
                    visit_range(
                        self.tree,
                        &self.tree.nodes[0],
                        &self.window,
                        first_row..end,
                        LevelContext::default(),
                        0,
                        &mut self.tiles,
                    )?;
                } else if let Some(branch) = self.branch.as_ref() {
                    visit_branch(
                        branch.as_slice(),
                        first_row..end,
                        LevelContext::default(),
                        0,
                        &mut self.tiles[0],
                    )?;
                } else {
                    unreachable!("a direct cursor is either grouped or follows one branch")
                }
                self.next_row = end;
            }
            Some(state) => {
                let tile = &mut self.tiles[0];
                while self.next_row < len
                    && tile.slots < self.target_slots
                    && self.next_row - first_row < rows_to_boundary
                {
                    visit_node(
                        self.next_row,
                        LevelContext::default(),
                        0,
                        &state.bound,
                        tile,
                    )?;
                    self.next_row += 1;
                }
            }
        }

        for (index, (leaf, tile)) in self.tree.leaves
            [self.window.start as usize..self.window.end as usize]
            .iter()
            .zip(&self.tiles)
            .enumerate()
        {
            debug_assert_ne!(tile.slots, 0);
            let def_owner = &self.tiles[tile.def_owner.map_or(index, |owner| owner as usize)];
            debug_assert_eq!(
                def_owner.def_levels_len(),
                usize::from(leaf.max_def_level != 0) * tile.slots
            );
            debug_assert_eq!(def_owner.slots, tile.slots);
            let rep_owner = &self.tiles[tile.rep_owner.map_or(index, |owner| owner as usize)];
            debug_assert_eq!(
                rep_owner.rep_levels_len(),
                usize::from(leaf.max_rep_level != 0) * tile.slots
            );
            debug_assert_eq!(rep_owner.slots, tile.slots);
        }
        Ok(Some(CursorBatch { tiles: &self.tiles }))
    }
}

/// Walk one leaf's branch, appending to its tile.
///
/// A lone leaf has no fan-out to amortise, and the fan-out walk makes it pay
/// for one on every run: a leaf window to intersect and a tile slice to loop
/// over. Following the branch instead is measurably cheaper, so the two shapes
/// stay separate even though they read the same tree.
fn visit_branch(
    path: &[&TreeNode],
    range: Range<usize>,
    ctx: LevelContext,
    rep: i16,
    out: &mut LeafTile,
) -> Result<()> {
    if range.is_empty() {
        return Ok(());
    }
    let (node, child_path) = path.split_first().unwrap();
    match &node.kind {
        TreeKind::Leaf | TreeKind::Null | TreeKind::DictionaryLeaf => {
            visit_direct_leaf(node, range, ctx, rep, out)
        }
        TreeKind::Struct => {
            let child_ctx = LevelContext {
                def_level: ctx.def_level + node.nullable as i16,
                ..ctx
            };
            scan_nullable_runs(node, range, |valid, range| {
                if valid {
                    visit_branch(child_path, range, child_ctx, rep, out)
                } else {
                    out.push_level_run(ctx.def_level, rep, range.len());
                    Ok(())
                }
            })
        }
        TreeKind::List(bounds) => match bounds {
            ListBounds::Offsets32(offsets) => {
                visit_branch_list_rows(node, child_path, range, ctx, rep, out, |row| {
                    (offsets[row].as_usize(), offsets[row + 1].as_usize())
                })
            }
            ListBounds::Offsets64(offsets) => {
                visit_branch_list_rows(node, child_path, range, ctx, rep, out, |row| {
                    (offsets[row].as_usize(), offsets[row + 1].as_usize())
                })
            }
            ListBounds::Fixed(width) => {
                visit_branch_list_rows(node, child_path, range, ctx, rep, out, |row| {
                    let start = row * width;
                    (start, start + width)
                })
            }
            ListBounds::View32 { offsets, sizes } => {
                visit_branch_list_rows(node, child_path, range, ctx, rep, out, |row| {
                    let start = offsets[row].as_usize();
                    (start, start + sizes[row].as_usize())
                })
            }
            ListBounds::View64 { offsets, sizes } => {
                visit_branch_list_rows(node, child_path, range, ctx, rep, out, |row| {
                    let start = offsets[row].as_usize();
                    (start, start + sizes[row].as_usize())
                })
            }
        },
        TreeKind::RunEndEncoded | TreeKind::Dictionary => {
            unreachable!("an indexed node is never reached by a range walk")
        }
    }
}

fn visit_branch_list_rows(
    node: &TreeNode,
    child_path: &[&TreeNode],
    range: Range<usize>,
    ctx: LevelContext,
    rep: i16,
    out: &mut LeafTile,
    bounds: impl Fn(usize) -> (usize, usize) + Copy,
) -> Result<()> {
    let list_def = ctx.def_level + node.nullable as i16;
    let child_ctx = LevelContext {
        def_level: list_def + 1,
        rep_level: ctx.rep_level + 1,
    };
    let flat_child = out.max_rep_level == child_ctx.rep_level;
    let mut flush = |kind: ListSlotKind, rows: Range<usize>| -> Result<()> {
        match kind {
            ListSlotKind::Null => out.push_level_run(ctx.def_level, rep, rows.len()),
            ListSlotKind::Empty => out.push_level_run(list_def, rep, rows.len()),
            ListSlotKind::NonEmpty => {
                let values_start = bounds(rows.start).0;
                let values_end = bounds(rows.end - 1).1;
                visit_branch(
                    child_path,
                    values_start..values_end,
                    child_ctx,
                    child_ctx.rep_level,
                    out,
                )?;
                patch_list_starts(
                    out,
                    values_start,
                    rows,
                    child_ctx.rep_level,
                    rep,
                    flat_child,
                    bounds,
                );
            }
        }
        Ok(())
    };
    scan_list_slots(node, range, bounds, &mut flush)
}

/// Walk `range` at `node`, appending to the tile of every leaf below it.
///
/// `out` holds every leaf in `window`; a node reaches its own tiles by
/// translating the leaf range it records relative to that window.
fn visit_range<'a>(
    tree: &'a LevelTree,
    node: &'a TreeNode,
    window: &Range<u32>,
    range: Range<usize>,
    ctx: LevelContext,
    rep: i16,
    out: &mut [LeafTile],
) -> Result<()> {
    let full_subtree = window.start <= node.leaves.start && node.leaves.end <= window.end;
    visit_range_inner(
        RangeScope {
            tree,
            window,
            full_subtree,
        },
        node,
        range,
        ctx,
        rep,
        out,
    )
}

#[derive(Clone, Copy)]
struct RangeScope<'a> {
    tree: &'a LevelTree,
    window: &'a Range<u32>,
    full_subtree: bool,
}

/// As [`visit_range`], with containment already resolved by the parent.
fn visit_range_inner<'a>(
    scope: RangeScope<'a>,
    node: &'a TreeNode,
    range: Range<usize>,
    ctx: LevelContext,
    rep: i16,
    out: &mut [LeafTile],
) -> Result<()> {
    if range.is_empty() {
        return Ok(());
    }
    match &node.kind {
        TreeKind::Leaf | TreeKind::Null | TreeKind::DictionaryLeaf => {
            let output = (node.leaves.start - scope.window.start) as usize;
            visit_direct_leaf(node, range, ctx, rep, &mut out[output])
        }
        TreeKind::Struct => visit_struct(scope, node, range, ctx, rep, out),
        TreeKind::List(bounds) => match bounds {
            ListBounds::Offsets32(offsets) => {
                visit_list_rows(scope, node, range, ctx, rep, out, |row| {
                    (offsets[row].as_usize(), offsets[row + 1].as_usize())
                })
            }
            ListBounds::Offsets64(offsets) => {
                visit_list_rows(scope, node, range, ctx, rep, out, |row| {
                    (offsets[row].as_usize(), offsets[row + 1].as_usize())
                })
            }
            ListBounds::Fixed(width) => visit_list_rows(scope, node, range, ctx, rep, out, |row| {
                let start = row * width;
                (start, start + width)
            }),
            ListBounds::View32 { offsets, sizes } => {
                visit_list_rows(scope, node, range, ctx, rep, out, |row| {
                    let start = offsets[row].as_usize();
                    (start, start + sizes[row].as_usize())
                })
            }
            ListBounds::View64 { offsets, sizes } => {
                visit_list_rows(scope, node, range, ctx, rep, out, |row| {
                    let start = offsets[row].as_usize();
                    (start, start + sizes[row].as_usize())
                })
            }
        },
        TreeKind::RunEndEncoded | TreeKind::Dictionary => {
            unreachable!("an indexed node is never reached by a range walk")
        }
    }
}

/// Recurse into each child whose leaves the window still wants.
///
/// A full subtree walks every child without range checks. Above a selected
/// subtree, only children intersecting `window` are visited.
fn visit_struct<'a>(
    scope: RangeScope<'a>,
    node: &'a TreeNode,
    range: Range<usize>,
    ctx: LevelContext,
    rep: i16,
    out: &mut [LeafTile],
) -> Result<()> {
    let child_ctx = LevelContext {
        def_level: ctx.def_level + node.nullable as i16,
        ..ctx
    };
    // Children are one contiguous block, so the run loop walks them as a slice
    // rather than indexing the arena once per child.
    let children = &scope.tree.nodes[node.children.start as usize..node.children.end as usize];
    let output_leaves = if scope.full_subtree {
        node.leaves.start - scope.window.start..node.leaves.end - scope.window.start
    } else {
        node.leaves.start.max(scope.window.start) - scope.window.start
            ..node.leaves.end.min(scope.window.end) - scope.window.start
    };
    scan_nullable_runs(node, range, |valid, range| {
        if !valid {
            for out in &mut out[output_leaves.start as usize..output_leaves.end as usize] {
                out.push_level_run(ctx.def_level, rep, range.len());
            }
            return Ok(());
        }
        for child in children {
            if !scope.full_subtree
                && (child.leaves.start >= scope.window.end
                    || scope.window.start >= child.leaves.end)
            {
                continue;
            }
            let child_is_full = scope.full_subtree
                || (scope.window.start <= child.leaves.start
                    && child.leaves.end <= scope.window.end);
            visit_range_inner(
                RangeScope {
                    full_subtree: child_is_full,
                    ..scope
                },
                child,
                range.clone(),
                child_ctx,
                rep,
                out,
            )?;
        }
        Ok(())
    })
}

fn visit_list_rows<'a>(
    scope: RangeScope<'a>,
    node: &'a TreeNode,
    range: Range<usize>,
    ctx: LevelContext,
    rep: i16,
    out: &mut [LeafTile],
    bounds: impl Fn(usize) -> (usize, usize) + Copy,
) -> Result<()> {
    // Resolved once, not once per non-empty run of list rows.
    let child = &scope.tree.nodes[node.children.start as usize];
    let leaves = if scope.full_subtree {
        (node.leaves.start - scope.window.start) as usize
            ..(node.leaves.end - scope.window.start) as usize
    } else {
        (node.leaves.start.max(scope.window.start) - scope.window.start) as usize
            ..(node.leaves.end.min(scope.window.end) - scope.window.start) as usize
    };
    let list_def = ctx.def_level + node.nullable as i16;
    let child_ctx = LevelContext {
        def_level: list_def + 1,
        rep_level: ctx.rep_level + 1,
    };
    let mut flush = |kind: ListSlotKind, rows: Range<usize>| -> Result<()> {
        match kind {
            ListSlotKind::Null | ListSlotKind::Empty => {
                let def = if kind == ListSlotKind::Null {
                    ctx.def_level
                } else {
                    list_def
                };
                for out in &mut out[leaves.clone()] {
                    out.push_level_run(def, rep, rows.len());
                }
            }
            ListSlotKind::NonEmpty => {
                let values_start = bounds(rows.start).0;
                let values_end = bounds(rows.end - 1).1;
                visit_range_inner(
                    scope,
                    child,
                    values_start..values_end,
                    child_ctx,
                    child_ctx.rep_level,
                    out,
                )?;
                for out in &mut out[leaves.clone()] {
                    // A leaf whose deepest repetition is this list's child
                    // emits exactly one slot per element, so its list starts
                    // are at known offsets rather than found by scanning.
                    let flat_child = out.max_rep_level == child_ctx.rep_level;
                    patch_list_starts(
                        out,
                        values_start,
                        rows.clone(),
                        child_ctx.rep_level,
                        rep,
                        flat_child,
                        bounds,
                    );
                }
            }
        }
        Ok(())
    };
    scan_list_slots(node, range, bounds, &mut flush)
}

fn visit_direct_leaf(
    node: &TreeNode,
    range: Range<usize>,
    ctx: LevelContext,
    rep: i16,
    out: &mut LeafTile,
) -> Result<()> {
    let len = range.len();
    let def = ctx.def_level + node.nullable as i16;
    let Some(nulls) = node.nulls.as_ref() else {
        out.push_value_range(def, rep, range);
        return Ok(());
    };

    if !node.nullable {
        if let Some(index) = range.clone().find(|&index| nulls.is_null(index)) {
            return Err(super::required_null(&node.name, index));
        }
        out.push_value_range(def, rep, range);
        return Ok(());
    }

    if nulls.null_count() == nulls.len() {
        out.push_level_run(ctx.def_level, rep, len);
        return Ok(());
    }

    // Avoid constructing a sliced NullBuffer for the tiny ranges produced by
    // nested lists. NullBuffer::slice recomputes a popcount; reading the source
    // bitmap directly matches the old builder's proven short-range path.
    if len < super::BULK_FILL_MIN_LEN {
        out.slots += len;
        let direct = &mut out.direct;
        direct.rep_levels.append_dense_run(rep, len);
        let bits = nulls.inner();
        direct
            .def_levels
            .extend_from_iter(range.clone().map(|index| {
                // SAFETY: `range` is a valid range of this bound leaf.
                let valid = unsafe { bits.value_unchecked(index) };
                def - (!valid as i16)
            }));
        direct.values.append_sparse_range(nulls, range);
        return Ok(());
    }

    let range_nulls = nulls.slice(range.start, len);
    let null_count = range_nulls.null_count();
    if null_count == 0 {
        out.push_value_range(def, rep, range);
        return Ok(());
    }
    if null_count == len {
        out.push_level_run(ctx.def_level, rep, len);
        return Ok(());
    }

    out.slots += len;
    let direct = &mut out.direct;
    direct.rep_levels.append_dense_run(rep, len);
    // Scattered nulls produce short runs whose metadata costs more than
    // filling the level buffer from the bitmap in one pass.
    if levels_have_compact_runs(&range_nulls) {
        let mut position = 0;
        for (start, end) in range_nulls.valid_slices() {
            direct
                .def_levels
                .append_run(ctx.def_level, start - position);
            direct.def_levels.append_run(def, end - start);
            direct
                .values
                .append_range(range.start + start..range.start + end);
            position = end;
        }
        direct.def_levels.append_run(ctx.def_level, len - position);
    } else {
        let bits = nulls.inner();
        direct
            .def_levels
            .extend_from_iter(range.clone().map(|index| {
                // SAFETY: `range` is a valid slice of `array` and therefore of
                // its logical null buffer.
                let valid = unsafe { bits.value_unchecked(index) };
                def - (!valid as i16)
            }));
        direct
            .values
            .append_sparse(range_nulls, range.start, len - null_count);
    }
    Ok(())
}

/// Whether validity runs are long enough to justify their per-run metadata.
///
/// Sampled over a bounded prefix, and deliberately so: what decides this is the
/// run *structure*, which null density does not capture — a half-null column is
/// perfectly run-compact when its nulls are clustered and worthless when they
/// alternate. The scan stops as soon as the average run is provably too short.
fn levels_have_compact_runs(nulls: &NullBuffer) -> bool {
    let len = nulls.len().min(super::plan::LEVEL_RUN_PROBE_SIZE);
    if len < super::plan::MIN_AVERAGE_LEVEL_RUN_LENGTH {
        return false;
    }
    let mut runs = 1;
    let mut previous = nulls.is_valid(0);
    for index in 1..len {
        let valid = nulls.is_valid(index);
        runs += usize::from(valid != previous);
        if runs * super::plan::MIN_AVERAGE_LEVEL_RUN_LENGTH > len {
            return false;
        }
        previous = valid;
    }
    true
}

#[derive(Clone, Copy, PartialEq)]
enum ListSlotKind {
    Null,
    Empty,
    NonEmpty,
}

fn use_sparse_null_runs(nulls: &NullBuffer, len: usize) -> bool {
    len >= super::BULK_FILL_MIN_LEN && levels_have_compact_runs(nulls)
}

fn scan_nullable_runs(
    node: &TreeNode,
    range: Range<usize>,
    mut visit: impl FnMut(bool, Range<usize>) -> Result<()>,
) -> Result<()> {
    let Some(nulls) = node.nulls.as_ref() else {
        return visit(true, range);
    };
    if !node.nullable {
        if let Some(index) = range.clone().find(|&index| nulls.is_null(index)) {
            return Err(super::required_null(&node.name, index));
        }
        return visit(true, range);
    }
    if nulls.null_count() == nulls.len() {
        return visit(false, range);
    }
    if use_sparse_null_runs(nulls, range.len()) {
        let range_nulls = nulls.slice(range.start, range.len());
        let mut position = 0;
        for (start, end) in range_nulls.valid_slices() {
            if position != start {
                visit(false, range.start + position..range.start + start)?;
            }
            visit(true, range.start + start..range.start + end)?;
            position = end;
        }
        if position != range.len() {
            visit(false, range.start + position..range.end)?;
        }
        return Ok(());
    }

    let mut run_start = range.start;
    let mut run_valid = nulls.is_valid(run_start);
    for index in run_start + 1..range.end {
        let valid = nulls.is_valid(index);
        if valid != run_valid {
            visit(run_valid, run_start..index)?;
            run_start = index;
            run_valid = valid;
        }
    }
    visit(run_valid, run_start..range.end)
}

fn scan_list_slots(
    node: &TreeNode,
    range: Range<usize>,
    bounds: impl Fn(usize) -> (usize, usize) + Copy,
    mut flush: impl FnMut(ListSlotKind, Range<usize>) -> Result<()>,
) -> Result<()> {
    if node
        .nulls
        .as_ref()
        .is_some_and(|nulls| nulls.null_count() == nulls.len())
    {
        if !node.nullable {
            return Err(super::required_null(&node.name, range.start));
        }
        return flush(ListSlotKind::Null, range);
    }

    if let Some(nulls) = node.nulls.as_ref()
        && node.nullable
        && use_sparse_null_runs(nulls, range.len())
    {
        let range_nulls = nulls.slice(range.start, range.len());
        let mut position = 0;
        for (start, end) in range_nulls.valid_slices() {
            if position != start {
                flush(
                    ListSlotKind::Null,
                    range.start + position..range.start + start,
                )?;
            }
            scan_list_rows(
                range.start + start..range.start + end,
                bounds,
                |_, start, end| {
                    Ok(if start == end {
                        ListSlotKind::Empty
                    } else {
                        ListSlotKind::NonEmpty
                    })
                },
                &mut flush,
            )?;
            position = end;
        }
        if position != range.len() {
            flush(ListSlotKind::Null, range.start + position..range.end)?;
        }
        return Ok(());
    }

    match node.nulls.as_ref() {
        Some(nulls) => scan_list_rows(
            range,
            bounds,
            |row, start, end| {
                if nulls.is_null(row) {
                    if node.nullable {
                        Ok(ListSlotKind::Null)
                    } else {
                        Err(super::required_null(&node.name, row))
                    }
                } else if start == end {
                    Ok(ListSlotKind::Empty)
                } else {
                    Ok(ListSlotKind::NonEmpty)
                }
            },
            flush,
        ),
        None => scan_list_rows(
            range,
            bounds,
            |_, start, end| {
                Ok(if start == end {
                    ListSlotKind::Empty
                } else {
                    ListSlotKind::NonEmpty
                })
            },
            flush,
        ),
    }
}

/// Always inlined: `bounds` and `classify` are closures over the bound list
/// layout, and leaving this loop out of line turns each of them into a call
/// per row.
#[inline(always)]
fn scan_list_rows(
    range: Range<usize>,
    bounds: impl Fn(usize) -> (usize, usize),
    classify: impl Fn(usize, usize, usize) -> Result<ListSlotKind>,
    mut flush: impl FnMut(ListSlotKind, Range<usize>) -> Result<()>,
) -> Result<()> {
    let mut run_start = range.start;
    let (first_start, mut previous_end) = bounds(run_start);
    let mut run_kind = classify(run_start, first_start, previous_end)?;
    for row in run_start + 1..range.end {
        let (start, end) = bounds(row);
        let kind = classify(row, start, end)?;
        let contiguous = kind != ListSlotKind::NonEmpty || previous_end == start;
        if kind != run_kind || !contiguous {
            flush(run_kind, run_start..row)?;
            run_start = row;
            run_kind = kind;
        }
        previous_end = end;
    }
    flush(run_kind, run_start..range.end)
}

fn patch_list_starts(
    out: &mut LeafTile,
    values_start: usize,
    rows: Range<usize>,
    child_rep: i16,
    rep: i16,
    flat_child: bool,
    bounds: impl Fn(usize) -> (usize, usize),
) {
    // A group of sibling leaves with the same deepest list has an identical
    // repetition stream. Only the group's owner materializes and patches it.
    if out.rep_owner.is_some() || rep == child_rep {
        return;
    }
    let levels = out.direct.rep_levels.materialize_mut().unwrap();
    if flat_child {
        let values_end = bounds(rows.end - 1).1;
        let slot_start = levels.len() - (values_end - values_start);
        for row in rows {
            levels[slot_start + bounds(row).0 - values_start] = rep;
        }
        return;
    }
    // One reverse pass over the appended slots. Counting child-element starts
    // back from the end locates every list start without an inner loop or a
    // bounds check per slot.
    let values_end = bounds(rows.end - 1).1;
    let mut starts = rows.rev();
    let mut next_stamp_at = values_end - bounds(starts.next().unwrap()).0;
    let mut seen = 0usize;
    for level in levels.iter_mut().rev() {
        // The child is written before its parent, so nothing already in the
        // buffer for this run sits below the child rep level.
        if *level <= child_rep {
            seen += 1;
            if seen == next_stamp_at {
                *level = rep;
                match starts.next() {
                    Some(row) => next_stamp_at = values_end - bounds(row).0,
                    None => break,
                }
            }
        }
    }
}

/// One path node with its Arrow buffers resolved for the lifetime of one cursor.
///
/// The indexed walker visits a single row per call, so every downcast it would
/// otherwise repeat for each row is resolved once here. This is the same trick
/// [`TreeNode`] plays for the range walker; indexed branches bind the same buffers once.
#[derive(Debug)]
struct BoundNode<'a> {
    kind: BoundKind<'a>,
    /// Physical validity of the array visited at this node, when it has nulls.
    nulls: Option<&'a NullBuffer>,
    /// The normalized field contract in scope at this node.
    nullable: bool,
    name: &'a str,
}

#[derive(Debug)]
enum BoundKind<'a> {
    /// `DataType::Null`: every row is logically null.
    Null,
    /// A primitive leaf. The row index is the physical value position.
    Leaf,
    /// A dictionary whose values are a leaf. The *key* index is emitted; the
    /// value pipeline composes the key mapping, so the values are consulted
    /// only for their validity.
    DictionaryLeaf {
        keys: DictionaryKeys<'a>,
        value_nulls: Option<&'a NullBuffer>,
        null_values: bool,
    },
    /// A dictionary over a non-leaf: resolve the key, then descend.
    Dictionary { keys: DictionaryKeys<'a> },
    /// Run-end encoded: map the row to its physical run, then descend.
    RunEnds { ends: RunEnds<'a>, base: usize },
    /// A struct. The selected child is already the next bound node.
    Struct,
}

/// Bind one leaf's branch to the Arrow buffers the tree already owns.
///
/// The indexed walker visits a single row per call, so it wants a linear view
/// of the branch rather than the tree: every downcast it would otherwise repeat
/// for each row is resolved once, here.
fn bind_indexed_branch<'a>(tree: &'a LevelTree, leaf: &TreeLeaf) -> Result<Box<[BoundNode<'a>]>> {
    let mut bound = Vec::with_capacity(leaf.branch.len());
    for index in leaf.branch.iter().copied() {
        let node = &tree.nodes[index as usize];
        let array = node.array.as_ref();
        let dictionary = matches!(node.kind, TreeKind::Dictionary | TreeKind::DictionaryLeaf)
            .then(|| array.as_any_dictionary());
        let nulls = match dictionary {
            Some(dictionary) => dictionary.keys().nulls(),
            None => array.nulls(),
        };
        let kind = match &node.kind {
            TreeKind::Null => BoundKind::Null,
            TreeKind::Leaf => BoundKind::Leaf,
            TreeKind::DictionaryLeaf => {
                let dictionary = dictionary.unwrap();
                let values = dictionary.values();
                BoundKind::DictionaryLeaf {
                    keys: super::super::dictionary_keys(dictionary.keys()),
                    value_nulls: values.nulls(),
                    null_values: matches!(values.data_type(), DataType::Null),
                }
            }
            TreeKind::Dictionary => BoundKind::Dictionary {
                keys: super::super::dictionary_keys(dictionary.unwrap().keys()),
            },
            TreeKind::RunEndEncoded => {
                let (ends, base, _) = super::super::run_ends_of(array)?;
                BoundKind::RunEnds { ends, base }
            }
            TreeKind::Struct => BoundKind::Struct,
            TreeKind::List(_) => unreachable!("indexed list binding remains eager"),
        };
        bound.push(BoundNode {
            kind,
            nulls,
            nullable: node.nullable,
            name: &node.name,
        });
    }
    Ok(bound.into_boxed_slice())
}

#[inline(always)]
fn bound_is_null(nulls: Option<&NullBuffer>, index: usize) -> bool {
    nulls.is_some_and(|nulls| nulls.is_null(index))
}

fn visit_node(
    index: usize,
    ctx: LevelContext,
    rep: i16,
    path: &[BoundNode<'_>],
    out: &mut LeafTile,
) -> Result<()> {
    let (node, child_path) = path.split_first().unwrap();
    match &node.kind {
        BoundKind::RunEnds { ends, base } => {
            let position = base + index;
            let depth = out.ree_depth;
            let physical = match out.ree_runs.get(depth).copied() {
                Some(mut run) if run == 0 || position >= ends.end_of(run.saturating_sub(1)) => {
                    while ends.end_of(run) <= position {
                        run += 1;
                    }
                    run
                }
                _ => ends.run_of(position),
            };
            if let Some(run) = out.ree_runs.get_mut(depth) {
                *run = physical;
            } else {
                debug_assert_eq!(out.ree_runs.len(), depth);
                out.ree_runs.push(physical);
            }
            out.ree_depth += 1;
            let result = visit_node(physical, ctx, rep, child_path, out);
            out.ree_depth -= 1;
            result
        }
        BoundKind::DictionaryLeaf {
            keys,
            value_nulls,
            null_values,
        } => {
            if bound_is_null(node.nulls, index) {
                return emit_null(node, ctx, rep, index, out);
            }
            let key = keys.key_at(index);
            if *null_values || bound_is_null(*value_nulls, key) {
                return emit_null(node, ctx, rep, index, out);
            }
            out.push_value(ctx.def_level + node.nullable as i16, rep, index);
            Ok(())
        }
        BoundKind::Dictionary { keys } => {
            if bound_is_null(node.nulls, index) {
                return emit_null(node, ctx, rep, index, out);
            }
            visit_node(keys.key_at(index), ctx, rep, child_path, out)
        }
        BoundKind::Null => emit_null(node, ctx, rep, index, out),
        BoundKind::Leaf => {
            if bound_is_null(node.nulls, index) {
                emit_null(node, ctx, rep, index, out)
            } else {
                out.push_value(ctx.def_level + node.nullable as i16, rep, index);
                Ok(())
            }
        }
        BoundKind::Struct => {
            if bound_is_null(node.nulls, index) {
                return emit_null(node, ctx, rep, index, out);
            }
            visit_node(
                index,
                LevelContext {
                    def_level: ctx.def_level + node.nullable as i16,
                    ..ctx
                },
                rep,
                child_path,
                out,
            )
        }
    }
}

fn emit_null(
    node: &BoundNode<'_>,
    ctx: LevelContext,
    rep: i16,
    index: usize,
    out: &mut LeafTile,
) -> Result<()> {
    if !node.nullable {
        return Err(super::required_null(node.name, index));
    }
    out.push_level(ctx.def_level, rep);
    Ok(())
}

fn incompatible(contract: FieldContract<'_>, actual: &DataType) -> ParquetError {
    ParquetError::ArrowError(format!(
        "Incompatible type. Field '{}' has type {}, array has type {}",
        contract.name, contract.data_type, actual
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Int32Type;
    use arrow_array::{Int32Array, Int32RunArray, MapArray, RunArray, StructArray, make_array};
    use arrow_buffer::Buffer;
    use arrow_data::ArrayDataBuilder;
    use std::sync::Arc;

    fn tree_of(field: &Field, array: ArrayRef) -> LevelTree {
        LevelTree::build(field, &array).unwrap()
    }

    #[test]
    fn map_key_planning_rejects_optional_contracts() {
        let map_type = |key| {
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(vec![key, Field::new("value", DataType::Int32, true)].into()),
                    false,
                )),
                false,
            )
        };
        let map_array = |keys: ArrayRef| -> ArrayRef {
            let entries = StructArray::new(
                vec![
                    Field::new("key", keys.data_type().clone(), false),
                    Field::new("value", DataType::Int32, true),
                ]
                .into(),
                vec![keys, Arc::new(Int32Array::from(vec![42]))],
                None,
            );
            Arc::new(MapArray::new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::new(vec![0i32, 1].into()),
                entries,
                None,
                false,
            ))
        };
        let ree = Arc::new(
            Int32RunArray::try_new(&Int32Array::from(vec![1]), &Int32Array::from(vec![7])).unwrap(),
        );
        let ree_type = ree.data_type().clone();
        let dense = map_array(Arc::new(Int32Array::from(vec![7])));
        let required = Field::new(
            "map",
            map_type(Field::new("key", DataType::Int32, false)),
            true,
        );
        assert_eq!(LevelTree::build(&required, &dense).unwrap().leaf_count(), 2);
        for array in [dense, map_array(ree)] {
            // Only the target contract is validated; REE remains a compatible
            // physical layout under a required logical key.
            assert_eq!(
                crate::arrow::arrow_writer::compute_leaves(&required, &array)
                    .unwrap()
                    .len(),
                2
            );
            for key in [
                Field::new("key", DataType::Int32, true),
                Field::new("key", ree_type.clone(), false),
                Field::new(
                    "key",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(ree_type.clone())),
                    false,
                ),
            ] {
                let field = Field::new("map", map_type(key), true);
                let err = LevelTree::build(&field, &array).unwrap_err();
                assert!(
                    err.to_string()
                        .contains("Map key field 'key' must be non-nullable"),
                    "{err}"
                );
                let err = crate::arrow::arrow_writer::compute_leaves(&field, &array).unwrap_err();
                assert!(
                    err.to_string()
                        .contains("Map key field 'key' must be non-nullable"),
                    "{err}"
                );
            }
        }
    }

    #[test]
    fn direct_leaf_empty_required_and_bulk_null_paths() {
        let field = Field::new("a", DataType::Int32, false);
        let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None]));
        let tree = tree_of(&field, array);
        let mut tile = LeafTile::new(0, 0, false, None, None);
        visit_range(
            &tree,
            &tree.nodes[0],
            &(0..1),
            0..0,
            LevelContext::default(),
            0,
            std::slice::from_mut(&mut tile),
        )
        .unwrap();
        assert_eq!(tile.slots, 0);
        assert!(
            visit_range(
                &tree,
                &tree.nodes[0],
                &(0..1),
                0..2,
                LevelContext::default(),
                0,
                std::slice::from_mut(&mut tile),
            )
            .is_err()
        );

        let field = Field::new("a", DataType::Int32, true);
        let array: ArrayRef = Arc::new(Int32Array::from(
            (0..80)
                .map(|index| (index % 4 == 0).then_some(index))
                .collect::<Vec<_>>(),
        ));
        let len = array.len();
        let tree = tree_of(&field, array);
        let mut tile = LeafTile::new(1, 0, false, None, None);
        visit_range(
            &tree,
            &tree.nodes[0],
            &(0..1),
            0..len,
            LevelContext::default(),
            0,
            std::slice::from_mut(&mut tile),
        )
        .unwrap();
        assert_eq!(tile.slots, 80);
        assert_eq!(tile.direct.values.as_ref().len(), 20);
    }

    #[test]
    fn tree_shares_ancestors_and_numbers_leaves_in_order() {
        let inner = Field::new("c", DataType::Int32, true);
        let fields = vec![
            Field::new("a", DataType::Int32, true),
            Field::new(
                "b",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                true,
            ),
        ];
        let struct_field = Field::new(
            "s",
            DataType::Struct(fields.clone().into_iter().chain([inner]).collect()),
            true,
        );
        let array: ArrayRef = Arc::new(arrow_array::StructArray::new_null(
            match struct_field.data_type() {
                DataType::Struct(fields) => fields.clone(),
                _ => unreachable!(),
            },
            3,
        ));
        let tree = tree_of(&struct_field, array);
        assert_eq!(tree.leaf_count(), 3);
        assert_eq!(
            tree.leaves
                .iter()
                .map(|leaf| (leaf.max_def_level, leaf.max_rep_level))
                .collect::<Vec<_>>(),
            [(2, 0), (4, 1), (2, 0)]
        );
        // The struct is one node shared by every leaf, and its children are a
        // contiguous block whose leaf ranges tile the leaf array in order.
        let root = &tree.nodes[0];
        assert_eq!(root.leaves, 0..3);
        let mut next = 0;
        for child in root.children.clone() {
            assert_eq!(tree.nodes[child as usize].leaves.start, next);
            next = tree.nodes[child as usize].leaves.end;
        }
        assert_eq!(next, 3);
        // The all-null struct is a constant run, and the list covers one leaf,
        // so there is no shared scan worth one combined walk.
        assert!(tree.write_windows().is_none());
    }

    #[test]
    fn grouped_subtree_uses_window_relative_leaf_indices() {
        let a_field = Arc::new(Field::new("a", DataType::Int32, false));
        let a = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let b_field = Arc::new(Field::new("b", DataType::Int32, false));
        let b = Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef;
        let c_field = Arc::new(Field::new("c", DataType::Int32, false));
        let c = Arc::new(Int32Array::from(vec![7, 8, 9])) as ArrayRef;
        let nested = Arc::new(StructArray::from((
            vec![(b_field, b), (c_field, c)],
            Buffer::from([0b00000101]),
        ))) as ArrayRef;
        let nested_field = Arc::new(Field::new("nested", nested.data_type().clone(), true));
        let outer = Arc::new(StructArray::from(vec![
            (a_field, a),
            (nested_field, nested),
        ])) as ArrayRef;
        let field = Field::new("root", outer.data_type().clone(), false);
        let tree = tree_of(&field, outer);

        assert_eq!(tree.write_windows().unwrap(), [0..1, 1..3]);
        let mut cursor = tree.cursor(1..3, 1024, 1024).unwrap();
        let tiles = cursor.next_tiles().unwrap().unwrap();
        assert_eq!(tiles.len(), 2);
        for leaf in 0..2 {
            let levels = tiles
                .leaf(leaf, tree.terminal(leaf as u32 + 1))
                .def_level_data();
            assert_eq!(
                (0..levels.len())
                    .map(|index| levels.value_at(index).unwrap())
                    .collect::<Vec<_>>(),
                [1, 0, 1]
            );
        }
        assert!(cursor.next_tiles().unwrap().is_none());
    }

    #[test]
    fn leaves_below_the_same_list_share_repetition_levels() {
        let a_field = Arc::new(Field::new("a", DataType::Int32, false));
        let a = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let b_field = Arc::new(Field::new("b", DataType::Int32, false));
        let b = Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef;
        let c_field = Arc::new(Field::new("c", DataType::Int32, false));
        let c = Arc::new(Int32Array::from(vec![7, 8, 9])) as ArrayRef;
        let values = StructArray::from(vec![(a_field, a), (b_field, b), (c_field, c)]);
        let element = Arc::new(Field::new("element", values.data_type().clone(), false));
        let data_type = DataType::List(element);
        let data = ArrayDataBuilder::new(data_type.clone())
            .len(3)
            .add_buffer(Buffer::from_iter([0_i32, 2, 2, 3]))
            .add_child_data(values.into_data())
            .build()
            .unwrap();
        let array = make_array(data);
        let field = Field::new("list", data_type, false);
        let tree = tree_of(&field, array);

        let windows = tree.write_windows().unwrap();
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0], 0..3);
        for window in [0..3, 0..1, 1..2, 2..3] {
            for target_rows in [1, 2, 1024] {
                let mut cursor = tree.cursor(window.clone(), 1024, target_rows).unwrap();
                let mut observed = vec![(Vec::new(), Vec::new(), Vec::new()); window.len()];
                let mut batches = 0;
                while let Some(tiles) = cursor.next_tiles().unwrap() {
                    batches += 1;
                    assert_eq!(tiles.len(), window.len());
                    for (leaf, (definitions, repetitions, indices)) in
                        observed.iter_mut().enumerate()
                    {
                        let owner = (leaf != 0).then_some(0);
                        assert_eq!(tiles.tiles[leaf].def_owner, owner);
                        assert_eq!(tiles.tiles[leaf].rep_owner, owner);
                        if owner.is_some() {
                            assert_eq!(
                                tiles.tiles[leaf].direct.def_levels.as_ref(),
                                LevelDataRef::Absent
                            );
                            assert_eq!(
                                tiles.tiles[leaf].direct.rep_levels.as_ref(),
                                LevelDataRef::Absent
                            );
                        }
                        let batch = tiles.leaf(leaf, tree.terminal(window.start + leaf as u32));
                        definitions.extend(batch.def_level_data().cursor());
                        repetitions.extend(batch.rep_level_data().cursor());
                        let selection = batch.value_selection();
                        indices.extend((0..selection.len()).map(|index| selection.index_at(index)));
                    }
                }
                assert_eq!(batches, 3_usize.div_ceil(target_rows));
                for (definitions, repetitions, indices) in observed {
                    assert_eq!(definitions, [1, 1, 0, 1]);
                    assert_eq!(repetitions, [0, 1, 0, 0]);
                    assert_eq!(indices, [0, 1, 2]);
                }
            }
        }
    }

    #[test]
    fn shared_prefix_is_preserved_across_an_indexed_sibling() {
        let a_field = Arc::new(Field::new("a", DataType::Int32, false));
        let a = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let b_field = Arc::new(Field::new("b", DataType::Int32, false));
        let b = Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef;
        let direct = Arc::new(StructArray::from(vec![(a_field, a), (b_field, b)])) as ArrayRef;
        let direct_field = Arc::new(Field::new("direct", direct.data_type().clone(), false));

        let c_field = Arc::new(Field::new("c", DataType::Int32, false));
        let c = Arc::new(Int32Array::from(vec![7])) as ArrayRef;
        let d_field = Arc::new(Field::new("d", DataType::Int32, false));
        let d = Arc::new(Int32Array::from(vec![8])) as ArrayRef;
        let run_values = StructArray::from(vec![(c_field, c), (d_field, d)]);
        let run_ends = Int32Array::from(vec![3]);
        let indexed =
            Arc::new(RunArray::<Int32Type>::try_new(&run_ends, &run_values).unwrap()) as ArrayRef;
        let indexed_field = Arc::new(Field::new("indexed", indexed.data_type().clone(), false));

        let outer = Arc::new(StructArray::from((
            vec![(direct_field, direct), (indexed_field, indexed)],
            Buffer::from([0b00000101]),
        ))) as ArrayRef;
        let field = Field::new("root", outer.data_type().clone(), true);
        let tree = tree_of(&field, outer);

        assert_eq!(tree.write_windows().unwrap(), [0..2, 2..3, 3..4]);
    }
}
