use parquet::arrow::arrow_reader::RowSelectionPolicy;

use super::fixture::CaseFixture;
use super::model::{CaseSpec, PAYLOAD_VALUE_MODULUS, ROWS_PER_GROUP};
use super::runner::run_collect_payload0;
use super::shapes::expand_pattern;

pub(crate) async fn preflight_auto(case: &CaseSpec, fixture: &CaseFixture) {
    let actual = run_collect_payload0(fixture, RowSelectionPolicy::default()).await;
    assert_eq!(
        actual.row_count, fixture.expected_rows,
        "{} returned an unexpected number of rows",
        case.name
    );

    let expected = expected_selected_global_rows(case);
    assert_eq!(actual.payload0.len(), expected.len(), "{}", case.name);
    if let Some((output_row, (actual, expected))) = actual
        .payload0
        .iter()
        .zip(&expected)
        .enumerate()
        .find(|(_, (actual, expected))| actual != expected)
    {
        panic!(
            "{} returned the wrong source row at output {output_row}: expected {expected}, got {actual}",
            case.name
        );
    }
}

fn expected_selected_global_rows(case: &CaseSpec) -> Vec<i32> {
    case.row_groups
        .iter()
        .copied()
        .enumerate()
        .flat_map(|(row_group_idx, pattern)| {
            expand_pattern(pattern, ROWS_PER_GROUP)
                .into_iter()
                .enumerate()
                .filter(|(_, selected)| *selected == 1)
                .map(move |(row_idx, _)| {
                    let global_row = row_group_idx * ROWS_PER_GROUP + row_idx;
                    global_row.wrapping_rem(PAYLOAD_VALUE_MODULUS) as i32
                })
        })
        .collect()
}
