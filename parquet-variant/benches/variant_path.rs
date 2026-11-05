use criterion::*;

use parquet_variant::VariantPath;

fn bench_parse_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("variant_path_parse");
    for (name, path) in [
        // Dot notation never enters the bracket parser
        ("dot_fields", "foo.bar.baz"),
        ("bracket_quoted_fields", "['foo']['bar']['baz']"),
        ("bracket_indexes", "[0][1][2]"),
        ("bracket_wildcards", "[*][*][*]"),
        // Escapes require building an unescaped copy of the field name
        ("bracket_escaped_field", r"['escaped\]field']"),
        ("mixed", "a['b.c'].d[2]['3']"),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| VariantPath::try_from(std::hint::black_box(path)).unwrap());
        });
    }
    group.finish();
}

criterion_group!(benches, bench_parse_path);

criterion_main!(benches);
