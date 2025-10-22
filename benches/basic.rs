use criterion::{criterion_group, criterion_main, Criterion, black_box};

// A tiny synthetic workload: build keys and simulate aggregation logic similar to parts of the main pipeline.
fn aggregate_sim(n: usize) -> usize {
    use std::collections::BTreeMap;
    let mut map: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    for i in 0..n {
        // emulate a rolling key distribution similar to modulo bucketing of log/csv fields
        let k = format!("k{}:{}", i % 10, (i / 10) % 100);
        let e = map.entry(k).or_insert((0, 0));
        e.0 += 1;            // count
        e.1 += (i % 100) as u64; // sum
    }
    map.len()
}

fn bench_aggregate(c: &mut Criterion) {
    c.bench_function("aggregate_sim_1e5", |b| b.iter(|| black_box(aggregate_sim(100_000))));
}

criterion_group!(benches, bench_aggregate);
criterion_main!(benches);
