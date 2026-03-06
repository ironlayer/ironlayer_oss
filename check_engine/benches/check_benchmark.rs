//! Criterion benchmarks for the IronLayer Check Engine.
//!
//! Benchmarks measure cold and warm check times for synthetic projects
//! of varying sizes (100, 500, 1000 models). Synthetic models include
//! headers, SQL bodies, and ref() macros to exercise all Phase 2 checkers.

use criterion::{criterion_group, criterion_main, Criterion};
use ironlayer_check_engine::config::CheckConfig;
use ironlayer_check_engine::engine::CheckEngine;
use std::fs;
use std::path::Path;
use tempfile::tempdir;

/// Create a synthetic IronLayer project with the given number of models.
fn create_synthetic_project(root: &Path, model_count: usize) {
    // Create ironlayer.yaml marker
    fs::write(root.join("ironlayer.yaml"), "version: 1\n").unwrap();

    let models_dir = root.join("models");
    fs::create_dir_all(&models_dir).unwrap();

    let kinds = [
        "FULL_REFRESH",
        "INCREMENTAL_BY_TIME_RANGE",
        "APPEND_ONLY",
        "MERGE_BY_KEY",
    ];

    for i in 0..model_count {
        let kind = kinds[i % kinds.len()];
        let extra = match kind {
            "INCREMENTAL_BY_TIME_RANGE" => "\n-- time_column: created_at",
            "MERGE_BY_KEY" => "\n-- unique_key: id",
            _ => "",
        };
        let content = format!(
            "-- name: model_{i}\n-- kind: {kind}{extra}\n-- materialization: TABLE\n\
             SELECT\n    id,\n    name,\n    created_at,\n    updated_at\n\
             FROM {{{{ ref('model_{}') }}}}\n\
             WHERE created_at > '2024-01-01'\n",
            if i > 0 { i - 1 } else { 0 }
        );
        fs::write(models_dir.join(format!("model_{i}.sql")), content).unwrap();
    }
}

fn bench_cold_100(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    create_synthetic_project(dir.path(), 100);

    // Delete cache file before each iteration for cold runs
    let cache_file = dir.path().join(".ironlayer").join("check_cache.json");
    c.bench_function("100_models_cold", |b| {
        b.iter(|| {
            let _ = fs::remove_file(&cache_file);
            let config = CheckConfig::default();
            let engine = CheckEngine::new(config);
            engine.check(dir.path())
        })
    });
}

fn bench_cold_500(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    create_synthetic_project(dir.path(), 500);

    // Delete cache file before each iteration for cold runs
    let cache_file = dir.path().join(".ironlayer").join("check_cache.json");
    c.bench_function("500_models_cold", |b| {
        b.iter(|| {
            let _ = fs::remove_file(&cache_file);
            let config = CheckConfig::default();
            let engine = CheckEngine::new(config);
            engine.check(dir.path())
        })
    });
}

fn bench_warm_500(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    create_synthetic_project(dir.path(), 500);

    // Prime the cache with a first run
    let config = CheckConfig::default();
    let engine = CheckEngine::new(config);
    engine.check(dir.path());

    c.bench_function("500_models_warm", |b| {
        b.iter(|| {
            let config = CheckConfig::default();
            let engine = CheckEngine::new(config);
            engine.check(dir.path())
        })
    });
}

criterion_group!(benches, bench_cold_100, bench_cold_500, bench_warm_500);
criterion_main!(benches);
