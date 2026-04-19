//! Benchmarks for the hot-path math. A regression on these numbers is a
//! regression on the 1 kHz monitor / 1 ms opportunity loop.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use xemm_rust::strategy::{OpportunityEvaluator, OrderSide};

fn evaluate_buy(c: &mut Criterion) {
    let evaluator = OpportunityEvaluator::new(1.0, 2.5, 10.0, 0.01);
    c.bench_function("evaluate_buy_opportunity", |b| {
        b.iter(|| {
            let _ = black_box(evaluator.evaluate_buy_opportunity(
                black_box(100.0),
                black_box(20.0),
                black_box(0),
            ));
        })
    });
}

fn evaluate_sell(c: &mut Criterion) {
    let evaluator = OpportunityEvaluator::new(1.0, 2.5, 10.0, 0.01);
    c.bench_function("evaluate_sell_opportunity", |b| {
        b.iter(|| {
            let _ = black_box(evaluator.evaluate_sell_opportunity(
                black_box(100.0),
                black_box(20.0),
                black_box(0),
            ));
        })
    });
}

fn recalculate_profit(c: &mut Criterion) {
    let evaluator = OpportunityEvaluator::new(1.0, 2.5, 10.0, 0.01);
    c.bench_function("recalculate_profit_raw", |b| {
        b.iter(|| {
            let _ = black_box(evaluator.recalculate_profit_raw(
                black_box(OrderSide::Buy),
                black_box(100.0),
                black_box(100.1),
                black_box(100.2),
            ));
        })
    });
}

criterion_group!(benches, evaluate_buy, evaluate_sell, recalculate_profit);
criterion_main!(benches);
