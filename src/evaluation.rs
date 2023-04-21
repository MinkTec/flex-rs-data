use polars::{lazy::dsl::{cols, Expr}, prelude::DataFrame};

// bruh
fn gen_sensor_names(n: usize) -> Vec<String> {
    vec![
        (1..n)
            .into_iter()
            .map(|x| format!("l{}", x))
            .collect::<Vec<String>>(),
        (1..n)
            .into_iter()
            .map(|x| format!("r{}", x))
            .collect::<Vec<String>>(),
    ]
    .into_iter()
    .flat_map(|x| x).collect()
}

