use num::Num;
use num::ToPrimitive;

pub struct StatsUtils;

trait Mean<T> {
    fn mean(self) -> T;
}

impl<T, N> Mean<f64> for T
where
    N: Num + ToPrimitive,
    T: IntoIterator<Item = N>,
{
    fn mean(self) -> f64 {
        let mut count = 0;

        let s = self
            .into_iter()
            .reduce(|a, b| {
                count += 1;
                a + b
            })
            .unwrap()
            .to_f64()
            .unwrap_or(0.0);

        s / count as f64
    }
}
