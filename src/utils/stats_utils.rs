use std::ops::Sub;

use num::Num;
use num::ToPrimitive;

pub struct StatsUtils;

trait Diff<R> {
    fn diff(self) -> R;
}

impl<I, T> Diff<Vec<I>> for T
where
    I: Sub<Output = I> + Copy,
    T: IntoIterator<Item = I>,
{
    fn diff(self) -> Vec<I> {
        self.into_iter().collect::<Vec<I>>()[..]
            .windows(2)
            .map(|x| x[1] - x[0])
            .collect()
    }
}

pub trait Extrema<T> {
    fn extrema(&self) -> (T, T);
}

impl Extrema<f64> for Vec<f64> {
    fn extrema(&self) -> (f64, f64) {
        let mut min = f64::MAX;
        let mut max = f64::MIN;
        self.into_iter().for_each(|x| {
            if x < &min {
                min = x.clone()
            }
            if x > &max {
                max = x.clone()
            }
        });
        (min, max)
    }
}

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
