use derive_more::Deref;
use serde::{Deserialize, Serialize};

use crate::utils::stats_utils::Extrema;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct NDHistogram {
    baskets: Vec<usize>,
    borders: Vec<Vec<f64>>,
}

impl NDHistogram {
    pub fn n(&self) -> usize {
        match self.borders.first() {
            Some(v) => v.len(),
            None => 0,
        }
    }

    pub fn dim(&self) -> usize {
        self.borders.len()
    }

    pub fn new(
        data: Vec<Vec<f64>>,
        n: usize,
        limits: Option<Vec<Option<(f64, f64)>>>,
    ) -> NDHistogram {
        if data.is_empty() || data.first().unwrap().is_empty() {
            return NDHistogram {
                baskets: vec![],
                borders: vec![],
            };
        }

        assert!(
            data.iter()
                .map(|x| x.len() == data.first().unwrap().len())
                .reduce(|a, b| a && b)
                .unwrap()
                && (limits.is_none() || limits.clone().unwrap().len() == data.len())
        );

        let limits = match limits {
            Some(l) => l,
            None => (0..n).map(|_| None).collect(),
        };

        let borders = data
            .iter()
            .enumerate()
            .map(|x| NDHistogram::gen_histogram_border(limits[x.0].unwrap_or(x.1.extrema()), n))
            .collect::<Vec<Vec<f64>>>();

        let mut baskets: Vec<usize> = vec![0; n.pow(data.len() as u32)];

        let deltas = borders
            .iter()
            .map(|x| (0.00000001 + x.last().unwrap() - x.first().unwrap()).max(0.000001))
            .collect::<Vec<f64>>();

        let mut coords: Vec<usize> = vec![0; data.len()];

        for i in 0..data.first().unwrap().len() {
            for d in 0..data.len() {
                coords[d] = (((data[d][i] - borders[d].first().unwrap()) / deltas[d]) * (n as f64))
                    .floor() as usize;
            }
            let index: usize = NDCoords(coords.clone()).into();
            baskets[index] += 1;
        }

        NDHistogram { baskets, borders }
    }

    fn gen_histogram_border(extrema: (f64, f64), n: usize) -> Vec<f64> {
        (0..=n)
            .into_iter()
            .map(|i| extrema.0 + (extrema.1 - extrema.0) / n as f64 * i as f64)
            .collect()
    }
}

#[derive(Debug, Deref)]
struct NDCoords(Vec<usize>);

impl Into<usize> for NDCoords {
    fn into(self) -> usize {
        self.iter()
            .rev()
            .enumerate()
            .map(|x| x.1 * self.len().pow(x.0 as u32))
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use crate::clustered_data::NDHistogram;

    #[test]
    fn test() {
        let inner = (1..100000).map(|x| x as f64).collect::<Vec<f64>>();

        let h = NDHistogram::new(vec![inner.clone(), inner], 3, None);

        assert_eq!(
            NDHistogram {
                baskets: vec![],
                borders: vec![]
            },
            h
        );
    }
}
