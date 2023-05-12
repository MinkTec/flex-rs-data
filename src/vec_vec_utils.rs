pub trait Transpose {
    fn transpose(self) -> Self;
}

impl<T> Transpose for Vec<Vec<T>>
where
    T: Copy,
{
    fn transpose(self) -> Self {
        if self.len() == 0 {
            return vec![];
        }
        if self[0].len() == 0 {
            return vec![vec![]];
        }

        assert!(self[..].windows(2).all(|x| x[0].len() == x[1].len()));

        let mut outer_vec = Vec::with_capacity(self[0].len());

        for x in 0..(self[0].len()) {
            outer_vec.push(Vec::with_capacity(self.len()));
            for y in 0..(self.len()) {
                // TODO: solve this without copy
                outer_vec[x][y] = self[y][x];
            }
        }
        outer_vec
    }
}
