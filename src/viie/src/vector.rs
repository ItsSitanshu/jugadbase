use ndarray::{Array1, Zip};
use num_traits::{Float, Num, NumCast};
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct Vector<T>
where
    T: Num + NumCast + Clone + Debug,
{
    data: Array1<T>,
}

impl<T> Vector<T>
where
    T: Num + NumCast + Clone + Debug,
{
    pub fn new(data: &[T]) -> Self {
        Vector {
            data: Array1::from(data.to_vec()),
        }
    }

    pub fn dim(&self) -> usize {
        self.data.len()
    }

    pub fn to_f32(&self) -> Vector<f32> {
        let casted = self
            .data
            .iter()
            .map(|x| NumCast::from(x.clone()).unwrap_or(0.0))
            .collect::<Vec<_>>();
        Vector::new(&casted)
    }

    pub fn cosine_similarity(&self, other: &Vector<T>) -> f32 {
        let a = self.to_f32();
        let b = other.to_f32();

        let dot = a.data.dot(&b.data);
        let norm_a = a.data.mapv(|x| x * x).sum().sqrt();
        let norm_b = b.data.mapv(|x| x * x).sum().sqrt();
        dot / (norm_a * norm_b)
    }

    pub fn raw(&self) -> &Array1<T> {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dim_f32() {
        let v = Vector::new(&[1.0f32, 2.0, 3.0]);
        assert_eq!(v.dim(), 3);
    }

    #[test]
    fn test_dim_f64() {
        let v = Vector::new(&[1.0f64, 2.0, 3.0]);
        assert_eq!(v.dim(), 3);
    }

    #[test]
    fn test_cosine_similarity_f32() {
        let v1 = Vector::new(&[1.0f32, 0.0, 1.0]);
        let v2 = Vector::new(&[1.0f32, 0.0, 1.0]);
        assert_eq!(v1.cosine_similarity(&v2), 1.0);
    }

    #[test]
    fn test_cosine_similarity_f64() {
        let v1 = Vector::new(&[1.0f64, 0.0, 1.0]);
        let v2 = Vector::new(&[1.0f64, 0.0, 1.0]);
        assert_eq!(v1.cosine_similarity(&v2), 1.0);
    }

    #[test]
    fn test_cosine_similarity_i16() {
        let v1 = Vector::new(&[1000i16, 0, 1000]);
        let v2 = Vector::new(&[1000i16, 0, 1000]);
        assert_eq!(v1.cosine_similarity(&v2), 1.0);
    }

}
