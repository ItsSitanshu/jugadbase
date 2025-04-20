// src/vector.rs

use ndarray::Array1;
use std::f32;

#[derive(Debug)]
pub struct Vector {
	data: Array1<f32>,
}

impl Vector {
	pub fn new(data: &[f32]) -> Self {
		Vector {
			data: Array1::from(data.to_vec()),
		}
	}

	pub fn cosine_similarity(&self, other: &Vector) -> f32 {
		let dot_product = self.data.dot(&other.data);
		let self_norm = self.data.mapv(|x| x * x).sum().sqrt();
		let other_norm = other.data.mapv(|x| x * x).sum().sqrt();

		dot_product / (self_norm * other_norm)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_cosine_similarity() {
		let v1 = Vector::new(&[1.0, 0.0, 1.0]);
		let v2 = Vector::new(&[1.0, 0.0, 1.0]);
		assert_eq!(v1.cosine_similarity(&v2), 1.0);
	}
}
