// src/index.rs

use crate::vector::Vector;
use std::collections::HashMap;

pub struct VectorIndex {
  	vectors: HashMap<String, Vector>,
}

impl VectorIndex {
  pub fn new() -> Self {
    VectorIndex {
    	vectors: HashMap::new(),
    }
  }

  pub fn insert(&mut self, id: String, vector: Vector) {
    self.vectors.insert(id, vector);
  }

  pub fn search(&self, query: &Vector, top_k: usize) -> Vec<(String, f32)> {
    let mut similarities: Vec<(String, f32)> = self.vectors.iter()
    	.map(|(id, vector)| (id.clone(), query.cosine_similarity(vector)))
      .collect();
    
    similarities.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    similarities.into_iter().take(top_k).collect()
  }
}