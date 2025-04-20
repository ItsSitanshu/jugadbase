/// src/index.rs

use crate::vector::Vector;
use std::collections::HashMap;

pub struct VectorIndex {
    pub dim: usize,
    vectors: HashMap<String, Vector>,
}

impl VectorIndex {
    pub fn with_dim(dim: usize) -> Self {
        VectorIndex {
            dim,
            vectors: HashMap::new(),
        }
    }

    pub fn insert(&mut self, id: String, vector: Vector) {
        assert_eq!(vector.dim(), self.dim, "Dimension mismatch");
        self.vectors.insert(id, vector);
    }

    pub fn delete(&mut self, id: String) {
        self.vectors.remove(&id);
    }

    pub fn update(&mut self, id: String, vector: Vector) {
        assert_eq!(vector.dim(), self.dim, "Dimension mismatch");
        if let Some(entry) = self.vectors.get_mut(&id) {
            *entry = vector;
        } else {
            panic!("Vector ID '{}' not found", id);
        }
    }

    pub fn search(&self, query: &Vector, top_k: usize) -> Vec<(String, f32)> {
        assert_eq!(query.dim(), self.dim, "Query dimension mismatch");
        let mut sims: Vec<(String, f32)> = self
            .vectors
            .iter()
            .map(|(id, vec)| (id.clone(), query.cosine_similarity(vec)))
            .collect();
        sims.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        sims.into_iter().take(top_k).collect()
    }
}