use crate::vector::Vector;
use std::collections::HashMap;
use num_traits::{Num, NumCast};
use std::fmt::Debug;

pub struct VectorIndex<T>
where
    T: Num + NumCast + Clone + Debug,
{
    pub dim: usize,
    vectors: HashMap<String, Vector<T>>,
}

impl<T> VectorIndex<T>
where
    T: Num + NumCast + Clone + Debug,
{
    pub fn with_dim(dim: usize) -> Self {
        VectorIndex {
            dim,
            vectors: HashMap::new(),
        }
    }

    pub fn insert(&mut self, id: String, vector: Vector<T>) {
        assert_eq!(vector.dim(), self.dim, "Dimension mismatch");
        self.vectors.insert(id, vector);
    }

    pub fn delete(&mut self, id: &str) -> Option<Vector<T>> {
        self.vectors.remove(id)
    }

    pub fn update(&mut self, id: &str, vector: Vector<T>) -> Result<(), String> {
        assert_eq!(vector.dim(), self.dim, "Dimension mismatch");
        if let Some(entry) = self.vectors.get_mut(id) {
            *entry = vector;
            Ok(())
        } else {
            Err(format!("Vector ID '{}' not found", id))
        }
    }

    pub fn search(&self, query: &Vector<T>, top_k: usize) -> Vec<(String, f32)> {
        assert_eq!(query.dim(), self.dim, "Query dimension mismatch");
        let mut sims: Vec<(String, f32)> = self
            .vectors
            .iter()
            .map(|(id, vec)| (id.clone(), query.cosine_similarity(vec)))
            .collect();
        sims.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        sims.into_iter().take(top_k).collect()
    }
    
    pub fn get(&self, id: &str) -> Option<&Vector<T>> {
        self.vectors.get(id)
    }
    
    pub fn len(&self) -> usize {
        self.vectors.len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::Vector;

    #[test]
    fn test_vector_index_f32() {
        let mut index = VectorIndex::<f32>::with_dim(3);
        
        let v1 = Vector::new(&[1.0f32, 0.0, 1.0]);
        let v2 = Vector::new(&[0.0f32, 1.0, 0.0]);
        
        index.insert("vec1".to_string(), v1.clone());
        index.insert("vec2".to_string(), v2);
        
        assert_eq!(index.len(), 2);
        
        let results = index.search(&v1, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "vec1");
        assert_eq!(results[0].1, 1.0);
        
        index.delete("vec1");
        assert_eq!(index.len(), 1);
    }
    
    #[test]
    fn test_vector_index_i32() {
        let mut index = VectorIndex::<i32>::with_dim(2);
        
        let v1 = Vector::new(&[10, 20]);
        let v2 = Vector::new(&[5, 10]);
        
        index.insert("vec1".to_string(), v1);
        index.insert("vec2".to_string(), v2.clone());
        
        let results = index.search(&v2, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "vec2");
        
        index.update("vec2", Vector::new(&[15, 25])).unwrap();
        let updated_vec = index.get("vec2").unwrap();
        assert_eq!(updated_vec.raw()[0], 15);
        assert_eq!(updated_vec.raw()[1], 25);
    }
}