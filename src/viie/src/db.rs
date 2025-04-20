use crate::index::VectorIndex;
use crate::vector::Vector;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug)]
pub enum VectorDBError {
    CollectionExists,
    CollectionNotFound,
    InvalidQuery,
    OtherError(String),
}

impl fmt::Display for VectorDBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            VectorDBError::CollectionExists => write!(f, "Collection already exists"),
            VectorDBError::CollectionNotFound => write!(f, "Collection not found"),
            VectorDBError::InvalidQuery => write!(f, "Invalid query"),
            VectorDBError::OtherError(ref err) => write!(f, "Error: {}", err),
        }
    }
}



pub type Result<T> = std::result::Result<T, VectorDBError>;

pub struct VectorDB {
    collections: HashMap<String, VectorIndex>,
}

impl VectorDB {
    pub fn new() -> Self {
        VectorDB {
            collections: HashMap::new(),
        }
    }

    pub fn create_collection(&mut self, name: &str, dim: usize) -> Result<()> {
        if self.collections.contains_key(name) {
            return Err(VectorDBError::CollectionExists);
        }
        self.collections.insert(name.to_string(), VectorIndex::with_dim(dim));
        Ok(())
    }

    pub fn delete_collection(&mut self, name: &str) -> Result<()> {
        if self.collections.remove(name).is_some() {
            Ok(())
        } else {
            Err(VectorDBError::CollectionNotFound)
        }
    }

    pub fn list_collections(&self) -> Vec<String> {
        self.collections.keys().cloned().collect()
    }

    pub fn insert(&mut self, collection: &str, id: String, vector: Vector) -> Result<()> {
        if let Some(idx) = self.collections.get_mut(collection) {
            idx.insert(id, vector);
            Ok(())
        } else {
            Err(VectorDBError::CollectionNotFound)
        }
    }

    pub fn update(&mut self, collection: &str, id: String, vector: Vector) -> Result<()> {
        if let Some(idx) = self.collections.get_mut(collection) {
            idx.update(id, vector);
            Ok(())
        } else {
            Err(VectorDBError::CollectionNotFound)
        }
    }

    pub fn delete(&mut self, collection: &str, id: String) -> Result<()> {
        if let Some(idx) = self.collections.get_mut(collection) {
            idx.delete(id);
            Ok(())
        } else {
            Err(VectorDBError::CollectionNotFound)
        }
    }

    pub fn search(&self, collection: &str, query: &Vector, top_k: usize) -> Result<Vec<(String, f32)>> {
        if let Some(idx) = self.collections.get(collection) {
            Ok(idx.search(query, top_k))
        } else {
            Err(VectorDBError::CollectionNotFound)
        }
    }

    fn embed_content(&self, _content: &str, dim: usize) -> Vector {
        // TODO: replace this stub with real embedding logic
        let zeros = vec![0.0; dim];
        Vector::new(&zeros)
    }

    pub fn embed_and_insert(&mut self, collection: &str, id: String, content: &str) -> Result<()> {
        if let Some(idx) = self.collections.get(collection) {
            let dim = idx.dim;
            let vec = self.embed_content(content, dim);
            drop(idx);
            return self.insert(collection, id, vec);
        }
        Err(VectorDBError::CollectionNotFound)
    }

    pub fn embed_batch(&mut self, collection: &str, items: Vec<(String, String)>) -> Result<()> {
        for (id, content) in items {
            self.embed_and_insert(collection, id, &content)?;
        }
        Ok(())
    }
}
