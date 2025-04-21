use crate::index::VectorIndex;
use crate::embedding::{Embedding, EmbeddingConfig, EmbeddingMethod};
use crate::vector::Vector;

use std::collections::HashMap;
use std::fmt;
use std::error::Error;
use num_traits::{Float, Num, NumCast};
use std::fmt::Debug;

#[derive(Debug)]
pub enum VectorDBError {
    CollectionExists,
    CollectionNotFound,
    InvalidQuery,
    EmbeddingError(String),
    OtherError(String),
}

impl fmt::Display for VectorDBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VectorDBError::CollectionExists => write!(f, "Collection already exists"),
            VectorDBError::CollectionNotFound => write!(f, "Collection not found"),
            VectorDBError::InvalidQuery => write!(f, "Invalid query"),
            VectorDBError::EmbeddingError(err) => write!(f, "Embedding error: {}", err),
            VectorDBError::OtherError(err) => write!(f, "Error: {}", err),
        }
    }
}

impl std::error::Error for VectorDBError {}

impl From<Box<dyn Error>> for VectorDBError {
    fn from(err: Box<dyn Error>) -> Self {
        VectorDBError::OtherError(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, VectorDBError>;

pub struct VectorDB<T = f32>
where
    T: Num + NumCast + Clone + Debug + Float,
{
    collections: HashMap<String, VectorIndex<T>>,
    config: EmbeddingConfig
}

impl<T> VectorDB<T>
where
    T: Num + NumCast + Clone + Debug + Float,
{
    pub fn new() -> Self {
        VectorDB {
            collections: HashMap::new(),
            config: EmbeddingConfig::default(),
        }
    }

    pub fn with_config(config: EmbeddingConfig) -> Self {
        VectorDB {
            collections: HashMap::new(),
            config,
        }
    }

    pub fn create_collection(&mut self, name: &str, dim: usize) -> Result<()> {
        if self.collections.contains_key(name) {
            return Err(VectorDBError::CollectionExists);
        }
        self.collections.insert(name.to_string(), VectorIndex::<T>::with_dim(dim));
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

    pub fn insert(&mut self, collection: &str, id: String, vector: Vector<T>) -> Result<()> {
        if let Some(idx) = self.collections.get_mut(collection) {
            idx.insert(id, vector);
            Ok(())
        } else {
            Err(VectorDBError::CollectionNotFound)
        }
    }

    pub fn update(&mut self, collection: &str, id: &str, vector: Vector<T>) -> Result<()> {
        if let Some(idx) = self.collections.get_mut(collection) {
            idx.update(id, vector)
                .map_err(|e| VectorDBError::OtherError(e))?;
            Ok(())
        } else {
            Err(VectorDBError::CollectionNotFound)
        }
    }

    pub fn delete(&mut self, collection: &str, id: &str) -> Result<()> {
        if let Some(idx) = self.collections.get_mut(collection) {
            idx.delete(id);
            Ok(())
        } else {
            Err(VectorDBError::CollectionNotFound)
        }
    }

    pub fn search(&self, collection: &str, query: &Vector<T>, top_k: usize) -> Result<Vec<(String, f32)>> {
        if let Some(idx) = self.collections.get(collection) {
            Ok(idx.search(query, top_k))
        } else {
            Err(VectorDBError::CollectionNotFound)
        }
    }
}

impl VectorDB<f32> {
    fn embed_content(&self, content: &str, dim: usize, technique: &str) -> Result<Vector<f32>> {
        let embedding_method = match technique {
            "random" => EmbeddingMethod::Random,
            "ascii_sum" => EmbeddingMethod::AsciiSum,
            "bag_of_words" => EmbeddingMethod::BagOfWords,
            "tf_idf" => EmbeddingMethod::TfIdf,
            "one_hot" => EmbeddingMethod::OneHotEncoding,
            "character_level" => EmbeddingMethod::CharacterLevel,
            "ngram" => EmbeddingMethod::NGram,
            "hashing_trick" => EmbeddingMethod::HashingTrick,
            "word_count" => EmbeddingMethod::WordCount,
            "word2vec" => EmbeddingMethod::Word2Vec,
            "bert" => EmbeddingMethod::BertEmbedding,
            "external_api" => EmbeddingMethod::ExternalApi,
            _ => {
                println!("Invalid embedding technique '{}'. Falling back to 'ascii_sum'.", technique);
                EmbeddingMethod::AsciiSum
            }
        };
        
        let config = EmbeddingConfig {
            method: embedding_method,
            dim,
            api_key: self.config.api_key.clone(),
            api_endpoint: self.config.api_endpoint.clone(),
            model_path: self.config.model_path.clone(),
            vocab_path: self.config.vocab_path.clone(),
            ngram_size: self.config.ngram_size,
            min_count: self.config.min_count,
            window_size: self.config.window_size,
        };
        
        match Embedding::generate_embedding(content, &config) {
            Ok(embedding) => Ok(Vector::new(&embedding)),
            Err(e) => Err(VectorDBError::EmbeddingError(e.to_string())),
        }
    }
    
    pub fn embed_and_insert(&mut self, collection: &str, id: String, content: &str, technique: &str) -> Result<()> {
        if let Some(idx) = self.collections.get(collection) {
            let dim = idx.dim;
            drop(idx);
            
            let vector = self.embed_content(content, dim, technique)?;
            self.insert(collection, id, vector)
        } else {
            Err(VectorDBError::CollectionNotFound)
        }
    }

    pub fn embed_batch(&mut self, collection: &str, items: Vec<(String, String)>, technique: &str) -> Result<()> {
        for (id, content) in items {
            self.embed_and_insert(collection, id, &content, technique)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::Vector;

    #[test]
    fn test_vector_db_operations() {
        let mut db = VectorDB::<f32>::new();
        
        // Test creating collections
        assert!(db.create_collection("test_collection", 3).is_ok());
        assert!(db.create_collection("test_collection", 3).is_err());
        
        // Test inserting vectors
        let v1 = Vector::new(&[1.0f32, 2.0, 3.0]);
        assert!(db.insert("test_collection", "vec1".to_string(), v1.clone()).is_ok());
        assert!(db.insert("nonexistent", "vec1".to_string(), v1.clone()).is_err());
        
        // Test updating vectors
        let v2 = Vector::new(&[4.0f32, 5.0, 6.0]);
        assert!(db.update("test_collection", "vec1", v2.clone()).is_ok());
        assert!(db.update("test_collection", "nonexistent", v2.clone()).is_err());
        
        // Test searching
        let query = Vector::new(&[4.0f32, 5.0, 6.0]);
        let results = db.search("test_collection", &query, 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "vec1");
        assert_eq!(results[0].1, 1.0);
        
        // Test deleting vectors
        assert!(db.delete("test_collection", "vec1").is_ok());
        let results = db.search("test_collection", &query, 10).unwrap();
        assert_eq!(results.len(), 0);
        
        // Test deleting collections
        assert!(db.delete_collection("test_collection").is_ok());
        assert!(db.delete_collection("test_collection").is_err());
    }
    
    #[test]
    fn test_vector_db_integer_type() {
        let mut db = VectorDB::<f64>::new();
        
        assert!(db.create_collection("int_collection", 2).is_ok());
        
        let v1 = Vector::new(&[10.0, 20.0]);
        let v2 = Vector::new(&[30.0, 40.0]);
        
        assert!(db.insert("int_collection", "vec1".to_string(), v1).is_ok());
        assert!(db.insert("int_collection", "vec2".to_string(), v2).is_ok());
        
        let query = Vector::new(&[15.0, 25.0]);
        let results = db.search("int_collection", &query, 2).unwrap();
        
        assert_eq!(results.len(), 2);
        // First result should be closer to query
        assert_eq!(results[0].0, "vec1");
    }
}