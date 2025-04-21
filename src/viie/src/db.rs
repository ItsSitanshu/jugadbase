use crate::index::VectorIndex;
use crate::embedding::{Embedding, EmbeddingConfig, EmbeddingMethod};
use crate::vector::Vector;

use std::collections::HashMap;
use std::fmt;
use std::error::Error;

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

pub struct VectorDB {
    collections: HashMap<String, VectorIndex>,
    config: EmbeddingConfig
}

impl VectorDB {
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

    fn embed_content(&self, content: &str, dim: usize, technique: &str) -> Result<Vector> {
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
        
        // Create a temporary config with the specified method
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