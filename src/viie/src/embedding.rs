use rust_bert::pipelines::sentence_embeddings::{SentenceEmbeddingsBuilder, SentenceEmbeddingsModel};
use anyhow::{Result, anyhow};
use std::error::Error;
use std::sync::Arc;
use tch::{CModule, Device, Tensor, Kind};
use tokio::sync::Mutex;
use reqwest::Client;
use std::collections::{HashMap, HashSet};
use std::cmp::Ordering;
use rand::Rng;

// use ndarray;
use serde::{Deserialize, Serialize};

pub struct Embedding;

#[derive(Debug, Clone)]
pub enum EmbeddingMethod {
    Random,
    AsciiSum,
    BagOfWords,
    TfIdf,
    OneHotEncoding,
    CharacterLevel,
    CosineSimilarity,
    WordCount,
    NGram,
    PosTagging,
    HashingTrick,
    Word2Vec,
    GloVe,
    FastText,
    BertEmbedding,
    UniversalSentenceEncoder,
    Doc2Vec,
    Lsa,
    Lda,
    PcaReduced,
    ExternalApi,
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    embedding: Vec<f32>,
    #[serde(default)]
    status: Option<String>,
}

pub struct EmbeddingConfig {
    pub method: EmbeddingMethod,
    pub dim: usize,
    pub api_key: Option<String>,
    pub api_endpoint: Option<String>,
    pub model_path: Option<String>,
    pub vocab_path: Option<String>,
    pub ngram_size: Option<usize>,
    pub min_count: Option<usize>,
    pub window_size: Option<usize>,
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        EmbeddingConfig {
            method: EmbeddingMethod::BagOfWords,
            dim: 100,
            api_key: None,
            api_endpoint: None,
            model_path: None,
            vocab_path: None,
            ngram_size: Some(3),
            min_count: Some(5),
            window_size: Some(5),
        }
    }
}

impl Embedding {
    // Random Embedding: Generates a vector with random values
    pub fn random_embedding(dim: usize) -> Vec<f32> {
        let mut rng = rand::rng();
        (0..dim)
            .map(|_| rng.random_range(-1.0..1.0))
            .collect()
    }
    
    // Simple Embedding: Uses the sum of ASCII values of the characters in the string
    pub fn ascii_sum_embedding(content: &str, dim: usize) -> Vec<f32> {
        let content_sum: u32 = content.chars().map(|c| c as u32).sum();
        let mut rng = rand::rng();
        
        (0..dim)
            .map(|i| {
                let value = (content_sum as f32 + i as f32) / 10.0;
                value + rng.random_range(-0.5..0.5) 
            })
            .collect()
    }

    // Bag of Words: Count occurrences of each word
    pub fn bag_of_words(text: &str, vocabulary: &[String], dim: usize) -> Vec<f32> {
        let words: Vec<&str> = text.split_whitespace().collect();
        let mut word_counts: HashMap<String, f32> = HashMap::new();
        
        for word in words {
            let normalized = word.to_lowercase();
            *word_counts.entry(normalized).or_insert(0.0) += 1.0;
        }
        
        let mut embedding = Vec::with_capacity(dim);
        for (_i, term) in vocabulary.iter().enumerate().take(dim) {
            embedding.push(*word_counts.get(term).unwrap_or(&0.0));
        }
        
        while embedding.len() < dim {
            embedding.push(0.0);
        }
        
        embedding
    }
    
    // TF-IDF Embedding
    pub fn tf_idf(text: &str, corpus: &[&str], vocabulary: &[String], dim: usize) -> Vec<f32> {
        let words: Vec<&str> = text.split_whitespace().collect();
        let total_words = words.len() as f32;
        
        let mut term_freqs: HashMap<String, f32> = HashMap::new();
        for word in words {
            let normalized = word.to_lowercase();
            *term_freqs.entry(normalized).or_insert(0.0) += 1.0 / total_words;
        }
        
        let mut doc_freqs: HashMap<String, f32> = HashMap::new();
        let total_docs = corpus.len() as f32;
        
        for doc in corpus {
            let doc_words: HashSet<String> = doc
                .split_whitespace()
                .map(|w| w.to_lowercase())
                .collect();
                
            for word in doc_words {
                *doc_freqs.entry(word).or_insert(0.0) += 1.0;
            }
        }
        
        let mut embedding = Vec::with_capacity(dim);
        for (_i, term) in vocabulary.iter().enumerate().take(dim) {
            let tf = *term_freqs.get(term).unwrap_or(&0.0);
            let df = *doc_freqs.get(term).unwrap_or(&0.0) + 1.0; // Add 1 to avoid division by zero
            let idf = (total_docs / df).ln();
            embedding.push(tf * idf);
        }
        
        while embedding.len() < dim {
            embedding.push(0.0);
        }
        
        embedding
    }
    
    // One-hot encoding for words
    pub fn one_hot_encoding(text: &str, vocabulary: &[String], dim: usize) -> Vec<f32> {
        let words: Vec<String> = text.split_whitespace()
            .map(|w| w.to_lowercase())
            .collect();
            
        let mut embedding = vec![0.0; dim];
        
        for word in words {
            if let Some(index) = vocabulary.iter().position(|v| v == &word) {
                if index < dim {
                    embedding[index] = 1.0;
                }
            }
        }
        
        embedding
    }
    
    // Character-level embedding
    pub fn character_level(text: &str, dim: usize) -> Vec<f32> {
        let chars: Vec<char> = text.chars().collect();
        let mut result = vec![0.0; dim];
        
        for (i, &c) in chars.iter().enumerate() {
            let index = i % dim;
            result[index] += (c as u32 as f32) / 128.0;
        }
        
        let max_val = result.iter()
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
            .copied() // This copies the f32 value out of the reference
            .unwrap_or(0.0);
        
        if max_val > 0.0 {
            for val in result.iter_mut() {
                *val /= max_val;
            }
        }
        
        result
    }

    // N-gram embedding
    pub fn ngram_embedding(text: &str, n: usize, dim: usize) -> Vec<f32> {
        let chars: Vec<char> = text.chars().collect();
        let mut ngram_counts: HashMap<String, f32> = HashMap::new();
        
        for i in 0..=chars.len().saturating_sub(n) {
            let ngram: String = chars[i..i+n].iter().collect();
            *ngram_counts.entry(ngram).or_insert(0.0) += 1.0;
        }
        
        let mut embedding = vec![0.0; dim];
        for (ngram, count) in ngram_counts {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            use std::hash::{Hash, Hasher};
            ngram.hash(&mut hasher);
            let hash = hasher.finish() as usize;
            let index = hash % dim;
            embedding[index] += count;
        }
        
        let sum: f32 = embedding.iter().sum();
        if sum > 0.0 {
            for val in embedding.iter_mut() {
                *val /= sum;
            }
        }
        
        embedding
    }
    
    // Hashing trick (feature hashing)
    pub fn hashing_trick(text: &str, dim: usize) -> Vec<f32> {
        let words: Vec<String> = text.split_whitespace()
            .map(|w| w.to_lowercase())
            .collect();
            
        let mut embedding = vec![0.0; dim];
        
        for word in words {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            use std::hash::{Hash, Hasher};
            word.hash(&mut hasher);
            let hash = hasher.finish() as usize;
            let index = hash % dim;
            
            let sign_hasher = std::collections::hash_map::DefaultHasher::new();
            word.hash(&mut hasher);
            let sign_hash = hasher.finish();
            let sign = if sign_hash % 2 == 0 { 1.0 } else { -1.0 };
            
            embedding[index] += sign;
        }
        
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for val in embedding.iter_mut() {
                *val /= norm;
            }
        }
        
        embedding
    }
    
    // Word count statistics
    pub fn word_count_stats(text: &str, dim: usize) -> Vec<f32> {
        let words: Vec<&str> = text.split_whitespace().collect();
        let total_words = words.len();
        
        if total_words == 0 {
            return vec![0.0; dim];
        }
        
        let word_count = total_words as f32;
        let unique_words = words.iter().collect::<HashSet<_>>().len() as f32;
        let avg_word_len = words.iter().map(|w| w.len()).sum::<usize>() as f32 / word_count;
        
        let mut alpha_count = 0;
        let mut digit_count = 0;
        let mut punct_count = 0;
        let mut upper_count = 0;
        
        for word in &words {
            for c in word.chars() {
                if c.is_alphabetic() {
                    alpha_count += 1;
                    if c.is_uppercase() {
                        upper_count += 1;
                    }
                } else if c.is_digit(10) {
                    digit_count += 1;
                } else if c.is_ascii_punctuation() {
                    punct_count += 1;
                }
            }
        }
        
        let mut stats = vec![
            word_count,
            unique_words,
            unique_words / word_count,  // lexical diversity
            avg_word_len,
            alpha_count as f32 / text.len() as f32,
            digit_count as f32 / text.len() as f32,
            punct_count as f32 / text.len() as f32,
            upper_count as f32 / text.len() as f32,
        ];
        
        while stats.len() < dim {
            stats.push(0.0);
        }
        
        stats.truncate(dim);
        stats
    }

    pub fn generate_embedding(text: &str, config: &EmbeddingConfig) -> Result<Vec<f32>, Box<dyn Error>> {
        match config.method {
            EmbeddingMethod::Random => Ok(Self::random_embedding(config.dim)),
            EmbeddingMethod::AsciiSum => Ok(Self::ascii_sum_embedding(text, config.dim)),
            EmbeddingMethod::BagOfWords => {
                let vocab = vec!["the".to_string(), "and".to_string(), "is".to_string()]; // Example vocabulary
                Ok(Self::bag_of_words(text, &vocab, config.dim))
            },
            EmbeddingMethod::TfIdf => {
                let corpus = vec!["example document one", "example document two"];
                let vocab = vec!["example".to_string(), "document".to_string(), "one".to_string(), "two".to_string()];
                Ok(Self::tf_idf(text, &corpus, &vocab, config.dim))
            },
            EmbeddingMethod::OneHotEncoding => {
                let vocab = vec!["the".to_string(), "and".to_string(), "is".to_string()]; // Example vocabulary
                Ok(Self::one_hot_encoding(text, &vocab, config.dim))
            },
            EmbeddingMethod::CharacterLevel => {
                Ok(Self::character_level(text, config.dim))
            },
            EmbeddingMethod::NGram => {
                let n = config.ngram_size.unwrap_or(3);
                Ok(Self::ngram_embedding(text, n, config.dim))
            },
            EmbeddingMethod::HashingTrick => {
                Ok(Self::hashing_trick(text, config.dim))
            },
            EmbeddingMethod::WordCount => {
                Ok(Self::word_count_stats(text, config.dim))
            },
            EmbeddingMethod::Word2Vec => {
                Err("Word2Vec requires the tch-rs crate with pre-trained models.".into())
            },
            EmbeddingMethod::GloVe => {
                Err("GloVe requires pre-trained embeddings loaded from file.".into())
            },
            EmbeddingMethod::FastText => {
                Err("FastText requires binding to the FastText library.".into())
            },
            EmbeddingMethod::BertEmbedding => {
                Err("BERT embeddings require rust-bert crate.".into())
            },
            EmbeddingMethod::UniversalSentenceEncoder => {
                Err("Universal Sentence Encoder requires TensorFlow bindings.".into())
            },
            EmbeddingMethod::Doc2Vec => {
                Err("Doc2Vec requires specific ML implementation.".into())
            },
            EmbeddingMethod::Lsa => {
                Err("LSA requires linear algebra operations from ndarray.".into())
            },
            EmbeddingMethod::Lda => {
                Err("LDA requires specific topic modeling implementation.".into())
            },
            EmbeddingMethod::PcaReduced => {
                Err("PCA reduction requires linear algebra from ndarray.".into())
            },
            EmbeddingMethod::ExternalApi => {
                if let (Some(api_key), Some(endpoint)) = (&config.api_key, &config.api_endpoint) {
                    // Err("External API call requires reqwest crate and valid API credentials.".into());
                    panic!("{} {}", api_key, endpoint);
                } else {
                    Err("Missing API key or endpoint for external API embedding.".into())
                }
            },
            EmbeddingMethod::CosineSimilarity => {
                Err("Cosine similarity is a comparison method, not an embedding generator.".into())
            },
            EmbeddingMethod::PosTagging => {
                Err("POS tagging requires a specific NLP library.".into())
            },
        }
    }
    
    
    pub fn word2vec_embedding(
        text: &str,
        model_path: &str,
        dim: usize,
        vocab: &HashMap<String, i64>,
    ) -> Result<Vec<f32>> {
        if text.trim().is_empty() {
            return Err(anyhow!("Empty text provided for embedding"));
        }
    
        let device = Device::Cpu;
        let word2vec = CModule::load(model_path)?;
    
        let words: Vec<&str> = text.split_whitespace().collect();
        if words.is_empty() {
            return Err(anyhow!("No words found in text"));
        }
    
        let mut embeddings = Vec::with_capacity(words.len());
    
        for word in words {
            let token_id = vocab
                .get(word)
                .ok_or_else(|| anyhow!("Word '{}' not found in vocabulary", word))?;
    
            let word_tensor = Tensor::f_from_slice(&[*token_id])?.to_device(device);
            let embedding = word2vec
                .forward_ts(&[word_tensor])?
                .to_kind(Kind::Float)
                .flatten(0, -1);
    
            let embedding_vec: Vec<f32> = embedding.try_into()?;
            embeddings.push(embedding_vec);
        }
    
        let mut avg_embedding = vec![0.0; dim];
        for embedding in &embeddings {
            if embedding.len() != dim {
                return Err(anyhow!(
                    "Embedding dimension mismatch: expected {}, got {}",
                    dim,
                    embedding.len()
                ));
            }
    
            for (i, &val) in embedding.iter().enumerate() {
                avg_embedding[i] += val;
            }
        }
    
        let word_count = embeddings.len() as f32;
        for val in &mut avg_embedding {
            *val /= word_count;
        }
    
        let magnitude = avg_embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if magnitude > 0.0 {
            for val in &mut avg_embedding {
                *val /= magnitude;
            }
        }
    
        Ok(avg_embedding)
    }
    
    
    pub fn bert_embedding(text: &str, dim: usize) -> Result<Vec<f32>> {
        if text.trim().is_empty() {
            return Err(anyhow!("Empty text provided for embedding"));
        }
    
        static MODEL_CACHE: once_cell::sync::OnceCell<Arc<Mutex<Option<SentenceEmbeddingsModel>>>> = 
            once_cell::sync::OnceCell::new();
        
        let model_cache = MODEL_CACHE.get_or_init(|| Arc::new(Mutex::new(None)));
        
        let model = SentenceEmbeddingsBuilder::local("path/to/all-MiniLM-L6-v2") // or .remote(...) if online
        .create_model()?;
    
        let embeddings = model.encode(&[text])?;
        if embeddings.is_empty() {
            return Err(anyhow!("Failed to generate embeddings"));
        }
    
        let mut result = embeddings[0].clone();
    
        if result.len() != dim {
            if result.len() > dim {
                result.truncate(dim);
            } else {
                result.resize(dim, 0.0);
            }
        }
    
        let magnitude = result.iter().map(|x| x * x).sum::<f32>().sqrt();
        if magnitude > 0.0 {
            for val in &mut result {
                *val /= magnitude;
            }
        }
    
        Ok(result)
    }
    
    
    pub async fn external_api_embedding(
        text: &str,
        api_key: &str,
        endpoint: &str,
        dim: usize
    ) -> Result<Vec<f32>> {
        if text.trim().is_empty() {
            return Err(anyhow!("Empty text provided for embedding"));
        }
    
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
        
        let payload = serde_json::json!({
            "text": text,
            "dimensions": dim,
            "normalize": true
        });
        
        let response = client.post(endpoint)
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;
        
        if !response.status().is_success() {
            return Err(anyhow!(
                "API request failed with status code: {}, body: {}",
                response.status(),
                response.text().await?
            ));
        }
    
        let result: ApiResponse = response.json().await?;
        
        if result.embedding.len() != dim {
            return Err(anyhow!(
                "API returned embedding with incorrect dimensions: expected {}, got {}",
                dim,
                result.embedding.len()
            ));
        }
    
        Ok(result.embedding)
    }
    

    pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() || a.is_empty() {
            return 0.0;
        }
        
        let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let mag_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let mag_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        
        if mag_a > 0.0 && mag_b > 0.0 {
            dot_product / (mag_a * mag_b)
        } else {
            0.0
        }
    }
}

pub fn create_embedding_generator(method: EmbeddingMethod, dim: usize) -> impl Fn(&str) -> Vec<f32> {
    let config = EmbeddingConfig {
        method,
        dim,
        ..Default::default()
    };
    
    move |text| {
        match Embedding::generate_embedding(text, &config) {
            Ok(embedding) => embedding,
            Err(_) => Embedding::random_embedding(dim), // Fallback to random embedding
        }
    }
}