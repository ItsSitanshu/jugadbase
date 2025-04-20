// src/main.rs

mod vector;
mod index;

use vector::Vector;
use index::VectorIndex;

fn main() {
    let mut index = VectorIndex::new();

    index.insert("doc1".to_string(), Vector::new(&[0.1, 0.2, 0.3]));
    index.insert("doc2".to_string(), Vector::new(&[0.4, 0.5, 0.6]));
    index.insert("doc3".to_string(), Vector::new(&[0.7, 0.8, 0.9]));

    let query = Vector::new(&[0.2, 0.3, 0.4]);

    let results = index.search(&query, 2);

    println!("Top 2 most similar vectors:");
    for (id, similarity) in results {
        println!("ID: {}, Similarity: {}", id, similarity);
    }
}