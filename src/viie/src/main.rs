// src/main.rs

mod vector; 
mod db;
mod index;

use db::VectorDB;
use vector::Vector;
use std::io::{self, Write};  

fn main() {
    let mut db = VectorDB::new();

    println!("Welcome to VIBE Vector Database!");
    println!("Type 'help' for available commands.");

    loop {
        print!("> ");
        io::stdout().flush().unwrap();  

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let input = input.trim();

            match input {
            "help" => {
                println!("Available commands:");
                println!("  create <collection_name> <dimension>   - Create a new collection");
                println!("  insert <collection_name> <id> <values>  - Insert a vector");
                println!("  update <collection_name> <id> <values>       - Update a vector");
                println!("  delete <collection_name> <id>       - Delete a vector");
                println!("  search <collection_name> <values>       - Search for top-k similar vectors");
                println!("  list                                    - List all collections");
                println!("  delete <collection_name>                - Delete a collection");
                println!("  exit                                    - Exit the application");
            }

            cmd if cmd.starts_with("create") => {
                let parts: Vec<&str> = cmd.split_whitespace().collect();
                if parts.len() == 3 {
                    if let Ok(dim) = parts[2].parse::<usize>() {
                        let _ = db.create_collection(parts[1], dim);
                        println!("Collection '{}' with dimension {} created", parts[1], dim);
                    } else {
                        println!("Invalid dimension.");
                    }
                } else {
                    println!("Usage: create <collection_name> <dimension>");
                }
            }

            cmd if cmd.starts_with("insert") => {
                let parts: Vec<&str> = cmd.split_whitespace().collect();
                if parts.len() == 4 {
                    let values: Vec<f32> = parts[3]
                        .split(',')
                        .filter_map(|s| s.trim().parse().ok())
                        .collect();
                    let vector = Vector::new(&values);
                    let _ = db.insert(parts[1], parts[2].to_string(), vector);
                    println!("Inserted vector into '{}'", parts[1]);
                } else {
                    println!("Usage: insert <collection_name> <id> <values>");
                }
            }

            cmd if cmd.starts_with("update") => {
                let parts: Vec<&str> = cmd.split_whitespace().collect();
                if parts.len() == 4 {
                    let values: Vec<f32> = parts[3]
                        .split(',')
                        .filter_map(|s| s.trim().parse().ok())
                        .collect();
                    let vector = Vector::new(&values);
                    let _ = db.update(parts[1], parts[2].to_string(), vector);
                    println!("Updated vector into '{}'", parts[1]);
                } else {
                    println!("Usage: update <collection_name> <id> <values>");
                }
            }

            cmd if cmd.starts_with("delete") => {
                let parts: Vec<&str> = cmd.split_whitespace().collect();
                if parts.len() == 3 {
                    let _ = db.delete(parts[0], parts[1].to_string());
                } else {
                    println!("Usage: delete <collection_name> <id>")
                }
            }

            cmd if cmd.starts_with("search") => {
                let parts: Vec<&str> = cmd.split_whitespace().collect();
                if parts.len() == 3 {
                    let values: Vec<f32> = parts[2]
                        .split(',')
                        .filter_map(|s| s.trim().parse().ok())
                        .collect();
                    let query = Vector::new(&values);
                    let top_k = 5;
                    match db.search(parts[1], &query, top_k) {
                        Ok(results) => {
                            println!("Top {} matches:", top_k);
                            for (id, score) in results {
                                println!("  {} => {:.4}", id, score);
                            }
                        },
                        Err(err) => {
                            println!("Error occurred: {}", err);
                        },
                    }
                    
                } else {
                    println!("Usage: search <collection_name> <values>");
                }
            }

            "list" => {
                let cols = db.list_collections();
                if cols.is_empty() {
                    println!("No collections yet.");
                } else {
                    println!("Collections:");
                    for c in cols {
                        println!("  - {}", c);
                    }
                }
            }

            cmd if cmd.starts_with("delete") => {
                let parts: Vec<&str> = cmd.split_whitespace().collect();
                if parts.len() == 2 {
                    let _ = db.delete_collection(parts[1]);
                    println!("Collection '{}' deleted", parts[1]);
                } else {
                    println!("Usage: delete <collection_name>");
                }
            }

            "exit" => {
                println!("Goodbye!");
                break;
            }

            _ => {
                println!("Unknown command. Type 'help' for available commands.");
            }
        }
    }
}