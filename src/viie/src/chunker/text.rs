use crate::chunker::Chunkable;

pub struct TextChunker;

impl Chunkable for TextChunker {
    type Input = String;
    type Output = String;

    fn chunk(text: Self::Input, max_length: i32) -> Result<Vec<Self::Output>> {
        let chunks = text
            .split_terminator(['.', '!', '?'])
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();

        let mut merged_chunks = vec![];
        let mut current = String::new();

        for chunk in chunks {
            if current.len() + chunk.len() < max_length {
                current.push_str(" ");
                current.push_str(chunk);
            } else {
                merged_chunks.push(current.trim().to_string());
                current = chunk.to_string();
            }
        }

        if !current.is_empty() {
            merged_chunks.push(current.trim().to_string());
        }

        Ok(merged_chunks)
    }
}
