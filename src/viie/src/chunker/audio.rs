use hound;
use base64::{engine::general_purpose, Engine as _};
use std::io::Cursor;
use anyhow::{Result, anyhow};

use crate::chunker::Chunkable;

pub enum AudioInput {
    Samples(Vec<i16>),
    Base64(String),
}

pub struct AudioChunker;

impl Chunkable for AudioChunker {
    type Input = AudioInput;
    type Output = Vec<i16>;

    fn chunk(input: Self::Input) -> Result<Vec<Self::Output>> {
        let samples = match input {
            AudioInput::Samples(s) => s,
            AudioInput::Base64(b64_string) => {
                let decoded = general_purpose::STANDARD
                    .decode(&b64_string)
                    .map_err(|e| anyhow!("Base64 decode failed: {}", e))?;

                let reader = hound::WavReader::new(Cursor::new(decoded))?;
                reader.into_samples::<i16>()
                      .filter_map(Result::ok)
                      .collect()
            }
        };

        let chunk_duration_ms = 1000;
        let sample_rate = 44100;
        let samples_per_chunk = (sample_rate as f32 * (chunk_duration_ms as f32 / 1000.0)) as usize;

        let chunks = samples
            .chunks(samples_per_chunk)
            .map(|chunk| chunk.to_vec())
            .collect();

        Ok(chunks)
    }
}
