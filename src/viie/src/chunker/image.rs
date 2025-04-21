use image::{DynamicImage, GenericImageView, ImageFormat};
use base64::{engine::general_purpose, Engine as _};
use std::io::Cursor;
use anyhow::{Result, anyhow};

use crate::chunker::Chunkable;

pub enum ImageInput {
    Dynamic(DynamicImage),
    Base64(String),
}

pub struct ImageChunker;

impl Chunkable for ImageChunker {
    type Input = ImageInput;
    type Output = DynamicImage;

    fn chunk(input: Self::Input) -> Result<Vec<Self::Output>> {
        let image = match input {
            ImageInput::Dynamic(img) => img,
            ImageInput::Base64(b64_string) => {
                let decoded = general_purpose::STANDARD
                    .decode(&b64_string)
                    .map_err(|e| anyhow!("Base64 decode failed: {}", e))?;
                let cursor = Cursor::new(decoded);
                image::load(cursor, ImageFormat::Png)?
            }
        };

        let (width, height) = image.dimensions();
        let patch_size = 224;
        let mut patches = vec![];

        for x in (0..width).step_by(patch_size as usize) {
            for y in (0..height).step_by(patch_size as usize) {
                let patch = image.crop_imm(x, y, patch_size, patch_size);
                patches.push(patch);
            }
        }

        Ok(patches)
    }
}
