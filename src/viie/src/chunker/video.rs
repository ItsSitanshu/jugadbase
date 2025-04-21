use anyhow::{Result, anyhow};
use base64::{engine::general_purpose, Engine as _};
use opencv::{
    prelude::*,
    videoio,
    imgcodecs,
    imgproc,
    core,
};
use image::{DynamicImage, ImageBuffer, Rgba};
use std::fs::File;
use std::io::Cursor;
use std::path::Path;

use crate::chunker::Chunkable;

pub enum VideoInput {
    FilePath(String),
    Base64(String),
}

pub struct VideoChunker;

impl Chunkable for VideoChunker {
    type Input = VideoInput;
    type Output = DynamicImage;

    fn chunk(input: Self::Input) -> Result<Vec<Self::Output>> {
        let tmp_path = match input {
            VideoInput::FilePath(path) => path,
            VideoInput::Base64(b64) => {
                let decoded = general_purpose::STANDARD
                    .decode(&b64)
                    .map_err(|e| anyhow!("Base64 decode failed: {}", e))?;

                let tmp_path = "/tmp/temp_video.mp4";
                std::fs::write(tmp_path, decoded)?;
                tmp_path.to_string()
            }
        };

        let mut cap = videoio::VideoCapture::from_file(&tmp_path, videoio::CAP_ANY)?;
        if !cap.is_opened()? {
            return Err(anyhow!("Could not open video"));
        }

        let fps = cap.get(videoio::CAP_PROP_FPS)? as usize;
        let every_nth_frame = fps; // Extract 1 frame per second

        let mut frame = Mat::default();
        let mut frames = vec![];
        let mut idx = 0;

        while cap.read(&mut frame)? {
            if idx % every_nth_frame == 0 {
                let mut rgb = Mat::default();
                imgproc::cvt_color(&frame, &mut rgb, imgproc::COLOR_BGR2RGB, 0)?;

                let size = rgb.size()?;
                let width = size.width as u32;
                let height = size.height as u32;
                let buf = rgb.data_bytes()?;

                let image = DynamicImage::ImageRgb8(
                    ImageBuffer::<image::Rgb<u8>, _>::from_raw(width, height, buf.to_vec())
                        .ok_or_else(|| anyhow!("Failed to convert Mat to image"))?,
                );
                frames.push(image);
            }
            idx += 1;
        }

        Ok(frames)
    }
}
