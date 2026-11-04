//! [`AlignedSbeWriter`] writes SBE messages in fixed-size, zero-padded file chunks.

use std::{fs::File, io::Write, path::Path};

use super::dtos::SbeEncode;

// Every schema in `dtos` uses the standard eight-byte SBE message header.
const HEADER_LENGTH: usize = 8;

/// Writes SBE messages to a flat file in fixed size, zero padded chunks.
///
/// See [`AlignedSbeReader`](crate::util::AlignedSbeReader) for more details on why
/// this struct exists.
///
/// Every chunk reserves enough trailing zeroes for an empty SBE header, so a
/// reader can stop decoding at the sentinel without carrying a frame across a
/// chunk boundary.
///
/// Call [`Self::flush`] to write the final buffered chunk before dropping the
/// writer.
pub struct AlignedSbeWriter {
    file: File,
    buf: Vec<u8>,
    chunk: usize,
}

impl AlignedSbeWriter {
    /// Create an [`AlignedSbeWriter`], writing to `path` with `chunk` chunk
    /// size. Clears the file at `path`, if it exists.
    pub fn create(path: impl AsRef<Path>, chunk: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(
            chunk > HEADER_LENGTH,
            "chunk must leave room for SBE data and an SBE header"
        );
        Ok(Self {
            file: File::create(path)?,
            buf: Vec::with_capacity(chunk),
            chunk,
        })
    }

    /// Encodes a complete message directly into the chunk buffer. Rotates to a
    /// new chunk if it does not fit in the existing chunk.
    pub fn write_msg<T: SbeEncode + ?Sized>(&mut self, message: &T) -> anyhow::Result<()> {
        let len = message.encoded_length_with_header()?;
        let payload_limit = self.chunk - HEADER_LENGTH;
        anyhow::ensure!(len <= payload_limit, "SBE message does not fit in a chunk");
        if self.buf.len() + len > payload_limit {
            self.flush()?;
        }

        self.buf.reserve(len);
        let start = self.buf.len();
        // SAFETY: We reserved at least `len` bytes in `self.buf`. We encode directly into
        // the uninitialized tail slice to eliminate redundant zero-initialization (memset).
        // `self.buf.set_len` is only updated after `encode` succeeds and verifies `written == len`.
        unsafe {
            let slice = std::slice::from_raw_parts_mut(self.buf.as_mut_ptr().add(start), len);
            message.encode(slice).and_then(|written| {
                anyhow::ensure!(
                    written == len,
                    "SBE encoded length mismatch: expected {len} bytes, wrote {written}"
                );
                self.buf.set_len(start + written);
                Ok(())
            })
        }
    }

    /// Flush the current buffer to disk, and clear it.
    pub fn flush(&mut self) -> anyhow::Result<()> {
        if !self.buf.is_empty() {
            self.buf.resize(self.chunk, 0);
            let write_res = self.file.write_all(&self.buf);
            self.buf.clear();
            write_res?;
        }
        Ok(())
    }
}
