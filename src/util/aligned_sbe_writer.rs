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

        let start = self.buf.len();
        self.buf.resize(start + len, 0);
        let result = message.encode(&mut self.buf[start..]).and_then(|written| {
            anyhow::ensure!(
                written == len,
                "SBE encoded length mismatch: expected {len} bytes, wrote {written}"
            );
            Ok(())
        });
        if result.is_err() {
            self.buf.truncate(start);
        }
        result
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

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingMessage(bool);

    impl SbeEncode for FailingMessage {
        fn encoded_length_with_header(&self) -> anyhow::Result<usize> {
            Ok(8)
        }

        fn encode(&self, buf: &mut [u8]) -> anyhow::Result<usize> {
            // Safe encoders may read their buffer before writing it.
            assert!(buf.iter().all(|byte| *byte == 0));
            buf.fill(42);
            if self.0 {
                anyhow::bail!("encoding failed");
            }
            Ok(7)
        }
    }

    #[test]
    fn failed_or_short_encoding_preserves_prior_messages() -> anyhow::Result<()> {
        for fail in [false, true] {
            let file = tempfile::NamedTempFile::new()?;
            let mut writer = AlignedSbeWriter::create(file.path(), 32)?;
            writer.buf.extend_from_slice(&[1, 2, 3]);
            assert!(writer.write_msg(&FailingMessage(fail)).is_err());
            assert_eq!(writer.buf, [1, 2, 3]);
            writer.flush()?;
            let bytes = std::fs::read(file.path())?;
            assert_eq!(&bytes[..3], &[1, 2, 3]);
            assert!(bytes[3..].iter().all(|byte| *byte == 0));
        }
        Ok(())
    }
}
