//! [`AlignedSbeWriter`] writes SBE messages in fixed-size, zero-padded chunks.
//!
//! The file format is the template's: messages never cross a chunk boundary;
//! a reader can therefore decode every chunk independently and stop at the
//! all-zero sentinel. The default chunk size is the template's 64 MiB so an
//! `AlignedSbeReader::open()` from the unmodified template decodes a closed
//! capture.
//!
//! Flushing is incremental: [`Self::flush`] writes only the encoded bytes, not
//! the padding. Padding is emitted when a chunk rotates and by
//! [`Self::finish`] (or [`Self::sync_all`]) at shutdown. This keeps
//! event-to-visible-output latency low without writing a whole chunk of zeroes
//! on every flush. The same aligned format is used for stdout, so redirecting
//! stdout to a file produces a capture the template reader can open.

use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
};

use sha2::{Digest, Sha256};

use super::dtos::SbeEncode;

// Every schema in `dtos` uses the standard eight-byte SBE message header.
const HEADER_LENGTH: usize = 8;

/// Writes SBE messages to an aligned chunked output.
pub struct AlignedSbeWriter<W: Write + Send = File> {
    writer: W,
    buf: Vec<u8>,
    chunk: usize,
    /// Bytes of `buf` already handed to `writer`, so a flush never repeats
    /// data.
    committed: usize,
    bytes_written: u64,
    /// Optional SHA-256 over every byte handed to the sink, including chunk
    /// padding. Enabled for rotated segments so the manifest can detect
    /// truncation or modification after finalization.
    checksum: Option<Sha256>,
}

impl AlignedSbeWriter<BufWriter<File>> {
    /// Create an aligned [`AlignedSbeWriter`] at a new path.
    ///
    /// Creation is exclusive: an existing file is an error. Callers that
    /// replace an artifact with [`Self::create`].
    pub fn create_new(path: impl AsRef<Path>, chunk: usize) -> anyhow::Result<Self> {
        validate_chunk(chunk)?;
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        Ok(Self::from_file(file, chunk))
    }

    /// Create an aligned [`AlignedSbeWriter`] writing to `path` with `chunk`
    /// chunk size, clearing the file at `path` if it exists.
    ///
    /// This is the explicit-overwrite constructor. Production startup rejects
    /// existing artifacts unless the operator enables `output.overwrite`.
    pub fn create(path: impl AsRef<Path>, chunk: usize) -> anyhow::Result<Self> {
        validate_chunk(chunk)?;
        let file = File::create(path)?;
        Ok(Self::from_file(file, chunk))
    }

    fn from_file(file: File, chunk: usize) -> Self {
        // The chunk buffer is the alignment unit; the OS write buffer only
        // needs to absorb syscall overhead. Do not double the 64 MiB default.
        let write_buffer = chunk.min(1 << 20);
        Self::new(BufWriter::with_capacity(write_buffer, file), chunk)
    }

    /// Finish the final chunk, flush, and persist the file.
    ///
    /// `fsync` is only valid for regular files. Pipes, FIFOs, character
    /// devices such as `/dev/stdout`, and sockets return `EINVAL` on Linux, so
    /// those sinks are flushed but not synced.
    pub fn sync_all(&mut self) -> anyhow::Result<()> {
        self.finish()?;
        let file = self.writer.get_ref();
        if file.metadata()?.file_type().is_file() {
            file.sync_all()?;
        }
        Ok(())
    }

    /// Flush visible bytes and persist them without closing the chunk.
    ///
    /// Periodic durability uses this instead of [`Self::sync_all`]: padding
    /// the active chunk on every sync would grow the capture by up to one
    /// chunk per interval. Readers can decode the committed messages; the
    /// remainder of the chunk stays unwritten until it fills or the file is
    /// finalized.
    pub fn sync_data(&mut self) -> anyhow::Result<()> {
        self.write_uncommitted()?;
        self.writer.flush()?;
        let file = self.writer.get_ref();
        if file.metadata()?.file_type().is_file() {
            file.sync_data()?;
        }
        Ok(())
    }
}

impl<W: Write + Send> AlignedSbeWriter<W> {
    /// Create an aligned writer over an arbitrary byte sink, such as an
    /// in-memory buffer. Chunks are padded exactly like file output.
    pub fn from_writer(writer: W, chunk: usize) -> anyhow::Result<Self> {
        validate_chunk(chunk)?;
        Ok(Self::new(writer, chunk))
    }

    fn new(writer: W, chunk: usize) -> Self {
        Self {
            writer,
            buf: Vec::with_capacity(chunk),
            chunk,
            committed: 0,
            bytes_written: 0,
            checksum: None,
        }
    }

    /// Start a SHA-256 over all bytes written from this point.
    ///
    /// Must be called before the first message so the digest covers the whole
    /// stream, including padding.
    pub fn enable_checksum(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.bytes_written == 0 && self.buf.is_empty(),
            "checksum must be enabled before the first message"
        );
        self.checksum = Some(Sha256::new());
        Ok(())
    }

    /// Hex SHA-256 of every byte written so far, when checksumming is enabled.
    #[must_use]
    pub fn checksum_hex(&self) -> Option<String> {
        self.checksum.as_ref().map(|hasher| {
            let digest = hasher.clone().finalize();
            let mut text = String::with_capacity(digest.len() * 2);
            for byte in digest {
                use std::fmt::Write as _;
                let _ = write!(text, "{byte:02x}");
            }
            text
        })
    }

    /// Total bytes handed to the underlying writer, including zero padding.
    #[must_use]
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Configured chunk size in bytes.
    #[must_use]
    pub fn chunk_size(&self) -> usize {
        self.chunk
    }

    /// Encodes a complete message. In aligned mode a message that would cross
    /// the current chunk boundary closes the chunk first.
    pub fn write_msg<T: SbeEncode + ?Sized>(&mut self, message: &T) -> anyhow::Result<()> {
        let len = message.encoded_length_with_header()?;
        let payload_limit = self.chunk - HEADER_LENGTH;
        anyhow::ensure!(len <= payload_limit, "SBE message does not fit in a chunk");
        if len > payload_limit - self.buf.len() {
            self.close_chunk()?;
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

    /// Make every encoded byte visible to readers without padding the current
    /// chunk. A reader can decode the complete messages written so far; a
    /// graceful close later pads the final chunk to the configured alignment.
    pub fn flush(&mut self) -> anyhow::Result<()> {
        self.write_uncommitted()?;
        self.writer.flush()?;
        Ok(())
    }

    /// Pad and write the final chunk, then flush. Idempotent.
    pub fn finish(&mut self) -> anyhow::Result<()> {
        self.close_chunk()?;
        self.writer.flush()?;
        Ok(())
    }

    fn write_uncommitted(&mut self) -> anyhow::Result<()> {
        // Retry short writes explicitly so `committed` and `bytes_written`
        // describe the bytes the sink accepted, including a prefix accepted
        // before an error. A later flush resumes at that boundary instead of
        // repeating the prefix.
        while self.committed < self.buf.len() {
            let start = self.committed;
            let end = self.buf.len();
            match self.writer.write(&self.buf[start..end]) {
                Ok(0) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    )
                    .into());
                }
                Ok(written) => {
                    let written_end = start + written;
                    self.committed = written_end;
                    self.bytes_written += written as u64;
                    if let Some(checksum) = &mut self.checksum {
                        checksum.update(&self.buf[start..written_end]);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    /// Rotate to the next chunk. Only meaningful for aligned output.
    ///
    /// An empty buffer is left alone: a capture with no messages stays empty,
    /// and a file that ends exactly on a chunk boundary does not gain a
    /// spurious empty padded chunk.
    fn close_chunk(&mut self) -> anyhow::Result<()> {
        if self.buf.is_empty() {
            self.committed = 0;
            return Ok(());
        }
        if self.buf.len() < self.chunk {
            self.buf.resize(self.chunk, 0);
        }
        self.write_uncommitted()?;
        self.buf.clear();
        self.committed = 0;
        Ok(())
    }
}

fn validate_chunk(chunk: usize) -> anyhow::Result<()> {
    anyhow::ensure!(
        chunk > HEADER_LENGTH,
        "chunk must leave room for SBE data and an SBE header"
    );
    anyhow::ensure!(
        chunk.is_power_of_two(),
        "chunk size must be a power of two so the reader can auto-detect it"
    );
    anyhow::ensure!(chunk >= 1024, "chunk size must be at least 1024 bytes");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor, Write};
    use std::sync::{Arc, Mutex};

    use super::*;

    struct TestMessage;

    impl SbeEncode for TestMessage {
        fn encoded_length_with_header(&self) -> anyhow::Result<usize> {
            Ok(16)
        }

        fn encode(&self, buf: &mut [u8]) -> anyhow::Result<usize> {
            buf[..16].fill(1);
            Ok(16)
        }
    }

    /// Deterministic byte sink: at most `max_per_write` bytes per call, with an
    /// optional absolute byte limit that fails the next write.
    #[derive(Clone)]
    struct ShortSink {
        written: Arc<Mutex<Vec<u8>>>,
        write_calls: Arc<Mutex<usize>>,
        max_per_write: usize,
        fail_at: Arc<Mutex<Option<usize>>>,
    }

    impl ShortSink {
        fn new(max_per_write: usize, fail_at: Option<usize>) -> Self {
            Self {
                written: Arc::new(Mutex::new(Vec::new())),
                write_calls: Arc::new(Mutex::new(0)),
                max_per_write,
                fail_at: Arc::new(Mutex::new(fail_at)),
            }
        }

        fn bytes(&self) -> Vec<u8> {
            self.written.lock().unwrap().clone()
        }

        fn write_calls(&self) -> usize {
            *self.write_calls.lock().unwrap()
        }

        fn allow_writes(&self) {
            *self.fail_at.lock().unwrap() = None;
        }
    }

    impl Write for ShortSink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            *self.write_calls.lock().unwrap() += 1;
            let mut written = self.written.lock().unwrap();
            let mut take = buf.len().min(self.max_per_write);
            if let Some(limit) = *self.fail_at.lock().unwrap() {
                if written.len() >= limit {
                    return Err(io::Error::other("injected short-write failure"));
                }
                take = take.min(limit - written.len());
            }
            if take == 0 {
                return Ok(0);
            }
            written.extend_from_slice(&buf[..take]);
            Ok(take)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Deterministic sink with a staging buffer: `write` accepts bytes into
    /// the stage, and `flush` either delivers the stage or fails before
    /// delivery. A failed flush therefore models a buffer whose contents are
    /// still pending, not bytes that were lost.
    #[derive(Clone)]
    struct FlushFailSink {
        staged: Arc<Mutex<Vec<u8>>>,
        delivered: Arc<Mutex<Vec<u8>>>,
        fail_flushes: Arc<Mutex<usize>>,
        flush_calls: Arc<Mutex<usize>>,
    }

    impl FlushFailSink {
        fn new(fail_flushes: usize) -> Self {
            Self {
                staged: Arc::new(Mutex::new(Vec::new())),
                delivered: Arc::new(Mutex::new(Vec::new())),
                fail_flushes: Arc::new(Mutex::new(fail_flushes)),
                flush_calls: Arc::new(Mutex::new(0)),
            }
        }

        fn delivered(&self) -> Vec<u8> {
            self.delivered.lock().unwrap().clone()
        }

        fn flush_calls(&self) -> usize {
            *self.flush_calls.lock().unwrap()
        }
    }

    impl Write for FlushFailSink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.staged.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            *self.flush_calls.lock().unwrap() += 1;
            let mut remaining = self.fail_flushes.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Err(io::Error::other("injected flush failure"));
            }
            drop(remaining);
            let mut staged = self.staged.lock().unwrap();
            self.delivered.lock().unwrap().append(&mut staged);
            Ok(())
        }
    }

    fn assert_messages_then_padding(bytes: &[u8], messages: usize, chunk: usize) {
        assert_eq!(bytes.len(), chunk);
        let message_bytes = messages * 16;
        assert!(bytes[..message_bytes].iter().all(|byte| *byte == 1));
        assert!(bytes[message_bytes..].iter().all(|byte| *byte == 0));
    }

    fn assert_message_then_padding(bytes: &[u8], chunk: usize) {
        assert_messages_then_padding(bytes, 1, chunk);
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    #[test]
    fn short_writes_are_completed_without_data_loss() {
        let sink = ShortSink::new(7, None);
        let mut writer = AlignedSbeWriter::from_writer(sink.clone(), 1024).unwrap();
        writer.write_msg(&TestMessage).unwrap();
        writer.flush().unwrap();
        assert_eq!(writer.bytes_written(), 16);
        writer.finish().unwrap();
        assert_eq!(writer.bytes_written(), 1024);
        assert!(sink.write_calls() > 1);
        assert_message_then_padding(&sink.bytes(), 1024);
    }

    #[test]
    fn write_error_before_any_byte_is_reported_without_progress() {
        let sink = ShortSink::new(64, Some(0));
        let mut writer = AlignedSbeWriter::from_writer(sink.clone(), 1024).unwrap();
        writer.write_msg(&TestMessage).unwrap();
        let error = writer.flush().unwrap_err();
        assert_eq!(sink.bytes().len(), 0);
        assert_eq!(writer.bytes_written(), 0);
        assert!(error.to_string().contains("injected short-write failure"));
    }

    #[test]
    fn partial_write_error_records_the_prefix_and_resumes_without_duplication() {
        let sink = ShortSink::new(4, Some(5));
        let mut writer = AlignedSbeWriter::from_writer(sink.clone(), 1024).unwrap();
        writer.write_msg(&TestMessage).unwrap();
        assert!(writer.flush().is_err());
        assert_eq!(sink.bytes().len(), 5);
        assert_eq!(writer.bytes_written(), 5);

        sink.allow_writes();
        writer.flush().unwrap();
        assert_eq!(sink.bytes().len(), 16);
        assert_eq!(writer.bytes_written(), 16);

        writer.finish().unwrap();
        assert_message_then_padding(&sink.bytes(), 1024);
    }

    #[test]
    fn partial_write_checksum_covers_the_accepted_prefix_and_resumed_bytes() {
        let sink = ShortSink::new(4, Some(5));
        let mut writer = AlignedSbeWriter::from_writer(sink.clone(), 1024).unwrap();
        writer.enable_checksum().unwrap();
        writer.write_msg(&TestMessage).unwrap();
        assert!(writer.flush().is_err());
        sink.allow_writes();
        writer.flush().unwrap();
        writer.finish().unwrap();
        assert_eq!(
            writer.checksum_hex().as_deref(),
            Some(sha256_hex(&sink.bytes()).as_str())
        );
    }

    #[test]
    fn flush_failure_keeps_accepted_bytes_and_retry_delivers_each_once() {
        let sink = FlushFailSink::new(1);
        let mut writer = AlignedSbeWriter::from_writer(sink.clone(), 1024).unwrap();
        writer.enable_checksum().unwrap();
        writer.write_msg(&TestMessage).unwrap();

        let error = writer.flush().unwrap_err();
        assert!(
            error.to_string().contains("injected flush failure"),
            "unexpected error: {error:#}"
        );
        assert_eq!(writer.bytes_written(), 16);
        assert!(
            sink.delivered().is_empty(),
            "a failed flush must not deliver the staged bytes"
        );

        // The failed flush left the bytes pending. Appending another message
        // before the retry must not repeat or reorder the first one.
        writer.write_msg(&TestMessage).unwrap();
        writer.flush().unwrap();
        assert_eq!(writer.bytes_written(), 32);
        assert_eq!(sink.delivered().len(), 32);
        assert!(sink.delivered()[..32].iter().all(|byte| *byte == 1));

        writer.finish().unwrap();
        assert_messages_then_padding(&sink.delivered(), 2, 1024);
        assert_eq!(
            writer.checksum_hex().as_deref(),
            Some(sha256_hex(&sink.delivered()).as_str())
        );
    }

    #[test]
    fn finish_retry_after_flush_failure_delivers_the_padded_chunk_once() {
        let sink = FlushFailSink::new(1);
        let mut writer = AlignedSbeWriter::from_writer(sink.clone(), 1024).unwrap();
        writer.write_msg(&TestMessage).unwrap();

        let error = writer.finish().unwrap_err();
        assert!(
            error.to_string().contains("injected flush failure"),
            "unexpected error: {error:#}"
        );
        assert_eq!(writer.bytes_written(), 1024);
        assert!(sink.delivered().is_empty());

        writer.finish().unwrap();
        assert_message_then_padding(&sink.delivered(), 1024);
        assert_eq!(writer.bytes_written(), 1024);

        // A further finish re-flushes an empty stage and must not append a
        // second chunk.
        let calls = sink.flush_calls();
        writer.finish().unwrap();
        assert_eq!(sink.delivered().len(), 1024);
        assert_eq!(sink.flush_calls(), calls + 1);
    }

    #[test]
    fn aligned_writer_pads_only_on_finish() {
        let mut writer = AlignedSbeWriter::from_writer(Cursor::new(Vec::new()), 4096).unwrap();
        writer.flush().unwrap();
        assert_eq!(writer.bytes_written(), 0);
        writer.finish().unwrap();
        // No messages: an empty aligned capture is an empty file.
        assert_eq!(writer.bytes_written(), 0);
    }

    #[test]
    fn flush_is_incremental_and_finish_pads() {
        let mut writer = AlignedSbeWriter::from_writer(Cursor::new(Vec::new()), 1024).unwrap();
        writer.write_msg(&TestMessage).unwrap();
        writer.flush().unwrap();
        assert_eq!(writer.bytes_written(), 16);
        writer.finish().unwrap();
        assert_eq!(writer.bytes_written(), 1024);
        // Finishing again must not add a spurious empty chunk.
        writer.finish().unwrap();
        assert_eq!(writer.bytes_written(), 1024);
    }
}
