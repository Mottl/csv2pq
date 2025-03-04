use std::{
    fs::File,
    io::{Read, Seek},
    path::Path,
};

use flate2::read::MultiGzDecoder;

/// Rewindable reader which can be used for both compressed and compressed files
pub enum RewindableReader {
    /// Uncompressed
    Plain(File),
    /// Compressed
    Compressed(MultiGzDecoder<File>),
}

impl RewindableReader {
    /// Opens plain or gzipped file and returns a reader
    pub fn open(filename: &Path) -> std::io::Result<RewindableReader> {
        let basename = filename.to_str().unwrap();
        let file = File::open(filename)?;
        if basename.ends_with(".gz") {
            Ok(RewindableReader::Compressed(MultiGzDecoder::new(file)))
        } else {
            Ok(RewindableReader::Plain(file))
        }
    }

    /// Rewinds a reader
    pub fn rewind(self) -> std::io::Result<Self> {
        match self {
            RewindableReader::Plain(mut file) => {
                file.rewind()?;
                Ok(RewindableReader::Plain(file))
            }
            RewindableReader::Compressed(multi_gz_decoder) => {
                let mut file = multi_gz_decoder.into_inner();
                file.rewind()?;
                Ok(RewindableReader::Compressed(MultiGzDecoder::new(file)))
            }
        }
    }
}

impl Read for RewindableReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            RewindableReader::Plain(file) => file.read(buf),
            RewindableReader::Compressed(multi_gz_decoder) => multi_gz_decoder.read(buf),
        }
    }
}
