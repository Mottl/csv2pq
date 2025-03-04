use std::{
    fs::{remove_file, rename, File},
    io::Write,
    path::Path,
};

/// Temporary file
pub struct TempFile {
    tmp_filename: String,
    file: File,
}

impl TempFile {
    /// Creates a new temporary file
    pub fn create_new(tmp_filename: String) -> std::io::Result<Self> {
        let file = File::create_new(&tmp_filename)?;
        Ok(Self { tmp_filename, file })
    }

    /// Flushes data and renames temporary file to a new one
    pub fn flush_and_rename(self, new_filename: impl AsRef<Path>) -> std::io::Result<()> {
        self.file.sync_all()?;
        rename(&self.tmp_filename, new_filename.as_ref())
    }
}

impl Write for TempFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = remove_file(&self.tmp_filename);
    }
}
