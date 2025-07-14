//--------------------------------------------------------------------------------------------------
// Macros
//--------------------------------------------------------------------------------------------------

// Helper macro to handle platform differences in mode constants
// On Linux, libc mode constants are u32, on macOS they are u16
#[cfg(test)]
macro_rules! mode_cast {
    ($mode:expr) => {{
        #[cfg(target_os = "macos")]
        {
            $mode as u32
        }
        #[cfg(target_os = "linux")]
        {
            $mode
        }
    }};
}

//--------------------------------------------------------------------------------------------------
// Modules
//--------------------------------------------------------------------------------------------------

#[cfg(test)]
mod create;

#[cfg(test)]
mod metadata;

#[cfg(test)]
mod init_krun;

//--------------------------------------------------------------------------------------------------
// Modules: Helper
//--------------------------------------------------------------------------------------------------

#[cfg(test)]
mod helper {
    use std::{fs::{self, File}, io, os::unix::fs::PermissionsExt, path::PathBuf, process::Command};

    use crate::virtio::{
        fs::filesystem::{ZeroCopyReader, ZeroCopyWriter},
        fs::passthrough::{Config, PassthroughFs},
    };

    use tempfile::TempDir;

    //--------------------------------------------------------------------------------------------------
    // Functions
    //--------------------------------------------------------------------------------------------------

    // Helper function to create a temporary directory with specified files
    pub(super) fn setup_test_dir(files: &[(&str, bool, u32)]) -> io::Result<TempDir> {
        let dir = TempDir::new()?;

        for (path, is_dir, mode) in files {
            let full_path = dir.path().join(path);
            if let Some(parent) = full_path.parent() {
                fs::create_dir_all(parent)?;
            }

            if *is_dir {
                fs::create_dir(&full_path)?;
            } else {
                // Create file with some content for testing
                fs::write(&full_path, format!("content of {}", path))?;
            }

            fs::set_permissions(&full_path, fs::Permissions::from_mode(*mode))?;
        }

        Ok(dir)
    }

    // Helper function to create a passthroughfs with a test directory
    pub(super) fn create_passthroughfs(
        files: Vec<(&str, bool, u32)>,
    ) -> io::Result<(PassthroughFs, TempDir)> {
        let temp_dir = setup_test_dir(&files)?;

        let cfg = Config {
            root_dir: temp_dir.path().to_string_lossy().into_owned(),
            xattr: true,
            ..Default::default()
        };

        let fs = PassthroughFs::new(cfg)?;
        Ok((fs, temp_dir))
    }

    // Debug utility to print the directory structure using tree command
    pub(super) fn debug_print_dir(temp_dir: &TempDir, show_perms: bool) -> io::Result<()> {
        if Command::new("tree").arg("--version").output().is_err() {
            println!(
                "tree command is not accessible. please install it to see the directory structure."
            );
            return Ok(());
        }

        println!("\n=== Directory Structure ===");
        println!("Root: {}", temp_dir.path().display());

        let mut tree_cmd = Command::new("tree");
        tree_cmd.arg("-a"); // show hidden files
        if show_perms {
            tree_cmd.arg("-p");
        }
        let output = tree_cmd.arg(temp_dir.path()).output()?;

        if output.status.success() {
            println!("{}", String::from_utf8_lossy(&output.stdout));
        } else {
            println!(
                "Error running tree command: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        println!("===========================\n");

        Ok(())
    }

    // Helper to get xattr value for testing
    pub(super) fn get_xattr(path: &PathBuf, key: &str) -> io::Result<Option<String>> {
        use std::ffi::CString;

        let path_cstr = CString::new(path.to_string_lossy().as_bytes())?;
        let key_cstr = CString::new(key)?;

        // Check if path is a symlink
        let metadata = std::fs::symlink_metadata(path)?;
        let is_symlink = metadata.file_type().is_symlink();

        let mut buf = vec![0u8; 256];

        #[cfg(target_os = "macos")]
        let res = unsafe {
            let options = if is_symlink { libc::XATTR_NOFOLLOW } else { 0 };
            libc::getxattr(
                path_cstr.as_ptr(),
                key_cstr.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
                0,
                options,
            )
        };

        #[cfg(target_os = "linux")]
        let res = unsafe {
            if is_symlink {
                libc::lgetxattr(
                    path_cstr.as_ptr(),
                    key_cstr.as_ptr(),
                    buf.as_mut_ptr() as *mut libc::c_void,
                    buf.len(),
                )
            } else {
                libc::getxattr(
                    path_cstr.as_ptr(),
                    key_cstr.as_ptr(),
                    buf.as_mut_ptr() as *mut libc::c_void,
                    buf.len(),
                )
            }
        };

        if res < 0 {
            let err = io::Error::last_os_error();
            #[cfg(target_os = "macos")]
            if err.raw_os_error() == Some(libc::ENOATTR) {
                return Ok(None);
            }
            #[cfg(target_os = "linux")]
            if err.raw_os_error() == Some(libc::ENODATA) {
                return Ok(None);
            }
            return Err(err);
        }

        buf.truncate(res as usize);
        Ok(Some(String::from_utf8_lossy(&buf).into_owned()))
    }

    // Helper to set xattr value for testing
    pub(super) fn set_xattr(path: &PathBuf, key: &str, value: &str) -> io::Result<()> {
        use std::ffi::CString;

        let path_cstr = CString::new(path.to_string_lossy().as_bytes())?;
        let key_cstr = CString::new(key)?;
        let value_bytes = value.as_bytes();

        // Check if path is a symlink
        let metadata = std::fs::symlink_metadata(path)?;
        let is_symlink = metadata.file_type().is_symlink();

        #[cfg(target_os = "macos")]
        let res = unsafe {
            let options = if is_symlink { libc::XATTR_NOFOLLOW } else { 0 };
            libc::setxattr(
                path_cstr.as_ptr(),
                key_cstr.as_ptr(),
                value_bytes.as_ptr() as *const libc::c_void,
                value_bytes.len(),
                0,
                options,
            )
        };

        #[cfg(target_os = "linux")]
        let res = unsafe {
            if is_symlink {
                libc::lsetxattr(
                    path_cstr.as_ptr(),
                    key_cstr.as_ptr(),
                    value_bytes.as_ptr() as *const libc::c_void,
                    value_bytes.len(),
                    0,
                )
            } else {
                libc::setxattr(
                    path_cstr.as_ptr(),
                    key_cstr.as_ptr(),
                    value_bytes.as_ptr() as *const libc::c_void,
                    value_bytes.len(),
                    0,
                )
            }
        };

        if res < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    //--------------------------------------------------------------------------------------------------
    // Types
    //--------------------------------------------------------------------------------------------------

    pub(super) struct TestContainer(pub(super) Vec<u8>);

    //--------------------------------------------------------------------------------------------------
    // Trait Implementations
    //--------------------------------------------------------------------------------------------------

    impl io::Write for TestContainer {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl ZeroCopyWriter for TestContainer {
        fn write_from(&mut self, f: &File, count: usize, off: u64) -> io::Result<usize> {
            use std::os::unix::fs::FileExt;

            // Pre-allocate space in our vector to avoid reallocations
            let original_len = self.0.len();
            self.0.resize(original_len + count, 0);

            // Read directly into our vector's buffer
            let bytes_read = f.read_at(&mut self.0[original_len..original_len + count], off)?;

            // Adjust the size to match what was actually read
            self.0.truncate(original_len + bytes_read);

            if bytes_read == 0 && count > 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected EOF",
                ));
            }

            Ok(bytes_read)
        }
    }

    impl io::Read for TestContainer {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let available = self.0.len();
            if available == 0 {
                return Ok(0);
            }

            let amt = std::cmp::min(buf.len(), available);
            buf[..amt].copy_from_slice(&self.0[..amt]);
            Ok(amt)
        }
    }

    impl ZeroCopyReader for TestContainer {
        fn read_to(&mut self, f: &File, count: usize, off: u64) -> io::Result<usize> {
            use std::os::unix::fs::FileExt;

            let available = self.0.len();
            if available == 0 {
                return Ok(0);
            }

            let to_write = std::cmp::min(count, available);
            let written = f.write_at(&self.0[..to_write], off)?;
            Ok(written)
        }
    }
}
