#[cfg(test)]
mod create;

#[cfg(test)]
mod metadata;


//--------------------------------------------------------------------------------------------------
// Modules: Helper
//--------------------------------------------------------------------------------------------------

#[cfg(test)]
mod helper {
    use std::{
        fs,
        io,
        os::unix::fs::PermissionsExt,
        path::PathBuf,
        process::Command,
    };

    use crate::virtio::fs::passthrough::{Config, PassthroughFs};

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
            println!("tree command is not accessible. please install it to see the directory structure.");
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

        let mut buf = vec![0u8; 256];

        let res = unsafe {
            libc::getxattr(
                path_cstr.as_ptr(),
                key_cstr.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
            )
        };

        if res < 0 {
            let err = io::Error::last_os_error();
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

        let res = unsafe {
            libc::setxattr(
                path_cstr.as_ptr(),
                key_cstr.as_ptr(),
                value_bytes.as_ptr() as *const libc::c_void,
                value_bytes.len(),
                0,
            )
        };

        if res < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }
}
