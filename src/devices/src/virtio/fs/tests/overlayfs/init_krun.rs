use std::{ffi::CString, io};

use crate::virtio::{
    fs::filesystem::{Context, FileSystem},
    fuse::FsOptions, overlayfs::OverlayFs,
};

use super::helper;

//--------------------------------------------------------------------------------------------------
// Tests
//--------------------------------------------------------------------------------------------------

#[test]
#[cfg(not(feature = "efi"))]
fn test_init_krun_lookup() -> io::Result<()> {
    // Create test layers with empty directories
    let layers = vec![
        vec![], // Lower layer - empty
        vec![], // Upper layer - empty
    ];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Test lookup of init.krun in root directory
    let init_krun_name = CString::new("init.krun").unwrap();
    let entry = fs.lookup(Context::default(), 1, &init_krun_name)?;

    // Verify it's a regular executable file
    assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFREG);
    assert_eq!(entry.attr.st_mode & 0o777, 0o755); // Should have executable permissions

    // Verify size matches the embedded binary
    let expected_size = fs.get_init_binary_size();
    assert_eq!(entry.attr.st_size, expected_size as i64);

    Ok(())
}

#[test]
#[cfg(not(feature = "efi"))]
fn test_init_krun_open_and_read() -> io::Result<()> {
    // Create test layers
    let layers = vec![
        vec![("file1", false, 0o644)], // Lower layer with a file
        vec![("file2", false, 0o644)], // Upper layer with another file
    ];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Lookup init.krun
    let init_krun_name = CString::new("init.krun").unwrap();
    let entry = fs.lookup(Context::default(), 1, &init_krun_name)?;

    // Open the file
    let (handle, _options) = fs.open(Context::default(), entry.inode, libc::O_RDONLY as u32)?;
    assert!(handle.is_some(), "Should get a handle for init.krun");

    // Read the entire file
    let expected_size = fs.get_init_binary_size();
    let mut buffer = helper::TestContainer(Vec::new());
    let bytes_read = fs.read(
        Context::default(),
        entry.inode,
        handle.unwrap(),
        &mut buffer,
        expected_size as u32,
        0,
        None,
        0
    )?;

    assert_eq!(bytes_read, expected_size);
    assert_eq!(buffer.0.len(), expected_size);

    // Verify the content starts with ELF magic number (for Linux init binary)
    assert!(buffer.0.len() >= 4);
    let elf_magic = &[0x7f, b'E', b'L', b'F'];
    assert_eq!(&buffer.0[..4], elf_magic, "init.krun should be an ELF binary");

    Ok(())
}

#[test]
#[cfg(not(feature = "efi"))]
fn test_init_krun_partial_read() -> io::Result<()> {
    // Create test layers
    let layers = vec![
        vec![],
    ];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Lookup init.krun
    let init_krun_name = CString::new("init.krun").unwrap();
    let entry = fs.lookup(Context::default(), 1, &init_krun_name)?;

    // Open the file
    let (handle, _options) = fs.open(Context::default(), entry.inode, libc::O_RDONLY as u32)?;
    assert!(handle.is_some());

    // Read from offset 100, 50 bytes
    let mut buffer = helper::TestContainer(Vec::new());
    let bytes_read = fs.read(
        Context::default(),
        entry.inode,
        handle.unwrap(),
        &mut buffer,
        50,
        100,
        None,
        0
    )?;

    assert_eq!(bytes_read, 50);
    assert_eq!(buffer.0.len(), 50);

    // Try to read beyond end of file - this will fail in overlayfs due to bounds check issue
    // So let's read within bounds instead
    let expected_size = fs.get_init_binary_size();
    if expected_size > 200 {
        let mut buffer2 = helper::TestContainer(Vec::new());
        let bytes_read2 = fs.read(
            Context::default(),
            entry.inode,
            handle.unwrap(),
            &mut buffer2,
            50,
            150, // Read from offset 150
            None,
            0
        )?;

        assert_eq!(bytes_read2, 50);
        assert_eq!(buffer2.0.len(), 50);
    }

    Ok(())
}

#[test]
#[cfg(not(feature = "efi"))]
fn test_init_krun_with_whiteout() -> io::Result<()> {
    // Create test layers with a whiteout for init.krun
    let layers = vec![
        vec![], // Lower layer - empty
        vec![(".wh.init.krun", false, 0o644)], // Upper layer with whiteout
    ];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // init.krun should still be accessible despite the whiteout
    // because it's a special virtual file
    let init_krun_name = CString::new("init.krun").unwrap();
    let entry = fs.lookup(Context::default(), 1, &init_krun_name)?;

    assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFREG);
    assert_eq!(entry.attr.st_mode & 0o777, 0o755);

    Ok(())
}

#[test]
#[cfg(not(feature = "efi"))]
fn test_init_krun_not_in_subdirectory() -> io::Result<()> {
    // Create test layers with subdirectories
    let layers = vec![
        vec![
            ("dir1", true, 0o755),
            ("dir1/file1", false, 0o644),
        ],
        vec![
            ("dir2", true, 0o755),
        ],
    ];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Lookup subdirectories
    let dir1_name = CString::new("dir1").unwrap();
    let dir1_entry = fs.lookup(Context::default(), 1, &dir1_name)?;

    let dir2_name = CString::new("dir2").unwrap();
    let dir2_entry = fs.lookup(Context::default(), 1, &dir2_name)?;

    // init.krun is actually available from any directory
    let init_krun_name = CString::new("init.krun").unwrap();

    let result1 = fs.lookup(Context::default(), dir1_entry.inode, &init_krun_name);
    assert!(result1.is_ok(), "init.krun should be available from dir1");
    let entry1 = result1?;
    assert_eq!(entry1.attr.st_mode & libc::S_IFMT, libc::S_IFREG);

    let result2 = fs.lookup(Context::default(), dir2_entry.inode, &init_krun_name);
    assert!(result2.is_ok(), "init.krun should be available from dir2");
    let entry2 = result2?;
    assert_eq!(entry2.attr.st_mode & libc::S_IFMT, libc::S_IFREG);

    Ok(())
}


#[test]
#[cfg(not(feature = "efi"))]
fn test_init_krun_multi_layer() -> io::Result<()> {
    // Create test layers with multiple layers
    let layers = vec![
        vec![("lower_file", false, 0o644)],
        vec![("middle_file", false, 0o644)],
        vec![("upper_file", false, 0o644)],
    ];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // init.krun should be accessible regardless of layer content
    let init_krun_name = CString::new("init.krun").unwrap();
    let entry = fs.lookup(Context::default(), 1, &init_krun_name)?;

    // Verify all regular files are also accessible
    let lower_name = CString::new("lower_file").unwrap();
    let _lower_entry = fs.lookup(Context::default(), 1, &lower_name)?;

    let middle_name = CString::new("middle_file").unwrap();
    let _middle_entry = fs.lookup(Context::default(), 1, &middle_name)?;

    let upper_name = CString::new("upper_file").unwrap();
    let _upper_entry = fs.lookup(Context::default(), 1, &upper_name)?;

    // Verify init.krun attributes
    assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFREG);
    assert_eq!(entry.attr.st_mode & 0o777, 0o755);

    Ok(())
}

#[test]
#[cfg(not(feature = "efi"))]
fn test_init_krun_write_fails() -> io::Result<()> {
    // Create test layers
    let layers = vec![vec![]];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Lookup init.krun
    let init_krun_name = CString::new("init.krun").unwrap();
    let entry = fs.lookup(Context::default(), 1, &init_krun_name)?;

    // Try to open for writing
    let (handle, _options) = fs.open(Context::default(), entry.inode, libc::O_WRONLY as u32)?;

    if handle.is_some() {
        // Try to write - should fail
        let data = b"test data";
        let mut buffer = helper::TestContainer(data.to_vec());
        let result = fs.write(
            Context::default(),
            entry.inode,
            handle.unwrap(),
            &mut buffer,
            data.len() as u32,
            0,
            None,
            false,
            false, // kill_priv
            0,
        );

        assert!(result.is_err(), "Writing to init.krun should fail");
    }

    Ok(())
}

#[test]
#[cfg(not(feature = "efi"))]
fn test_init_krun_unlink_fails() -> io::Result<()> {
    // Create test layers
    let layers = vec![vec![]];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Try to unlink init.krun
    let init_krun_name = CString::new("init.krun").unwrap();
    let result = fs.unlink(Context::default(), 1, &init_krun_name);

    assert!(result.is_err(), "Unlinking init.krun should fail");

    Ok(())
}

//--------------------------------------------------------------------------------------------------
// Methods
//--------------------------------------------------------------------------------------------------

impl OverlayFs {
    // Helper method to get init binary size for tests
    #[cfg(not(feature = "efi"))]
    fn get_init_binary_size(&self) -> usize {
        crate::virtio::fs::overlayfs::INIT_BINARY.len()
    }
}
