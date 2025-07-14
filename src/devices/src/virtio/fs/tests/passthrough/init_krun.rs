use std::{ffi::CString, io};

use crate::virtio::{
    fs::filesystem::{Context, FileSystem},
    fuse::FsOptions, passthrough::PassthroughFs,
};

use super::helper;

//--------------------------------------------------------------------------------------------------
// Tests
//--------------------------------------------------------------------------------------------------

#[test]
#[cfg(not(feature = "efi"))]
fn test_init_krun_lookup() -> io::Result<()> {
    // Create an empty test directory
    let files = vec![];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, false)?;

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
    // Create an empty test directory
    let files = vec![];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, false)?;

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
    // Create an empty test directory
    let files = vec![];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, false)?;

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

    // Read from near the end of file
    let expected_size = fs.get_init_binary_size();
    if expected_size > 20 {
        let mut buffer2 = helper::TestContainer(Vec::new());
        let bytes_read2 = fs.read(
            Context::default(),
            entry.inode,
            handle.unwrap(),
            &mut buffer2,
            10,
            (expected_size - 10) as u64,
            None,
            0
        )?;

        assert_eq!(bytes_read2, 10);
        assert_eq!(buffer2.0.len(), 10);
    }

    Ok(())
}

#[test]
#[cfg(not(feature = "efi"))]
fn test_init_krun_available_everywhere() -> io::Result<()> {
    // Create test directory with subdirectory
    let files = vec![
        ("subdir", true, 0o755),
    ];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // First lookup the subdirectory
    let subdir_name = CString::new("subdir").unwrap();
    let subdir_entry = fs.lookup(Context::default(), 1, &subdir_name)?;

    // init.krun is actually available from any directory in passthrough
    let init_krun_name = CString::new("init.krun").unwrap();
    let result = fs.lookup(Context::default(), subdir_entry.inode, &init_krun_name);

    // Based on the implementation, init.krun is available from any parent
    assert!(result.is_ok(), "init.krun should be available from any directory");
    let entry = result?;
    assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFREG);
    assert_eq!(entry.attr.st_mode & 0o777, 0o755);

    Ok(())
}

#[test]
#[cfg(not(feature = "efi"))]
fn test_init_krun_open_write_mode() -> io::Result<()> {
    // Create an empty test directory
    let files = vec![];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Lookup init.krun
    let init_krun_name = CString::new("init.krun").unwrap();
    let entry = fs.lookup(Context::default(), 1, &init_krun_name)?;

    // Try to open for writing - should get handle since init.krun is read-only at FS level
    let (handle, _options) = fs.open(Context::default(), entry.inode, libc::O_WRONLY as u32)?;
    assert!(handle.is_some(), "Should get handle even for write mode on init.krun");

    Ok(())
}

#[test]
#[cfg(not(feature = "efi"))]
fn test_init_krun_setattr_fails() -> io::Result<()> {
    use crate::virtio::fuse::SetattrValid;

    // Create an empty test directory
    let files = vec![];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Lookup init.krun
    let init_krun_name = CString::new("init.krun").unwrap();
    let entry = fs.lookup(Context::default(), 1, &init_krun_name)?;

    // Try to change permissions
    let mut attr = entry.attr;
    attr.st_mode = (attr.st_mode & !0o777) | 0o644;
    let valid = SetattrValid::MODE;
    let result = fs.setattr(Context::default(), entry.inode, attr, None, valid);

    assert!(result.is_err(), "Changing attributes of init.krun should fail");

    Ok(())
}

#[test]
#[cfg(not(feature = "efi"))]
fn test_init_krun_unlink_fails() -> io::Result<()> {
    // Create an empty test directory
    let files = vec![];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Try to unlink init.krun
    let init_krun_name = CString::new("init.krun").unwrap();
    let result = fs.unlink(Context::default(), 1, &init_krun_name);

    assert!(result.is_err(), "Unlinking init.krun should fail");

    Ok(())
}

#[test]
#[cfg(not(feature = "efi"))]
fn test_init_krun_getattr() -> io::Result<()> {
    // Create test directory with a regular file for comparison
    let files = vec![("regular_file", false, 0o644)];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // First lookup init.krun to get its inode
    let init_krun_name = CString::new("init.krun").unwrap();
    let entry = fs.lookup(Context::default(), 1, &init_krun_name)?;
    let init_inode = entry.inode;

    // Test getattr on init.krun
    let (attr, timeout) = fs.getattr(Context::default(), init_inode, None)?;

    // Verify the attributes match what we expect
    assert_eq!(attr.st_ino, init_inode);
    assert_eq!(attr.st_mode & libc::S_IFMT, libc::S_IFREG, "Should be a regular file");
    assert_eq!(attr.st_mode & 0o777, 0o755, "Should have executable permissions");
    assert_eq!(attr.st_nlink, 1, "Should have one link");
    assert_eq!(attr.st_uid, 0, "Should be owned by root");
    assert_eq!(attr.st_gid, 0, "Should be in root group");
    assert_eq!(attr.st_blksize, 4096, "Block size should be 4096");
    
    // Verify size matches the embedded binary
    let expected_size = crate::virtio::fs::passthrough::INIT_BINARY.len();
    assert_eq!(attr.st_size, expected_size as i64, "Size should match the embedded binary");
    
    // Verify blocks calculation (rounded up to 512-byte blocks)
    let expected_blocks = (expected_size as i64 + 511) / 512;
    assert_eq!(attr.st_blocks, expected_blocks, "Blocks should be correctly calculated");

    // Verify timeout is set correctly
    assert!(timeout.as_secs() > 0, "Timeout should be positive");

    // Test getattr on a regular file for comparison
    let regular_name = CString::new("regular_file").unwrap();
    let regular_entry = fs.lookup(Context::default(), 1, &regular_name)?;
    let (regular_attr, _) = fs.getattr(Context::default(), regular_entry.inode, None)?;

    // Verify the inodes are different
    assert_ne!(attr.st_ino, regular_attr.st_ino, "init.krun should have a different inode than regular files");

    Ok(())
}

#[test]
#[cfg(not(feature = "efi"))]
fn test_init_krun_getattr_after_open() -> io::Result<()> {
    // Create an empty test directory
    let files = vec![];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Lookup init.krun
    let init_krun_name = CString::new("init.krun").unwrap();
    let entry = fs.lookup(Context::default(), 1, &init_krun_name)?;

    // Open the file
    let (handle, _options) = fs.open(Context::default(), entry.inode, libc::O_RDONLY as u32)?;
    assert!(handle.is_some(), "Should get a handle for init.krun");

    // Test getattr with handle
    let (attr_with_handle, _) = fs.getattr(Context::default(), entry.inode, handle)?;

    // Test getattr without handle
    let (attr_without_handle, _) = fs.getattr(Context::default(), entry.inode, None)?;

    // Both should return the same attributes
    assert_eq!(attr_with_handle.st_size, attr_without_handle.st_size);
    assert_eq!(attr_with_handle.st_mode, attr_without_handle.st_mode);
    assert_eq!(attr_with_handle.st_ino, attr_without_handle.st_ino);

    Ok(())
}

//--------------------------------------------------------------------------------------------------
// Methods
//--------------------------------------------------------------------------------------------------

impl PassthroughFs {
    // Helper method to get init binary size for tests
    #[cfg(not(feature = "efi"))]
    fn get_init_binary_size(&self) -> usize {
        crate::virtio::fs::passthrough::INIT_BINARY.len()
    }
}

