use std::{ffi::CString, io};

use crate::virtio::{
    fs::filesystem::{Context, FileSystem},
    fuse::{FsOptions, SetattrValid},
};

use super::helper;

//--------------------------------------------------------------------------------------------------
// Tests
//--------------------------------------------------------------------------------------------------

#[test]
fn test_getattr_basic() -> io::Result<()> {
    // Create test files with different permissions
    let files = vec![
        ("file1", false, 0o644),
        ("file2", false, 0o600),
        ("dir1", true, 0o755),
        ("dir2", true, 0o700),
    ];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, true)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Test getattr on regular file
    let file1_name = CString::new("file1").unwrap();
    let file1_entry = fs.lookup(Context { uid: 1000, gid: 1000, pid: 1234 }, 1, &file1_name)?;
    let (file1_attr, _) = fs.getattr(Context { uid: 1000, gid: 1000, pid: 1234 }, file1_entry.inode, None)?;
    assert_eq!(file1_attr.st_mode & 0o777, 0o644);
    assert_eq!(file1_attr.st_mode & libc::S_IFMT, libc::S_IFREG);

    // Test getattr on directory
    let dir1_name = CString::new("dir1").unwrap();
    let dir1_entry = fs.lookup(Context { uid: 1000, gid: 1000, pid: 1234 }, 1, &dir1_name)?;
    let (dir1_attr, _) = fs.getattr(Context { uid: 1000, gid: 1000, pid: 1234 }, dir1_entry.inode, None)?;
    assert_eq!(dir1_attr.st_mode & 0o777, 0o755);
    assert_eq!(dir1_attr.st_mode & libc::S_IFMT, libc::S_IFDIR);

    // Test getattr on file with different permissions
    let file2_name = CString::new("file2").unwrap();
    let file2_entry = fs.lookup(Context { uid: 1000, gid: 1000, pid: 1234 }, 1, &file2_name)?;
    let (file2_attr, _) = fs.getattr(Context { uid: 1000, gid: 1000, pid: 1234 }, file2_entry.inode, None)?;
    assert_eq!(file2_attr.st_mode & 0o777, 0o600);
    assert_eq!(file2_attr.st_mode & libc::S_IFMT, libc::S_IFREG);

    Ok(())
}

#[test]
fn test_setattr_basic() -> io::Result<()> {
    // Create test files
    let files = vec![("file1", false, 0o600), ("dir1", true, 0o755)];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, true)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Test setattr on file - change mode
    let file1_name = CString::new("file1").unwrap();
    let file1_entry = fs.lookup(Context { uid: 1000, gid: 1000, pid: 1234 }, 1, &file1_name)?;

    // Change mode to 0640
    let mut attr = file1_entry.attr;
    attr.st_mode = (attr.st_mode & !0o777) | 0o640;
    let valid = SetattrValid::MODE;
    let (new_attr, _) = fs.setattr(Context { uid: 1000, gid: 1000, pid: 1234 }, file1_entry.inode, attr, None, valid)?;
    assert_eq!(new_attr.st_mode & 0o777, 0o640);

    // Verify the change persists
    let (verify_attr, _) = fs.getattr(Context { uid: 1000, gid: 1000, pid: 1234 }, file1_entry.inode, None)?;
    assert_eq!(verify_attr.st_mode & 0o777, 0o640);

    // Verify xattr was set
    let file_path = temp_dir.path().join("file1");
    let xattr_value = helper::get_xattr(&file_path, "user.containers.override_stat")?;
    assert!(xattr_value.is_some(), "Override xattr should be set");

    // Parse and verify xattr value (format: "uid:gid:mode")
    let xattr_str = xattr_value.unwrap();
    let parts: Vec<&str> = xattr_str.split(':').collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[2], "0100640"); // full mode in octal (S_IFREG | 0640)

    Ok(())
}

#[test]
fn test_setattr_with_context() -> io::Result<()> {
    // Create a test file
    let files = vec![("file1", false, 0o644)];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, true)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Test setattr with different context uid/gid
    let file1_name = CString::new("file1").unwrap();
    let file1_entry = fs.lookup(Context { uid: 1000, gid: 1000, pid: 1234 }, 1, &file1_name)?;

    // Create a context with different uid/gid
    let ctx = Context {
        uid: 2000,
        gid: 2000,
        pid: 1234,
    };

    // Change owner using context
    let mut attr = file1_entry.attr;
    attr.st_uid = ctx.uid;
    attr.st_gid = ctx.gid;
    let valid = SetattrValid::UID | SetattrValid::GID;
    let (new_attr, _) = fs.setattr(ctx, file1_entry.inode, attr, None, valid)?;

    // Verify the virtualized uid/gid
    assert_eq!(new_attr.st_uid, 2000);
    assert_eq!(new_attr.st_gid, 2000);

    // Verify xattr was set
    let file_path = temp_dir.path().join("file1");
    let xattr_value = helper::get_xattr(&file_path, "user.containers.override_stat")?;
    assert!(xattr_value.is_some(), "Override xattr should be set");

    // Parse and verify xattr value
    let xattr_str = xattr_value.unwrap();
    let parts: Vec<&str> = xattr_str.split(':').collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0], "2000"); // uid
    assert_eq!(parts[1], "2000"); // gid

    Ok(())
}

#[test]
fn test_getattr_with_override_xattr() -> io::Result<()> {
    // Create a test file
    let files = vec![("file1", false, 0o644)];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;

    // Set override xattr before init
    let file_path = temp_dir.path().join("file1");
    helper::set_xattr(&file_path, "user.containers.override_stat", "3000:3000:755")?;

    helper::debug_print_dir(&temp_dir, true)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Test getattr returns overridden values
    let file1_name = CString::new("file1").unwrap();
    let file1_entry = fs.lookup(Context { uid: 1000, gid: 1000, pid: 1234 }, 1, &file1_name)?;
    let (file1_attr, _) = fs.getattr(Context { uid: 1000, gid: 1000, pid: 1234 }, file1_entry.inode, None)?;

    // Should get overridden values
    assert_eq!(file1_attr.st_uid, 3000);
    assert_eq!(file1_attr.st_gid, 3000);
    assert_eq!(file1_attr.st_mode & 0o777, 0o755);

    Ok(())
}

#[test]
fn test_access_with_override() -> io::Result<()> {
    // Create a test file with restrictive permissions
    let files = vec![("file1", false, 0o600)];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;

    // Set override xattr to make it readable by others
    let file_path = temp_dir.path().join("file1");
    helper::set_xattr(&file_path, "user.containers.override_stat", "1000:1000:644")?;

    helper::debug_print_dir(&temp_dir, true)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let file1_name = CString::new("file1").unwrap();
    let file1_entry = fs.lookup(Context { uid: 1000, gid: 1000, pid: 1234 }, 1, &file1_name)?;

    // Context with different uid
    let other_ctx = Context {
        uid: 2000,
        gid: 2000,
        pid: 1234,
    };

    // Should be able to read (0o644 allows others to read)
    let result = fs.access(other_ctx, file1_entry.inode, libc::R_OK as u32);
    assert!(result.is_ok(), "Should be able to read with overridden permissions");

    // Should not be able to write (0o644 doesn't allow others to write)
    let result = fs.access(other_ctx, file1_entry.inode, libc::W_OK as u32);
    assert!(result.is_err(), "Should not be able to write with overridden permissions");

    Ok(())
}

#[test]
fn test_setattr_symlink() -> io::Result<()> {
    // Create a test file and symlink
    let files = vec![("target", false, 0o644)];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;

    // Create a symlink manually
    let target_path = temp_dir.path().join("target");
    let link_path = temp_dir.path().join("link");
    std::os::unix::fs::symlink(&target_path, &link_path)?;

    helper::debug_print_dir(&temp_dir, true)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Test setattr on symlink
    let link_name = CString::new("link").unwrap();
    let link_entry = fs.lookup(Context { uid: 1000, gid: 1000, pid: 1234 }, 1, &link_name)?;

    // Symlinks typically have mode 0777, but we'll try to set override
    let mut attr = link_entry.attr;
    attr.st_uid = 4000;
    attr.st_gid = 4000;
    let valid = SetattrValid::UID | SetattrValid::GID;
    let (new_attr, _) = fs.setattr(Context { uid: 1000, gid: 1000, pid: 1234 }, link_entry.inode, attr, None, valid)?;

    // Verify the virtualized uid/gid
    assert_eq!(new_attr.st_uid, 4000);
    assert_eq!(new_attr.st_gid, 4000);

    // Verify xattr was set on the symlink itself (not the target)
    let xattr_value = helper::get_xattr(&link_path, "user.containers.override_stat")?;
    if xattr_value.is_some() {
        // Some filesystems support xattrs on symlinks
        let xattr_str = xattr_value.unwrap();
    let parts: Vec<&str> = xattr_str.split(':').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], "4000"); // uid
        assert_eq!(parts[1], "4000"); // gid
    } else {
        // Filesystem doesn't support xattrs on symlinks, which is fine
        println!("Filesystem doesn't support xattrs on symlinks");
    }

    Ok(())
}

#[test]
fn test_setattr_directory() -> io::Result<()> {
    // Create a test directory
    let files = vec![("dir1", true, 0o755)];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, true)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Test setattr on directory
    let dir1_name = CString::new("dir1").unwrap();
    let dir1_entry = fs.lookup(Context { uid: 1000, gid: 1000, pid: 1234 }, 1, &dir1_name)?;

    // Change mode and owner
    let mut attr = dir1_entry.attr;
    attr.st_mode = (attr.st_mode & !0o777) | 0o750;
    attr.st_uid = 5000;
    attr.st_gid = 5000;
    let valid = SetattrValid::MODE | SetattrValid::UID | SetattrValid::GID;
    let (new_attr, _) = fs.setattr(Context { uid: 1000, gid: 1000, pid: 1234 }, dir1_entry.inode, attr, None, valid)?;

    assert_eq!(new_attr.st_mode & 0o777, 0o750);
    assert_eq!(new_attr.st_uid, 5000);
    assert_eq!(new_attr.st_gid, 5000);

    // Verify persistence
    let (verify_attr, _) = fs.getattr(Context { uid: 1000, gid: 1000, pid: 1234 }, dir1_entry.inode, None)?;
    assert_eq!(verify_attr.st_mode & 0o777, 0o750);
    assert_eq!(verify_attr.st_uid, 5000);
    assert_eq!(verify_attr.st_gid, 5000);

    // Verify xattr
    let dir_path = temp_dir.path().join("dir1");
    let xattr_value = helper::get_xattr(&dir_path, "user.containers.override_stat")?;
    assert!(xattr_value.is_some(), "Override xattr should be set on directory");

    Ok(())
}

#[test]
fn test_getattr_invalid_inode() -> io::Result<()> {
    // Create a simple test file
    let files = vec![("file1", false, 0o644)];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Test getattr with invalid inode
    let invalid_inode = 999999;
    let result = fs.getattr(Context { uid: 1000, gid: 1000, pid: 1234 }, invalid_inode, None);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::EBADF));

    Ok(())
}

#[test]
fn test_device_nodes_rdev() -> io::Result<()> {
    // Create test directory
    let files = vec![("dir1", true, 0o755)];
    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, true)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context { uid: 1000, gid: 1000, pid: 1234 };

    // Test block device with rdev
    let block_name = CString::new("test.blk").unwrap();
    let major = 8u32;  // Typical SCSI disk major
    let minor = 1u32;
    let rdev = libc::makedev(major, minor) as u32;

    let block_entry = fs.mknod(
        ctx,
        1,
        &block_name,
        libc::S_IFBLK | 0o660,
        rdev,
        0o022,
        Default::default(),
    )?;

    // Verify initial attributes including rdev
    assert_eq!(block_entry.attr.st_mode & libc::S_IFMT, libc::S_IFBLK);
    assert_eq!(block_entry.attr.st_mode & 0o777, 0o640);
    assert_eq!(block_entry.attr.st_rdev as u64, rdev as u64);

    // Test getattr preserves rdev
    let (attr, _) = fs.getattr(ctx, block_entry.inode, None)?;
    assert_eq!(attr.st_mode & libc::S_IFMT, libc::S_IFBLK);
    assert_eq!(attr.st_rdev as u64, rdev as u64);

    // Test setattr preserves rdev when changing other attributes
    let mut new_attr = attr;
    new_attr.st_mode = libc::S_IFBLK | 0o600;
    let valid = SetattrValid::MODE;
    let (updated_attr, _) = fs.setattr(ctx, block_entry.inode, new_attr, None, valid)?;
    assert_eq!(updated_attr.st_mode & libc::S_IFMT, libc::S_IFBLK);
    assert_eq!(updated_attr.st_mode & 0o777, 0o600);
    assert_eq!(updated_attr.st_rdev as u64, rdev as u64, "rdev should be preserved during setattr");

    // Verify xattr contains rdev for device node
    let block_path = temp_dir.path().join("test.blk");
    let xattr_value = helper::get_xattr(&block_path, "user.containers.override_stat")?;
    assert!(xattr_value.is_some(), "Device nodes should have xattr");

    if let Some(xattr) = xattr_value {
        let parts: Vec<&str> = xattr.split(':').collect();
        assert_eq!(parts.len(), 4, "Device node xattr should have uid:gid:mode:rdev format");
        assert_eq!(parts[0], "1000"); // uid from context
        assert_eq!(parts[1], "1000"); // gid from context
        let mode = u32::from_str_radix(parts[2], 8).unwrap();
        assert_eq!(mode & libc::S_IFMT, libc::S_IFBLK, "xattr should preserve file type");
        assert_eq!(mode & 0o777, 0o600, "xattr should have updated permissions");
        let xattr_rdev = u64::from_str_radix(parts[3], 10).unwrap();
        assert_eq!(xattr_rdev, rdev as u64, "xattr should preserve rdev");
    }

    // Test character device with rdev
    let char_name = CString::new("test.chr").unwrap();
    let char_major = 1u32;  // Typical mem device major
    let char_minor = 3u32;  // /dev/null
    let char_rdev = libc::makedev(char_major, char_minor) as u32;

    let char_entry = fs.mknod(
        ctx,
        1,
        &char_name,
        libc::S_IFCHR | 0o666,
        char_rdev,
        0o022,
        Default::default(),
    )?;

    // Verify rdev is preserved
    assert_eq!(char_entry.attr.st_mode & libc::S_IFMT, libc::S_IFCHR);
    assert_eq!(char_entry.attr.st_rdev as u64, char_rdev as u64);

    // Test FIFO (should not have rdev in xattr)
    let fifo_name = CString::new("test.fifo").unwrap();
    let _fifo_entry = fs.mknod(
        ctx,
        1,
        &fifo_name,
        libc::S_IFIFO | 0o644,
        0,
        0o022,
        Default::default(),
    )?;

    // Verify FIFO xattr doesn't include rdev
    let fifo_path = temp_dir.path().join("test.fifo");
    let xattr_value = helper::get_xattr(&fifo_path, "user.containers.override_stat")?;
    if let Some(xattr) = xattr_value {
        let parts: Vec<&str> = xattr.split(':').collect();
        assert_eq!(parts.len(), 3, "FIFO xattr should only have uid:gid:mode format");
    }

    Ok(())
}
